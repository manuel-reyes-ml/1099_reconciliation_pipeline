# Docstring for src/build_correction_file module
"""

Build a Matrix 1099 correction file from matched Relius/Matrix data.

Usage in code / notebooks:
    from src.buil_correction_file import build_correction_dataframe, write_correction_file

    corrections_df = build_correction_dataframe(primary_matches)
    output_path = write_correction_file(corrections_df)

This module is intentionally focused on the Matrix side:
- We only export Matrix transaction identifiers and participant info
- We include both current and suggested tax code

"""

from __future__ import annotations      # Tells Python to store type hints as strings rather than real objects at import time.

from datetime import datetime           # Allows us to get time and date from datetime class.
from pathlib import Path                # Represents filesystem paths in an object-oriented way.
from typing import Iterable, Optional   # These are for type hints.

import pandas as pd

from .config import REPORTS_DIR         # '.' in .config means 'sibling module'.



# --- Core funtions ------------------------------------------------------------

def build_correction_dataframe(
        matches: pd.DataFrame,
        allowed_actions: Optional[Iterable[str]] = ("UPDATE_1099",),
) -> pd.DataFrame:
    
    """

    Build a tidy correction DataFrame from the full matches DataFrame.

    Expected input columns (from reconcile_relius_matrix + cleaning):
        - _merge
        - date_within_tolerance
        - match_status
        - action
        - transaction_id        (Matrix)
        - txn_date              (Matrix)
        - ssn
        - participant_name OR full_name
        - matrix_account
        - tax_code_1, tax_code_2
        - suggested_tax_code_1, suggested_tax_code_2
        - correction_reason
    
    We select rows that:
        - have match_status == 'match_needs_correction'
        - have a non-null suggested_tax_code_1
        - (optionally) have an 'action' in allowed_action
        - and, if colums exist, are within date tolerance and present in both systems.

    Args:
        matches:
            DataFrame from reconcile_relius_matrix(), or a filtered
            subset like "primary_matches".
        allowed_actions:
            Which actions to include in the correction file. For now,
            typically ("UPDATE_1099",). You can extend this later.

    Returns:
        A DataFrame with one row per correction to send to Matrix, with
        columns aligned to a Matrix-style correction template.

    """

    df = matches.copy()

    # 1) Basic correction condition: status + suggestion present
    mask_needs_corr = df["match_status"].eq("match_needs_correction")  # .eq -> is Series 1 equal to Series 2, returns a boolean Series(True / False)
    mask_has_suggestion = df["suggested_tax_code_1"].notna()           # .notna() -> is Series not missing (NA), returns a boolean Series(True / False)

    # 2) If '_merge' and 'date_within_tolerance' exist, enforce them;
    #    otherwise assume the input is already filtered.
    if "_merge" in df.columns:
        mask_in_range = df["_merge"].eq("both")
    else:
        mask_in_range = pd.Series(True, index=df.index) # Creates a Series of all True. df.index same lenght (rows) as df. 
    
    if "date_within_tolerance" in df.columns:
        mask_in_range &= df["date_within_tolerance"].fillna(False)     # .fillna(False) -> replace NA values with boolean False.
                                                                       # '&=' -> means elementwise AND + assignment. Mean:
                                                                       #    mask_in_range = mask_in_range & df["date_within_tolerance"].fillna("False")
    
    # 3) Filter by allowed actions if 'action' column exists
    if "action" in df.columns and allowed_actions is not None:
        allowed_actions = set(allowed_actions)                         # Converts to Set for faster membership checks.
        mask_action = df["action"].isin(allowed_actions)               # Creates a Series of boolean -> is df["action"] values in 'allowed_actions'?
    else:
        mask_action = pd.Series(True, index=df.index)                  # Creates a Series of True, for the lenght(rows) of df.

    corr_mask = mask_needs_corr & mask_has_suggestion & mask_in_range & mask_action
    df_corr = df[corr_mask].copy()                                     # Boolean indexing: keeps only rows where corr_mask is True.

    # Expected output columns in Excel correction file
    out_cols=[
        "Transaction Id",
        "Transaction Date",
        "Participant SSN",
        "Participant Name",
        "Matrix Account",
        "Current Tax Code 1",
        "Current Tax Code 2",
        "New Tax Code 1",
        "New Tax Code 2",
        "Reason",
        "Action",
    ]

    if df_corr.empty:                                                  # True if there are no rows in df_corr DataFrame
        # Return an empty DataFrame with the expected columns
        return pd.DataFrame(columns=out_cols)                          # Creates an empty DataFrame with the correction template's columns and returns it.
      
    # 4) Reset index ONCE so all columns share the same RangeIndex
    df_corr = df_corr.reset_index(drop=True)                           # Creates a new integer index starting from 0.
                                                                       # Dros the old index(instead of turning it into a column).

    # 5) Build a unified participant name column INSIDE df_corr
    if "participant_name" in df_corr.columns:
        df_corr["participant_name_final"] = df_corr["participant_name"]
    elif "full_name" in df_corr.columns:
        df_corr["participant_name_final"] = df_corr["full_name"]
    else:
        df_corr["participant_name_final"] = pd.NA

    # 6) Rename columns to the Matrix correction template name
    # rename_map is a dictionary mapping internal column names -> output column names
    rename_map = {                                              
        "transaction_id": "Transaction Id",
        "txn_date": "Transaction Date",
        "ssn": "Participant SSN",
        "participant_name_final": "Participant Name",
        "matrix_account": "Matrix Account",
        "tax_code_1": "Current Tax Code 1",
        "tax_code_2": "Current Tax Code 2",
        "suggested_tax_code_1": "New Tax Code 1",
        "suggested_tax_code_2": "New Tax Code 2",
        "correction_reason": "Reason",
        "action": "Action",
    }
    
    out = df_corr.rename(columns=rename_map)                           # After this out["Transaction Id"] contains the values originally 
                                                                       #    in df_corr["transaction_id"].

    # 7) Keep only the columns we want, in the desired order
    out = out[out_cols]

    # 8) Optional: sort for readability (by plan, SSN, txn date)
    sort_cols = [
        col for col in ["Matrix Account", "Participant SSN", "Transaction Date"] 
        if col in out.columns
    ]
    if sort_cols:
        out = out.sort_values(sort_cols)                               # Sorts the rows by sort_columns in ascending order.
    
    return out.reset_index(drop=True)                                  # Reset index again so rowlablels are 0,1,2... in the final DataFrame you return.


# Write the Excel file
def write_correction_file(
        corrections_df: pd.DataFrame,
        output_path: Optional[Path | str] = None,
) -> Path:
    
    """

    Write the correction DataFrame to an Excel file.

    Args:
        corrections_df:
            DataFrame from build_correction_dataframe().
        output_path:
            Optional explicit path. If None, a timestamped file will be
            created under reports/samples/.

    Returns:
        Path to the written Excel file.

    """

    # Ensure reports/samples directory exists
    samples_dir = REPORTS_DIR / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)                     # Creates directory if it doesn't exist. 
                                                                       # parents=True -> also create parent directories if needed.
                                                                       # exis_ok=True -> no error if directory already exists.

    # datetime.now()           -> current date and time.
    # strftime("%Y%m%d_%H%M%S) -> format like '20251214_213045'
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = samples_dir / f"correction_file_{timestamp}.xlsx"
    else:
        output_path = Path(output_path)                                # Conver it to a Path object for consistent handling.
    
    corrections_df.to_excel(output_path, index=False)                  # Writes the DataDrame to an Excel '.xlsx' file.
                                                                       # index=False -> don't include pandas row index as a separate column.

    return output_path                                                 # You return a Path pointing to the saved file.



# --- Optional CLI-style entry point --------------------------------------------

def main() -> None:

    """

    Optional entry point to run the full pipeline from the command line.

    This is a simple example that:
      - loads raw Relius + Matrix from default RAW_DIR paths,
      - cleans them,
      - reconciles matches,
      - builds the correction DataFrame,
      - writes an Excel correction file.

    You can adapt the file names or integrate with argparse as needed.

    """

    # These imports are inside main() so that importing the module doesn't automatically import everything;
    #  they only needed when you run the pipeline.
    from .config import RAW_DATA_DIR, INHERITED_PLAN_IDS
    from .load_data import load_relius_excel, load_matrix_excel
    from .clean_relius import clean_relius
    from .clean_matrix import clean_matrix
    from .match_transactions import reconcile_relius_matrix



    # Adjust filenames here as needed (synthetic defaults)
    relius_path = RAW_DATA_DIR / "real_inherited_relius_2025.xlsx"
    matrix_path = RAW_DATA_DIR / "real_all_matrix_2025.xlsx"


    # LOAD RAW DATA
    relius_raw = load_relius_excel(relius_path, use_sample_if_none=True)
    matrix_raw = load_matrix_excel(matrix_path, use_sample_if_none=True)

    # CLEAN RAW DATA
    relius_clean = clean_relius(relius_raw)
    matrix_clean = clean_matrix(matrix_raw)

    # RECONCILE DATA RELIUS vs MATRIX
    matched = reconcile_relius_matrix(
        relius_clean,
        matrix_clean,
        plan_ids=INHERITED_PLAN_IDS,  # focus on inherited plans by default
        apply_business_rules=True,
    )

    # GENERATE CORRECTION FILE
    corrections_df = build_correction_dataframe(matched)
    output_path = write_correction_file(corrections_df)

    print(f"Corrections written to: {output_path}")
    print(f"Total corrections: {len(corrections_df)}")



if __name__ == "__main__":
    main()
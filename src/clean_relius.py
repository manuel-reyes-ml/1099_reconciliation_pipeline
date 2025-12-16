# Docstring for src/clean_relius module
"""
clean_relius.py

Cleaning and normalization for Relius distribution export data.

This module reads a Relius Excel export (typically a high-column-count operational
report) and transforms it into a canonical, analysis-ready pandas DataFrame used by:

- The inherited-plan reconciliation engine (`match_transactions.py`)
- Downstream correction-file generation (`build_correction_file.py`)
- Optional analytics / reporting notebooks (EDA, match-rate KPIs)

The intent is to reduce a large and inconsistent export into a stable schema with
reliable keys, normalized datatypes, and a derived distribution category that
supports business-rule decisions (e.g., rollover vs cash distribution).

Design goals
------------
- Canonical schema: standardize column names and datatypes so downstream matching
  logic is consistent and maintainable.
- Data quality: normalize SSNs, dates, and amounts to reduce false mismatches.
- Business interpretability: derive a distribution category from `DISTRNAM`
  (distribution description) to support rules and reporting.
- Traceability: retain Relius transaction identifiers and plan-level fields for
  auditing and troubleshooting.

Inputs
------
- Relius Excel export (.xlsx) containing distribution transactions.
  Column names may vary by export; mapping is controlled via `config.RELIUS_COLUMN_MAP`.

Core transformations
--------------------
1) Column standardization
   - Rename raw Relius headers to canonical names using `config.RELIUS_COLUMN_MAP`.
   - Keep only columns defined in `config.RELIUS_CORE_COLUMNS` (plus any required
     identifiers such as `trans_id_relius`).

2) Field normalization
   - SSN (`ssn`):
       Normalize to a 9-digit string: strip non-digits, handle Excel float-like
       strings, truncate >9 digits, left pad via zfill(9), invalid -> <NA>.
   - Dates (`exported_date`):
       Parse with pandas and coerce invalid values to NaT; store as date-only.
       This date is used as the “source” date for timing tolerance vs Matrix
       transaction date (Matrix occurs on/after Relius export date).
   - Amounts (`gross_amt`):
       Convert to numeric via `pd.to_numeric(errors="coerce")`.
   - Distribution code (`dist_code_1`):
       Clean to a standardized uppercase/stripped string.

3) Derived fields
   - Distribution category (`dist_category_relius`):
       Derived from `dist_name` (Relius `DISTRNAM`) using configurable keyword
       mapping rules. Typical categories include:
         - rollover
         - cash_distribution
         - rmd
         - partial_liquidation
       This category enables downstream business logic (e.g., inherited plan
       coding, rollover identification, etc.).

4) Deduplication
   Drop duplicate rows using `config.RELIUS_MATCH_KEYS` (or a conservative subset
   that includes stable identifiers like `trans_id_relius` when available).

Expected output schema (canonical)
----------------------------------
Typical canonical columns produced by this module include:

- plan_id
- ssn
- first_name
- last_name
- state
- gross_amt
- exported_date
- dist_code_1
- dist_name
- dist_category_relius
- trans_id_relius
- tax_year (when available in export)

Public API
----------
- clean_relius(path: str | Path) -> pd.DataFrame
    Main entrypoint. Returns a cleaned Relius distribution DataFrame ready for
    reconciliation against Matrix.

Privacy / compliance note
-------------------------
This repository is designed for portfolio use with synthetic or masked data.
Never commit real participant PII (SSNs, names, addresses) to source control.
The production implementation should run only in secure environments with proper
access controls and retention policies.
"""


# Tells Python not to execute type hints as real code now. Treat them more like comments/strings and
# it will resolve it later
from __future__ import annotations

import re # Python's regular expression module
from typing import Iterable #A type hint to describe an arg(variable) should be an object that can iterate:
                            # list, tuple, set, dict, str
import pandas as pd

from .config import (
    RELIUS_COLUMN_MAP,
    RELIUS_CORE_COLUMNS,
    RELIUS_MATCH_KEYS,
)



# --- Helper functions ------------------------------------------------------------

# _* prefix is a convention: "This is an internal helper,no part of the public API."
#
# -> str | pd.NA = type hint meaning "returns a string or (panda's nullable missing value)"
def _normalize_ssn(value) -> str | pd.NA: #Pylance says error in using pd.NA here but code runs fine

    """
  
    Normalize SSN to a 9-digit string.

    - Strips all non-digits.
    - Pads with leading zeros if needed
    - Returns <NA> if no digits are found
    
    """

    # Check if the value is pandas-style missing (NaN, None, etc.)
    if pd.isna(value):
        return pd.NA
    
    # Regex \D means "non-digits character"
    # re.sub(r"...") removes all non-digits
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return pd.NA
    
    #.zfill(9) pads with leading zeros to length 9
    # "123" -> "000000123"
    return digits.zfill(9)


# pd.Series expectes a single column from a DataFrame (a Series)
def _parse_date(series: pd.Series) -> pd.Series:

    """
    
    Convert various date formats to pandas datetime (date only).

    Handles:
    - Excel date serials
    - 'YYYY-MM-DD'
    - etc.

    Any unparsable values become NaT (Not a Time) / NaN (Not a Number).

    """

    # pd.to_datetime tries to parse each value into a datetime
        # error="coerce" -> invalid values becomes NaT instead of crashing
    # .dt.date take only the date part(no time of day), reurning Python datetime.date objects.
    return pd.to_datetime(series, errors="coerce").dt.date



def _to_float(series: pd.Series) -> pd.Series:

    """
    
    Convert amounts to float, handling strings and blanks gracefully.
    
    """

    # Converts values to numbers(int/float)
    # non-numeric values become NaN instead of crashing
    return pd.to_numeric(series, errors="coerce")



def _drop_unneeded_columns(df: pd.DataFrame, keep: Iterable[str]) -> pd.DataFrame:

    """
    
    Keep only specified columns (ignore others).

    Returns a shallow copy with just the desired columns.
    
    """
    # df.columns = Index of column names in te DataFrame (iterable object)
    cols = [c for c in keep if c in df.columns] #List comprehension

    # df[cols] = Select only those columns
    # .copy() return a new DataFrame, not a view.
    return df[cols].copy()



def _classify_relius_dist_type(name: str | float | None) -> str:

    """
    
    Map free-text DISTRNAM (dist_name) to a normalized disribution category.

    Examples based on Relius raw file:
    
        - 'RMD ACH'                         -> 'rmd'
        - 'Partial liquidation gross ACH    -> 'partial_cash'
        - 'Rollover'                        -> 'rollover'
        - 'Partial Rollover - Net'          -> 'partial_rollover'

    Anything that doesn't match known patter is labeled 'other'.

    You can refine this mapping later once you've explored real values in notebooks.

    """

    # If the value isn't a string (NaN, float, None), just return 'other'
    if not isinstance(name, str):
        return "other"
    
    # Normalize name removing leading/trailing spaces  and all lowecase (case-insensitive matching)
    text = name.strip().lower()
    
    if "rollover" in text:
        if "partial" in text:
            return "partial_rollover"
        return "rollover"
    
    if "rmd" in text:
        return "rmd"
    
    if ("partial" in text and "liquidation") or "recurring" in text:
        return "partial_cash"
    
    if "liquidation" in text and "full" in text:
        return "final_cash"
    
    return "other"



# --- Main cleaning functions ------------------------------------------------------------

#raw_df expects panda DataFrame from load_data.py
#drop_rows_missing_keys = flag to control wether the function drops rows that are missing key fields
def clean_relius(
        raw_df: pd.DataFrame,
        drop_rows_missing_keys: bool = True,
) -> pd.DataFrame:
        
    """
        
    Clean and normalize Relius export.

    Steps:
    1. Rename raw columns to canonical names using RELIUS_COLUMN_MAP
    2. Keep only the core columns defined in RELIUS_CORE_COLUMNS
    3. Clean SSNs, dates, amounts, and distribution codes
    4. Derive a normalized distribution category from dist_name
    5. Optionally drop rows missing key fields
    6. Drop duplicate rows based on RELIUS_MATCH_KEYS

    Args:
        raw_df:
            DataFrame as loaded from the raw Relius Excel export.
        drop_rows_missing_keys:
            If True, drop rows where any of the match keys are missing
            (plan_id, ssn, gross_amt, exported_date, tax_year).
        
    Returns:
        A cleaned DataFrame ready for matching with Matrix.
        
    """

    # Work on a copy so we don't mutate the original loaded DataFrame
    df = raw_df.copy()

    # 1) Rename raw columns -> canonical names
    df = df.rename(columns=RELIUS_COLUMN_MAP)

    # 2) Keep only the core columns we care about
    df = _drop_unneeded_columns(df, RELIUS_CORE_COLUMNS)

    # 3) Clean fields

    # SSN
    if "ssn" in df.columns:
        # Apply _normalize_ssn  to each valye in the 'ssn' column
        df["ssn"] = df["ssn"].apply(_normalize_ssn)
    
    # Dates
    if "exported_date" in df.columns:
        df["exported_date"] = _parse_date(df["exported_date"]) # Returns either Series of datetime.date or NaT

    # Tax year
    if "tax_year" in df.columns:
        # Convert to number, if invalid -> NaN
        # .astype("Int64") = pandas' nullable integer type (suppoer NA)
        df["tax_year"] = pd.to_numeric(df["tax_year"], errors="coerce").astype("Int64")

    # Amounts
    if "gross_amt" in df.columns:
        df["gross_amt"] = _to_float(df["gross_amt"])

    # distribution code (Relius perspective)
    if "dist_code_1" in df.columns:
        # Pandas string pipeline
        df["dist_code_1"] = (
            df["dist_code_1"]
            .astype(str)      # convert all values to strings(even NaN becomes 'nan')
            .str.strip()      # trim spaces around each string
            .str.upper()     # uppercases the string(at the distribution codes are: '7', '1', 'G', etc.)
        )

    # Distribution name -> category
    if "dist_name" in df.columns:
        # Apply _classify_relius_dist_type  to each value in the 'dist_name' column
        # Store normalized category in new column 'dist_category_relius'
        df["dist_category_relius"] = df["dist_name"].apply(_classify_relius_dist_type)

    # Full name (for matching Matrix and reporting)
    if "first_name" in df.columns and "last_name" in df.columns:
        df["full_name"] = (
            df["first_name"].fillna("").astype(str).str.strip() # .astype(str) ensure string
            + " "
            + df["last_name"].fillna("").astype(str).str.strip()
        ).str.strip() # removes leading/trailing spaces in case one side was empty
    
    # 4) Optionally drop rows missing key fields for matching
    match_key_cols = [c for c in RELIUS_MATCH_KEYS if c in df.columns]
    if drop_rows_missing_keys and match_key_cols:
        # Drops any row that has NaN/NA in an of those key columns
        df = df.dropna(subset=match_key_cols, how="any")

    
    # 5) Drop duplicate rows based on match keys
    if match_key_cols:
        # If multiple rows have the same values in all match key columns, only keep the first
        df = df.drop_duplicates(subset=match_key_cols, keep="first")
    
    return df
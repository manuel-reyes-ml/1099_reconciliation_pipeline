# Docstring for src/load_data.py module
"""

Data loading utilities for the 1099 reconciliation pipeline.

Reponsible for:
- Reading raw Relius and Matrix Excel exports into pandas DataFrames
- Doing basic sanity checks (e.g., required columns exist)
- Leaving cleaning / renaming to the cleaning modules

"""

from pathlib import Path
from typing import Optional #For type hinting optional parameters | Describing the allowed types for an arg(variable)

import pandas as pd #The main data manipulation library for data tables

# Relative imports from the config module in the same package /src
from .config import (
    SAMPLE_DIR,
    RELIUS_COLUMN_MAP,
    MATRIX_COLUMN_MAP,
)


# df: pd.DataFrame -> type hinting that df "should be" pandas DataFrame
# required_cols -> a list of strings representing required column names
# source_name: str -> "should be" a string -- just used in error messages 
# to say "Relius/Matrix is missing X column"
# -> None indicates this function does not return anything (it just does checks/raises errors)
#
# _* prefix is a convention: "This is an internal helper, no part of the public API."
def _validate_columns(df: pd.DataFrame, required_cols, source_name: str) -> None:

    """
    
    Ensure that the DataFrame has at least the required columns.

    Args:
        df: DataFrame to validate.
        required_cols: Itrable of column names that MUST be present in df.
        source_name: Label for error messages (e.g. 'Relius', 'Matrix').

    Raises:
        ValueError: if any required column is missing.

    """

    # Check for missing columns
        #df.columns is a pandas index object listing all column names in the DataFrame
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"{source_name}: missing required columns: {missing}"
            f"Present columns: {list(df.columns)}"
        )



def load_relius_excel(
        path: Optional[Path] = None,     # Type hint: "Should be" either a Path object or None / Newer Python: path: Path | None = None
        use_sample_if_none: bool = True, # If path is None, load sample data
        sheet_name: Optional[str] = 0,   # Type hint: Which sheet to load from Excel file, default first sheet
) -> pd.DataFrame:                       # Returns a pandas DataFrame
    
    """
    
    Load Relius transaction / 1099 export from Excel file.

    Args:
        path:
            Path to the Excel file. If None and use_sample_if_none is True,
            defaults to SAMPLE_DIR / 'relius_sample.xlsx'.
        use_sample_if_none:
            If True and path is None, loan from the sample  directory.
        sheet_name:
            Sheet name or index to read (defaults to first sheet).

    Returns:
        pandas.DataFrame with raw Relius data (no clearning/renaming yet).

    """

    # Determine path to load
    if path is None:
        if not use_sample_if_none:
            raise ValueError("No path provided and use_sample_if_none=False.")
        path = SAMPLE_DIR / 'relius_sample.xlsx'

    path = Path(path)     #Ensure path is a Path object
    if not path.exists(): #Check if file exists
        raise FileNotFoundError(f"Relius Excel file not found at: {path}")
    
    
    # Load Excel file into DataFrame
        # df becomes a pandas DataFrame object with rows/columns from Excel
        # At this point df has raw column names as in the Excel file
    df = pd.read_excel(path, sheet_name=sheet_name)
    
    # Basic required columns (raw names as they appear in the Excel file)
    required_cols = list(RELIUS_COLUMN_MAP.keys())
    _validate_columns(df, required_cols, source_name="Relius") #Check required columns

    return df  #Return the loaded DataFrame without any cleaning yet



def load_matrix_excel(
        path: Optional[Path] = None,
        use_sample_if_none: bool = True,
        sheet_name: Optional[str] = 0,
) -> pd.DataFrame:
    
    """
    
    Load Matrix disbursement / 1099 export from Excel file.

    Args:
        path:
            Path to the Excel file. If None and use_sample_if_none is True,
            defaults to SAMPLE_DIR / 'matrix_sample.xlsx'.
        use_sample_if_none:
            If True and path is None, loan from the sample  directory.
        sheet_name:
            Sheet name or index to read (defaults to first sheet).

    Returns:
        pandas.DataFrame with raw Relius data (no clearning/renaming yet).

    """

    if path is None:
        if not use_sample_if_none:
            raise ValueError("No path provided and use_sample_if_none=False.")
        path = SAMPLE_DIR / "matrix_sample.xlsx"

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Matrix Excel file not found at: {path}")
    
    df = pd.read_excel(path, sheet_name=sheet_name)

    required_cols = list(MATRIX_COLUMN_MAP.keys())
    _validate_columns(df, required_cols, source_name="Matrix")

    return df

"""

Cleaning and normalization for Matrix export data.

This module:

- Renames raw Matrix columns to canonical names using config.MATRIX_COLUMN_MAP
- Keeps only the core columns defined in config.MATRIX_CORE_COLUMNS
- Cleans and normalizes:
    - SSN (9-digit string)
    - Dates (txn_date -> datetime.date)
    - Amounts (gross_amt -> float)
    - Tax codes (tax_code_1 / tax_code_2 -> uppercase, stripped)
    - State and text fields (stripped)
- Filters out rows that are NOT useful for Relius distribution matching:
    - Matrix accounts in {07N00442, 07I00442, 07M00442}
    - Transaction Type in
        {Account Transfer, Suspense Transfer,
        ACH Distribution Reject, Check Stop}
- Drops duplicate rows based on config.MATRIX_MATCH_KEYS

"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from .config import (
    MATRIX_COLUMN_MAP,
    MATRIX_CORE_COLUMNS,
    MATRIX_MATCH_KEYS,
)



# --- Helper functions ------------------------------------------------------------

def _normalize_ssn(value) -> str | pd.NA:

    """

    Normalize SSN to a 9-digit string for Matrix.

    Handles cases where Excel reads SSNs as floats, e.g.:
        194362032.0 -> '194562032'
        1.94362032e8 -> '194562032
    
    Logic:
    - Convert to string, strip
    - Remove all non-digits
    - If we end up with more than 9 digits, keep only the first 9
      (SSNs are 9 digtits by definitation)
    - Pad with leading zeros if fewer than 9

    """

    if pd.isna(value):
        return pd.NA
    
    text = str(value).strip()
    if not text:
        return pd.NA

    digits = re.sub(r"\D", "", text) # keep only 0-9
    if not digits:
        return pd.NA
    
    # If we got more than 9 digits (e.g. '1945620320' from '194562032.0'),
    # just take the first 9. This is safe because SSNs are exactly 9 digits.
    if len(digits) > 9:
        digits = digits[:9]

    return digits.zfill(9)



def _parse_date(series: pd.Series) -> pd.Series:

    """
    
    Convert various date formats to pandas datetime (date only).

    Handles:
    - Excel date serials
    - 'YYYY-MM-DD'
    -'MM/DD/YYYY'
    - etc.

    Any unparsable values become NaT / NaN
    
    """

    return pd.to_datetime(series, errors="coerce").dt.date



def _to_float(series: pd.Series) -> pd.Series:

    """
    
    Convert amounts to float, handling strings and blanks gracefully.
    
    """

    return pd.to_numeric(series, errors="coerce")



def _drop_unneeded_columns(df: pd.DataFrame, keep: Iterable[str]) -> pd.DataFrame:

    """
    
    Keep only specified columns (ignore others).

    Returns a shallow copy with just the desired columns.
    
    """

    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()



def _normalize_tax_code(value) -> str | pd.NA:

    """
    
    Normalize Matrix tax code values.

    Matrix examples:
        '7 - Normal Distributions'
        'G - Rollover'
        '7'
        ' G   -   Something '

    We want just the primary code character: '7' or 'G'
    
    Logic:
    - Convert to string, strip
    - Remove leading 'CODE ' if it exists (defensive)
    - Find the first alphanumeric [0-9A-Z] and return it (uppercase)
    - If nothing found, return <NA>

    """

    if pd.isna(value):
        return pd.NA
    
    text = str(value).strip()
    if not text:
        return pd.NA
    
    # Remove leading "code " if it ever appears
    text = re.sub(r"^code\s*", "", text, flags=re.IGNORECASE)

    # Find first alphanumeric code character
    m = re.search(r"[0-9A-Z]", text.upper())
    if not m:
        return pd.NA
    
    return m.group(0) # e.g. '7', 'G'



# --- Filter configuration specific to Matrix -------------------------------------

# Matrix accounts we want to completely ignore for matching
IGNORED_MATRIX_ACCOUNTS = {
    "07B00442",
    "07I00442",
    '07M00442',
}

# Transaction types that are not true distributions for our purposes
IGNORED_TXN_METHODS = {
    "account transfer",
    "suspense transfer",
    "ach distribution reject",
    "check stop",
}



# --- Main cleaning function -------------------------------------

def clean_matrix(
        raw_df: pd.DataFrame,
        drop_rows_missing_keys: bool = True,
) -> pd.DataFrame:
    
    """
    
    Clean and normalize Matrix report.

    Steps:
    1. Rename raw columns to canonical names using MATRIX_COLUMN_MAP
    2. Keep only the core columns defined in MATRIX_CORE_COLUMNS
    3. Clean SSNs, dates, amounts, tax codes, and text fields
    4. Filter out:
        - rows with matrix_account in IGNORED_MATRIX_ACCOUNTS
        - rows with txn_method in IGNORED_TXN_METHODS
    5. Optionally drop rows missing key fields
    6. Drop Duplicate rows based on MATRIX_MATCH_KEYS

    Args:
        raw_df:
            DataFrame as loaded from the raw Matrix Excel export.
        drop_rows_missing_keys:
            If True, drop rows where any of the match keys are missing
            (plan_id, ssn, gross_amt, txn_date).
    
    Returns:
        A cleaned DataFrame ready for matching with Relius.
    
    """

    # Work on a copy so we don't mutate the original loaded DataFrame
    df = raw_df.copy()

    # 1) Rename raw columns -> canonical names
    df = df.rename(columns=MATRIX_COLUMN_MAP)

    # 2) Keep only the core columns we care about
    df = _drop_unneeded_columns(df, MATRIX_CORE_COLUMNS)

    # 3) Clean fields

    # SSN
    if "ssn" in df.columns:
        df["ssn"] = df["ssn"].apply(_normalize_ssn)
    
    # Dates
    if "txn_date" in df.columns:
        df["txn_date"] = _parse_date(df["txn_date"])
    
    # Amounts
    if "gross_amt" in df.columns:
        df["gross_amt"] = _to_float(df["gross_amt"])

    # State
    if "state" in df.columns:
        df["state"] = (
            df["state"]
            .astype(str)
            .str.strip()
            .str.upper()
        )
    
    # Tax codes: extract primary code character (e.g. '7', 'G')
    for col in ["tax_code_1", "tax_code_2"]:
        if col in df.columns:
            df[col] = df[col].apply(_normalize_tax_code)
    
    # Transaction method (ACH / Wire / Check)
    if "txn_method" in df.columns:
        df["txn_method"] = (
            df["txn_method"]
            .astype(str)
            .str.strip()
        )
    
    # Distribution type (Matrix perspective - keep raw but cleaned)
    if "dist_type" in df.columns:
        df["dist_type"] = (
            df["dist_type"]
            .astype(str)
            .str.strip()
        )
    
    # Convenience: participant name normalized
    if "participant_name" in df.columns:
        df["partipant_name"] = (
            df["participant_name"]
            .astype(str)
            .str.strip()
        )
    
    # 4) Filter out unwanted accounts and transaction types

    # Ignore specific Matrix accounts entirely
    if "matrix_account" in df.columns:
        mask_bad_acct = df["matrix_account"].astype(str).isin(IGNORED_MATRIX_ACCOUNTS)
    else:
        mask_bad_acct = pd.Series(False, index=df.index)
    
    # Ignore rows where Transaction Type is in the excluded set
    if "txn_method" in df.columns:
        method_lower = df["txn_method"].astype(str).str.strip().str.lower()
        mask_bad_method = method_lower.isin(IGNORED_TXN_METHODS)
    else:
        mask_bad_method = pd.Series(False, index=df.index)
    
    mask_drop = mask_bad_acct | mask_bad_method
    df = df[~mask_drop].copy()

    # 5) Optionally drop rows missing key fields for matching
    match_key_cols = [c for c in MATRIX_MATCH_KEYS if c in df.columns]
    if drop_rows_missing_keys and match_key_cols:
        df = df.dropna(subset=match_key_cols, how="any")
    
    # 6) Drop duplicate rows based on match keys
    if match_key_cols:
        df = df.drop_duplicates(subset=match_key_cols, keep="first")

    return df
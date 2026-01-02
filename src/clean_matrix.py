# Docstring for src/clean_matrix module
"""
clean_matrix.py

Cleaning and normalization for Matrix distribution export data.

This module reads a Matrix Excel export and transforms it into a canonical,
analysis-ready pandas DataFrame that can be used by:

- The inherited-plan reconciliation engine (`match_planid.py`)
- The age-based tax-code engine (`age_taxcode_analysis.py`)
- Downstream reporting / correction file generation (`build_correction_file.py`)

The goal is to convert a messy operational export (mixed types, inconsistent
formats, non-distribution rows) into a stable schema with reliable keys.

Design goals
------------
- Canonical schema: normalize column names and datatypes so downstream logic
  can be consistent across files and tax years.
- Data quality: enforce repeatable normalization for SSNs, dates, amounts, and
  tax codes to reduce false mismatches.
- Noise reduction: remove non-distribution transactions and known accounts that
  should not participate in matching/correction workflows.
- Traceability: keep identifiers (Transaction Id) and key operational fields for
  auditability.

Inputs
------
- Matrix Excel export (.xlsx) containing distribution activity.
  Column names may vary by export; mapping is controlled via `config.MATRIX_COLUMN_MAP`.

Core transformations
--------------------
1) Column standardization
   - Rename raw Matrix headers to canonical names using `config.MATRIX_COLUMN_MAP`.
   - Keep only columns defined in `config.MATRIX_CORE_COLUMNS` (plus any required
     identifiers for matching and correction output).

2) Field normalization
   - SSN (`ssn`):
       Normalize to a 9-digit string: strip non-digits, handle Excel float-like
       strings, truncate >9 digits, left pad via zfill(9), invalid -> <NA>.
   - Dates (`txn_date`):
       Parse with pandas and coerce invalid values to NaT; store as date-only.
   - Amounts (`gross_amt`):
       Convert to numeric via `pd.to_numeric(errors="coerce")`.
   - Fed taxable amount (`fed_taxable_amt`):
       Convert to numeric via the same coercion as gross amount.
   - Tax codes (`tax_code_1`, `tax_code_2`):
       Normalize to 1–2 leading characters (e.g., "7", "11", "G", "H") from
       strings like "7 - Normal Distribution". This prevents accidental truncation
       and supports multi-digit codes.
   - Roth initial contribution year (`roth_initial_contribution_year`):
       Convert to numeric and store as pandas nullable integer (Int64).
   - Text fields (participant name, state, plan_id, transaction type):
       Strip whitespace and standardize casing where appropriate.

3) Filtering (noise reduction)
   Filter out rows that are not meaningful for distribution matching/corrections:
   - Matrix accounts excluded (configurable):
       e.g., {"07B00442", "07I00442", "07M00442"}
   - Transaction types excluded (configurable):
       e.g., {"Account Transfer", "Suspense Transfer", "ACH Distribution Reject", "Check Stop"}

4) Deduplication
   Drop duplicate rows using `config.MATRIX_MATCH_KEYS` (or a conservative subset
   that includes stable identifiers like `transaction_id` when available).

Expected output schema (canonical)
----------------------------------
Typical canonical columns produced by this module include:

- plan_id
- matrix_account
- transaction_id
- txn_date
- txn_method (transaction type)
- ssn
- participant_name
- state
- gross_amt
- tax_code_1
- tax_code_2

Downstream engines may require additional fields depending on workflow, but
the above set forms the "core" operational schema.

Public API
----------
- clean_matrix(path: str | Path) -> pd.DataFrame
    Main entrypoint. Returns a cleaned DataFrame ready for matching/correction engines.

- (optional helper functions)
    Internal helpers may include SSN and tax code normalization functions.

Privacy / compliance note
-------------------------
This project is designed for portfolio use with synthetic or masked data. Never
commit real participant PII (SSNs, names, addresses) to source control. Run the
production version only in secure environments with appropriate access controls.
"""


from __future__ import annotations

import re
from typing import Iterable
import warnings

import pandas as pd

from .config import (
    MATRIX_COLUMN_MAP,
    MATRIX_CORE_COLUMNS,
    MATRIX_MATCH_KEYS,
)

from .normalizers import (
    build_validation_issues,
    normalize_plan_id_series,
    normalize_ssn_series,
    normalize_state_series,
    normalize_tax_code_series,
    normalize_text_series,
    cross_validate_series,
    validate_amounts_series,
    validate_dates_series,
    validate_1099r_code_series,
    validate_ssn_series,
    to_date_series,
    to_int64_nullable_series,
    to_numeric_series,
)



# Refer to notes in src/clean_relius.py for better understanding in helpfer funtions...

# --- Helper functions ------------------------------------------------------------


def _drop_unneeded_columns(df: pd.DataFrame, keep: Iterable[str]) -> pd.DataFrame:

    """
    
    Keep only specified columns (ignore others).

    Returns a shallow copy with just the desired columns.
    
    """

    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()


def _normalize_transaction_id(value) -> str | pd.NA:

    """
    
    Normalize Transaction ID values.

    Matrix reads them as floats, examples when getting to DataFrame:
        '44324568' -> 44324568.0

    We want just the original transaction ID, without the decimal 0
    
    Logic:
    - Convert to string, strip
    - Find the first numeric digits '\d' before the ending 0 and return it
    - If nothing found, return <NA>

    """

    if pd.isna(value):
        return pd.NA
    
    text = str(value).strip()
    if not text:
        return pd.NA
    
    text = re.sub(r"\D","",text)
    if not text:
        return pd.NA
    

    # Find first numeric code characters before the ending 0: re.search(pattern, string) - if found returns match object, if not returns None.
    # '(\d+): find any numerical digits (1 or more) and extract this part
    # '0$': stop to extract when you reach literal '0'(zero)
    m = re.search(r"(\d+)0$", text)
    if not m:
        return pd.NA
    
    
    # .group(1) returns the part (...) from the match object returned by re.search()
    return m.group(1) # e.g. '12345'



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
    3. Filter out:
        - rows with matrix_account in IGNORED_MATRIX_ACCOUNTS
        - rows with txn_method in IGNORED_TXN_METHODS
    4. Clean SSNs, dates, amounts, tax codes, and text fields
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

    # 3) Filter out unwanted accounts and transaction types

    # Ignore specific Matrix accounts entirely
    # mask_bad_acct receives a Series (vector of 1 column)
    if "matrix_account" in df.columns:
        # Assigns True if values are in IGNORED_MATRIX_ACCOUNTS (if any)
        mask_bad_acct = df["matrix_account"].astype(str).isin(IGNORED_MATRIX_ACCOUNTS) # .isin() checks each row of the Series 
    else:                                                                              # in the given Set or List.
        # Creates a Series and assigns False to all rows (as a Series = 1 Column)
        mask_bad_acct = pd.Series(False, index=df.index) # df.index represents all rows in the DataFrame


    # Ignore rows where Transaction Type is in the excluded Set or List
    if "txn_method" in df.columns:
        # method_lower gets assigned a Series of 'tax_method' values in lower case
        method_lower = df["txn_method"].astype(str).str.strip().str.lower() # normalize before comparing
        mask_bad_method = method_lower.isin(IGNORED_TXN_METHODS)
    else:
        mask_bad_method = pd.Series(False, index=df.index)


    # mask_bad_acct and mask_bad_method both are pandas Series of booleans indexed like df
    # '|' in pandas is OR (logical computation) and will compare each row of the two Series
    #   e.g.: T OR F = T ; F OR F =  F
    #
    # A Series of True or False will be assigned to mask_drop indexed like df
    mask_drop = mask_bad_acct | mask_bad_method

    # ~ is the bitwise NOT operator for boolean Series, it flips True <-> False,
    #  so for mask_drop: [False, True, True]
    #        ~mask_drop: [True, False, False]
    #
    # df[~mask_drop] us boolean indexing in pandas, so keep the rows where mask_drop is false (converted to True), meaning
    #   rows that are not bad (not included on the two Lists or Sets above).
    df = df[~mask_drop].copy() # create a new DataFrame


    # 4) Clean fields

    if "plan_id" in df.columns:
        df["plan_id"] = normalize_plan_id_series(df["plan_id"])

    # SSN
    if "ssn" in df.columns:
        df["ssn"] = normalize_ssn_series(df["ssn"]) # .apply() applies function to all values in Series df[...]
        invalid_mask = df["ssn"].isna() | (df["ssn"].str.len() != 9)
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            warnings.warn(
                f"Matrix SSN normalization produced {invalid_count} invalid values.",
                stacklevel=2,
            )
    
    # Dates
    if "txn_date" in df.columns:
        df["txn_date"] = to_date_series(df["txn_date"]) # function takes a Series df[...] directly
    
    # Amounts
    if "gross_amt" in df.columns:
        df["gross_amt"] = to_numeric_series(df["gross_amt"])

    if "fed_taxable_amt" in df.columns:
        df["fed_taxable_amt"] = to_numeric_series(df["fed_taxable_amt"])

    if "roth_initial_contribution_year" in df.columns:
        df["roth_initial_contribution_year"] = to_int64_nullable_series(df["roth_initial_contribution_year"])
        # Int64 is pandas’ nullable integer dtype. It holds real integers and a proper missing value (<NA>) in the same column.
        #  You can still do numeric operations/filters cleanly while preserving missingness.

    # State
    if "state" in df.columns:
        df["state"] = normalize_state_series(df["state"])
    
    # Tax codes: extract primary code character (e.g. '7', 'G')
    for col in ["tax_code_1", "tax_code_2"]:
        if col in df.columns:
            df[col] = normalize_tax_code_series(df[col])
            lengths = df[col].str.len()
            invalid_tax = df[col].notna() & lengths.gt(2)
            invalid_tax_count = int(invalid_tax.sum())
            if invalid_tax_count > 0:
                warnings.warn(
                    f"Matrix tax code normalization produced {invalid_tax_count} values longer than 2 characters.",
                    stacklevel=2,
                )

    # Transaction IDs: extract transaction id from float format (e.g. 44324566.0 -> '44324566')
    if "transaction_id" in df.columns:
        df["transaction_id"] = df["transaction_id"].apply(_normalize_transaction_id)
    
    # Transaction method (ACH / Wire / Check)
    if "txn_method" in df.columns:
        df["txn_method"] = normalize_text_series(df["txn_method"], strip=True, upper=False)
    
    # Distribution type (Matrix perspective - keep raw but cleaned)
    if "dist_type" in df.columns:
        df["dist_type"] = normalize_text_series(df["dist_type"], strip=True, upper=False)
    
    # Convenience: participant name normalized
    if "participant_name" in df.columns:
        df["partipant_name"] = normalize_text_series(df["participant_name"], strip=True, upper=False)

    # Validation flags and issues
    ssn_valid = (
        validate_ssn_series(df["ssn"])
        if "ssn" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    amount_valid = (
        validate_amounts_series(df["gross_amt"], df["fed_taxable_amt"])
        if {"gross_amt", "fed_taxable_amt"} <= set(df.columns)
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    date_valid = (
        validate_dates_series(df["txn_date"])
        if "txn_date" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    code_1099r_valid = (
        validate_1099r_code_series(df["tax_code_1"])
        if "tax_code_1" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    cross_field_issues = (
        cross_validate_series(df["gross_amt"], df["fed_taxable_amt"], df["tax_code_1"])
        if {"gross_amt", "fed_taxable_amt", "tax_code_1"} <= set(df.columns)
        else None
    )

    df["ssn_valid"] = ssn_valid
    df["amount_valid"] = amount_valid
    df["date_valid"] = date_valid
    df["code_1099r_valid"] = code_1099r_valid
    df["validation_issues"] = build_validation_issues(
        ssn_valid,
        amount_valid,
        date_valid,
        code_1099r_valid,
        cross_field_issues=cross_field_issues,
    )
    
    
    # 5) Optionally drop rows missing key fields for matching
    match_key_cols = [c for c in MATRIX_MATCH_KEYS if c in df.columns]
    if drop_rows_missing_keys and match_key_cols:
        df = df.dropna(subset=match_key_cols, how="any") # Drops rows where the value has NaN/NA in 'any' of those key columns, when
                                                         #  how='all' drops only when 'all' of the columns have NaN/NA.
                                                         # 
                                                         # By default .dropna(axis=0) to drop rows but you can use
                                                         #  axis=1 to drop columns
    

    # 6) Drop duplicate rows based on match keys
    if match_key_cols:
        df = df.drop_duplicates(subset=match_key_cols, keep="first") # Drops duplicate rows based only by the match_key_cols
                                                                     # Keep only the first row
                                                                     # .drop_duplicates() only works on rows not on columns
                                                                     #   a trick is to transpose from columns to rows,
                                                                     #   df.T swaps rows & columns (rows become columns), and then
                                                                     #   we can use df.T.drop_duplicates().T, .T at the end to transpose again

    return df

# Docstring for src/clean_relius_roth_basis module
"""
clean_relius_roth_basis.py

Cleaning and normalization for Relius Roth basis export data.

This module reads a Relius Roth basis Excel export and transforms it into a
canonical, analysis-ready pandas DataFrame for Roth basis analytics and
downstream matching.

Inputs
------
- Relius Excel export (.xlsx) with columns:
    PLANID, SSNUM, LASTNAM, FIRSTNAM, FIRSTTAXYEARROTH, Total

Core transformations
--------------------
1) Column standardization
   - Rename raw Relius headers to canonical names.
   - Keep only columns needed for Roth basis analytics.

2) Field normalization
   - SSN (`ssn`):
       Normalize to a 9-digit string: strip non-digits, left pad via zfill(9),
       invalid -> <NA>.
   - First Roth tax year (`first_roth_tax_year`):
       Convert to pandas nullable integer (Int64) to preserve missingness.
   - Roth basis amount (`roth_basis_amt`):
       Convert to numeric via `pd.to_numeric(errors="coerce")`.
   - Text fields (plan_id, first_name, last_name):
       Strip whitespace.

3) Deduplication
   - Drop duplicates on (plan_id, ssn) while keeping the row with the most
     non-null year/basis data.

Expected output schema (canonical)
----------------------------------
- plan_id
- ssn
- first_name
- last_name
- first_roth_tax_year
- roth_basis_amt

Public API
----------
- clean_relius_roth_basis(raw_df: pd.DataFrame) -> pd.DataFrame

Privacy / compliance note
-------------------------
This project is designed for portfolio use with synthetic or masked data. Never
commit real participant PII (SSNs, names, addresses) to source control. Run the
production version only in secure environments with proper access controls.
"""

from __future__ import annotations

from typing import Iterable
import warnings

import pandas as pd
from .config import (
    RELIUS_ROTH_BASIS_COLUMN_MAP,
    RELIUS_ROTH_BASIS_CORE_COLUMNS,
)
from .normalizers import (
    build_validation_issues,
    normalize_plan_id_series,
    normalize_ssn_series,
    normalize_text_series,
    validate_amounts_series,
    validate_ssn_series,
    to_int64_nullable_series,
    to_numeric_series,
)

# --- Helper functions ------------------------------------------------------------

def _drop_unneeded_columns(df: pd.DataFrame, keep: Iterable[str]) -> pd.DataFrame:

    """Return a copy with only the requested columns (ignore extras)."""

    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()


# --- Main cleaning function ------------------------------------------------------

def clean_relius_roth_basis(raw_df: pd.DataFrame) -> pd.DataFrame:

    """
    Clean and normalize a Relius Roth basis export to canonical schema.

    Steps:
    1) Rename raw columns to canonical names.
    2) Keep only the Roth basis columns we care about.
    3) Normalize SSN, participant names, first Roth tax year, and basis amount.
    4) Deduplicate on (plan_id, ssn) favoring rows with non-null year/basis.
    """
    
    df = raw_df.copy()

    # 1) Standardize column names
    df = df.rename(columns=RELIUS_ROTH_BASIS_COLUMN_MAP)

    # 2) Keep only the core Roth basis columns
    df = _drop_unneeded_columns(df, RELIUS_ROTH_BASIS_CORE_COLUMNS)

    # 3) Normalize fields
    if "ssn" in df.columns:
        # Notebook smoke check:
        # df["ssn"].str.len().value_counts(dropna=False)
        # ["040511830", 40511830.0, "40511830.0"] -> "040511830"
        df["ssn"] = normalize_ssn_series(df["ssn"])
        invalid_mask = df["ssn"].isna() | (df["ssn"].str.len() != 9)
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            warnings.warn(
                f"Roth basis SSN normalization produced {invalid_count} invalid values.",
                stacklevel=2,
            )

    if "plan_id" in df.columns:
        df["plan_id"] = normalize_plan_id_series(df["plan_id"])

    for col in ["first_name", "last_name"]:
        if col in df.columns:
            df[col] = normalize_text_series(df[col], strip=True, upper=False)

    if "first_roth_tax_year" in df.columns:
        df["first_roth_tax_year"] = to_int64_nullable_series(df["first_roth_tax_year"])

    if "roth_basis_amt" in df.columns:
        df["roth_basis_amt"] = to_numeric_series(df["roth_basis_amt"])

    # Validation flags and issues
    ssn_valid = (
        validate_ssn_series(df["ssn"])
        if "ssn" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    amount_valid = (
        validate_amounts_series(df["roth_basis_amt"])
        if "roth_basis_amt" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    date_valid = pd.Series(pd.NA, index=df.index, dtype="boolean")
    code_1099r_valid = pd.Series(pd.NA, index=df.index, dtype="boolean")

    df["ssn_valid"] = ssn_valid
    df["amount_valid"] = amount_valid
    df["date_valid"] = date_valid
    df["code_1099r_valid"] = code_1099r_valid
    df["validation_issues"] = build_validation_issues(
        ssn_valid,
        amount_valid,
        date_valid,
        code_1099r_valid,
    )

    # 4) Deduplicate by identifiers, keeping the row with the most non-null signals
    if {"plan_id", "ssn"} <= set(df.columns):
        completeness_cols = ["first_roth_tax_year", "roth_basis_amt"]
        df["__completeness__"] = df[completeness_cols].notna().sum(axis=1)
        df = (
            df.sort_values("__completeness__", ascending=False)
            .drop_duplicates(subset=["plan_id", "ssn"], keep="first")
            .drop(columns="__completeness__")
        )

    return df

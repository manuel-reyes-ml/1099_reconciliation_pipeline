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

import re
from numbers import Integral, Real
from typing import Iterable
import warnings

import pandas as pd


ROTH_BASIS_COLUMN_MAP = {
    "PLANID": "plan_id",
    "SSNUM": "ssn",
    "LASTNAM": "last_name",
    "FIRSTNAM": "first_name",
    "FIRSTTAXYEARROTH": "first_roth_tax_year",
    "Total": "roth_basis_amt",
}

ROTH_BASIS_COLUMNS = [
    "plan_id",
    "ssn",
    "first_name",
    "last_name",
    "first_roth_tax_year",
    "roth_basis_amt",
]


# --- Helper functions ------------------------------------------------------------

def _normalize_ssn(value) -> str | pd.NA:

    """Normalize SSN to a 9-digit string; return <NA> for invalid/unsafe inputs."""

    if pd.isna(value):
        return pd.NA

    if isinstance(value, Integral) and not isinstance(value, bool):
        return f"{int(value):09d}"

    if isinstance(value, Real) and not isinstance(value, Integral):
        if pd.isna(value):
            return pd.NA
        if value.is_integer():
            return f"{int(value):09d}"
        return pd.NA

    value_str = str(value).strip()
    if re.match(r"^\d+\.0$", value_str):
        value_str = value_str[:-2]

    digits = re.sub(r"\D", "", value_str)
    if not digits:
        return pd.NA

    if len(digits) < 9:
        digits = digits.zfill(9)

    if len(digits) != 9:
        return pd.NA

    return digits


def _to_float(series: pd.Series) -> pd.Series:

    """Convert amounts to float with coercion for non-numeric noise."""

    return pd.to_numeric(series, errors="coerce")


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
    df = df.rename(columns=ROTH_BASIS_COLUMN_MAP)

    # 2) Keep only the core Roth basis columns
    df = _drop_unneeded_columns(df, ROTH_BASIS_COLUMNS)

    # 3) Normalize fields
    if "ssn" in df.columns:
        # Notebook smoke check:
        # df["ssn"].str.len().value_counts(dropna=False)
        # ["040511830", 40511830.0, "40511830.0"] -> "040511830"
        df["ssn"] = df["ssn"].map(_normalize_ssn).astype("string")
        invalid_mask = df["ssn"].isna() | (df["ssn"].str.len() != 9)
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            warnings.warn(
                f"Roth basis SSN normalization produced {invalid_count} invalid values.",
                stacklevel=2,
            )

    for col in ["plan_id", "first_name", "last_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "first_roth_tax_year" in df.columns:
        df["first_roth_tax_year"] = (
            pd.to_numeric(df["first_roth_tax_year"], errors="coerce").astype("Int64")
        )

    if "roth_basis_amt" in df.columns:
        df["roth_basis_amt"] = _to_float(df["roth_basis_amt"])

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

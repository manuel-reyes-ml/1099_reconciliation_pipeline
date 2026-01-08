# Docstring for src/clean_relius_demo module
"""
clean_relius_demo.py

Cleaning and normalization for Relius participant demographic exports.

This module cleans a Relius participant master/demographics export and
transforms it into a canonical, analysis-ready pandas DataFrame for
age-based tax-code analytics and downstream matching.

Inputs
------
- Relius export DataFrame with columns:
    PLANID, SSNUM, FIRSTNAM, LASTNAM, BIRTHDATE, TERM_DATE

Expected output schema (canonical)
----------------------------------
- plan_id
- ssn
- first_name
- last_name
- dob
- term_date
- ssn_valid
- amount_valid
- date_valid
- code_1099r_valid
- validation_issues

Public API
----------
- clean_relius_demo(raw_df: pd.DataFrame) -> pd.DataFrame

Privacy / compliance note
-------------------------
This project is designed for portfolio use with synthetic or masked data. Never
commit real participant PII (SSNs, names, addresses) to source control. Run the
production version only in secure environments with proper access controls.
"""

from __future__ import annotations

import warnings

import pandas as pd

from ..core.config import RELIUS_DEMO_COLUMN_MAP
from ..core.normalizers import (
    build_validation_issues,
    normalize_plan_id_series,
    normalize_ssn_series,
    normalize_text_series,
    to_date_series,
    validate_dates_series,
    validate_ssn_series,
)


def clean_relius_demo(raw_df: pd.DataFrame) -> pd.DataFrame:

    """

    Clean the Relius participant/master export that contains
    - plan_id
    - ssn
    - first_name
    - last_name
    - dob
    - term_date

    This is separate from the distribution export used in the
    inherited-plan matching engine.

    """

    df_raw = raw_df.copy()

    # Standardize column names so the mapping works robustly
    df_raw.columns = [c.strip().upper() for c in df_raw.columns]

    # Verify required columns exist
    required = list(RELIUS_DEMO_COLUMN_MAP.keys())
    missing = [c for c in required if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Relius demo file: {missing}")

    # Keep only mapped columns
    df = df_raw[required].copy()

    # Rename to canonical names
    df = df.rename(columns=RELIUS_DEMO_COLUMN_MAP)

    # Normalize SSN
    df["ssn"] = normalize_ssn_series(df["ssn"])
    invalid_mask = df["ssn"].isna() | (df["ssn"].str.len() != 9)
    invalid_count = int(invalid_mask.sum())
    if invalid_count > 0:
        warnings.warn(
            f"Relius demo SSN normalization produced {invalid_count} invalid values.",
            stacklevel=2,
        )

    # Normalize DOB and term_date to date objects
    df["dob"] = to_date_series(df["dob"])
    df["term_date"] = to_date_series(df["term_date"])

    # Normalize plan/name text fields
    df["plan_id"] = normalize_plan_id_series(df["plan_id"])
    df["first_name"] = normalize_text_series(df["first_name"], strip=True, upper=False)
    df["last_name"] = normalize_text_series(df["last_name"], strip=True, upper=False)

    # Validation flags and issues
    ssn_valid = (
        validate_ssn_series(df["ssn"])
        if "ssn" in df.columns
        else pd.Series(pd.NA, index=df.index, dtype="boolean")
    )
    amount_valid = pd.Series(pd.NA, index=df.index, dtype="boolean")
    if "term_date" in df.columns:
        date_valid = validate_dates_series(df["term_date"]).mask(df["term_date"].isna())
    else:
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

    # Drop rows with no usable SSN
    df = df[df["ssn"].notna()].copy()

    # If there are duplicates for the same (plan_id, ssn), keep the last
    df = (
        df.sort_values(["plan_id", "ssn"]).drop_duplicates(["plan_id", "ssn"], keep="last")
    )

    return df

# Docstring for src/age_taxcode_analysis module
"""
age_taxcode_analysis.py

Age-based 1099-R tax-code correction engine for Matrix distribution exports.

This module implements a standalone analysis flow that recommends 1099-R tax code
updates for 2025 Matrix distributions using participant demographic and
employment/termination data sourced from a separate Relius “participant master”
export (DOB, termination date by plan, participant identifiers).

Design goals
------------
- Operate independently from the inherited-plan matching engine
  (`match_transactions.py`) so the two workflows can be run separately.
- Reuse the existing correction-file builder (`build_correction_file.py`) by
  producing a DataFrame that contains:
    - match_status (e.g., "match_needs_correction")
    - action (e.g., "UPDATE_1099")
    - suggested_tax_code_1 / suggested_tax_code_2
    - traceability fields like transaction_id, ssn, plan_id, txn_date

Inputs
------
1) Matrix distributions (cleaned output from `clean_matrix.clean_matrix()`):
   Expected canonical columns include:
     - plan_id
     - ssn
     - txn_date
     - transaction_id
     - tax_code_1, tax_code_2 (normalized to 1–2 characters)
     - participant_name / matrix_account (optional but recommended)

2) Relius participant master / demographics file (via `clean_relius_demo()`):
   Expected canonical columns include:
     - plan_id
     - ssn
     - dob
     - term_date (optional; may be missing/blank)

Core business rules
-------------------
Non-Roth plans:
  1) If age at distribution >= 59.5 -> Tax Code 1 = "7"
  2) If age at distribution < 59.5:
     2.1) If term_date exists:
          - age at termination >= 55 -> Tax Code 1 = "2"
          - age at termination < 55  -> Tax Code 1 = "1"
     2.2) If term_date does NOT exist:
          - age at distribution < 55 -> Tax Code 1 = "1"
          - age at distribution >= 55 -> Tax Code 1 = "2"

Roth plans (identified by plan_id starting with "300005" OR plan_id ending in "R"):
  - Tax Code 1 must be "B"
  - Tax Code 2 is computed using the same age logic as above (1 / 2 / 7)

Exclusions (not processed by this engine)
----------------------------------------
- Matrix rows where tax_code_1 indicates rollover codes ("G", "H"), because those
  codes are driven by distribution type rather than age.
- Plans listed in INHERITED_PLAN_IDS, because inherited-plan logic is handled by
  the separate inherited matching/correction workflow.

Key outputs
-----------
`run_age_taxcode_analysis()` returns a DataFrame with expected/suggested codes and
classification fields. Rows with `match_status == "match_needs_correction"` can
be passed directly into `build_correction_dataframe()` to generate the Excel
correction file.

Important notes
---------------
- This module does not require matching by dollar amount; it joins Matrix to
  Relius demographics using (plan_id, ssn).
- Age calculations are based on month-level differences to handle month/day
  boundaries more accurately than naive year subtraction.
- All repository data should remain synthetic or masked when published. Never
  commit real SSNs, DOBs, or termination dates.

Public API
----------
- clean_relius_demo(path) -> pd.DataFrame
- run_age_taxcode_analysis(matrix_df, relius_demo_df) -> pd.DataFrame
"""


from __future__ import annotations
from pathlib import Path

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .config import AGE_TAXCODE_CONFIG, INHERITED_PLAN_IDS

from .normalizers import (
    normalize_ssn_series,
    normalize_tax_code_series,
    normalize_text_series,
    to_date_series,
)

import warnings



RELIUS_DEMO_COLUMN_MAP = {
    "PLANID": "plan_id",
    "SSNUM" : "ssn",
    "FIRSTNAM" : "first_name",
    "LASTNAM" : "last_name",
    "BIRTHDATE" : "dob",
    "TERM_DATE" : "term_date"
}

def clean_relius_demo(path: Path | str | bytes) -> pd.DataFrame:

    """
    
    Clean the Relius participant/master file that contains
    - plan_id
    - ssn 
    - first_name
    - last_name 
    - dob
    - term_date

    This is separate from the distribution export used in the
    inherited-plan matching engine.
    
    """

    df_raw = pd.read_excel(path, dtype=str)

    # Standardize column names so the mapping woek robustly
    df_raw.columns = [c.strip().upper() for c in df_raw.columns]

    # Verify required columns exist
    required = list(RELIUS_DEMO_COLUMN_MAP.keys())
    missing = [c for c in required if c not in df_raw.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Relius demo file: {missing}")
    
    # Kepp only mapped columns
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
    df["plan_id"] = normalize_text_series(df["plan_id"], strip=True, upper=False)
    df["first_name"] = normalize_text_series(df["first_name"], strip=True, upper=False)
    df["last_name"] = normalize_text_series(df["last_name"], strip=True, upper=False)

    # Drop rows with no usable SSN
    df = df[df["ssn"].notna()].copy()

    # If there are duplicates for the same (plan_id, ssn), keep the last
    df = (
        df.sort_values(["plan_id", "ssn"])
        .drop_duplicates(["plan_id", "ssn"], keep="last")

    )

    return df



def attach_demo_to_matrix(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
) -> pd.DataFrame:
    
    """
    
    Join cleaned Matrix distributions with Relius demographics (DOB, term_date)
    using plan_id + ssn as keys.

    Assumes 'matrix_df' is the output of 'clean_matrix.clean_matrix()'
    
    """

    key_cols = ["plan_id", "ssn"]

    demo_cols = ["dob", "term_date", "first_name", "last_name"]
    demo = relius_demo_df[key_cols + demo_cols].copy()

    merged = matrix_df.merge(
        demo,
        on=key_cols,
        how="left",
        suffixes=("", "_demo")
    )

    # Defensive: make sure these are dates
    merged["dob"] = to_date_series(merged["dob"])
    merged["term_date"] = to_date_series(merged["term_date"])

    # Prefer Matrix participant_name; fall back to Relius first/last is missing
    if "participant_name" in merged.columns:
        merged["full_name"] = merged["participant_name"]
    else:
        merged["full_name"] = pd.NA
    
    mask_missing = merged["full_name"].isna()
    merged.loc[mask_missing, "full_name"] = (
        merged.loc[mask_missing, "first_name"].fillna("").str.strip()
        + " "
        + merged.loc[mask_missing, "last_name"].fillna("").str.strip()
    ).str.strip().replace("", pd.NA)

    return merged



def _compute_age_years(dob_series: pd.Series, asof_series: pd.Series) -> pd.Series:

    """
    
    Compute age in years (float) between DOB and  an 'as of' date for each row.

    - Uses month-based difference to handle month/day boundaries
      more accurately that naive year subtraction.
    - Returns float with NaN where DOB or as-of is missing/invalid.
    
    """

    dob = pd.to_datetime(dob_series, errors="coerce")
    asof = pd.to_datetime(asof_series, errors="coerce")

    age_years = pd.Series(pd.NA, index=dob.index, dtype="Float64")

    valid = dob.notna() & asof.notna()
    if not valid.any():
        return age_years
    
    dob_v = dob[valid]
    asof_v = asof[valid]

    months = (asof_v.dt.year - dob_v.dt.year) * 12 + (asof_v.dt.month - dob_v.dt.month)
    months -= (asof_v.dt.day < dob_v.dt.day).astype(int)

    age_years.loc[valid] = months / 12

    return age_years



def _is_roth_plan_id(plan_id: object) -> bool:

    """
    
    Identify Roth plans by plan_id pattern:

        - Any plan_id starting with '300005'
        - Any plan_id ending with 'R'
    
    Adjust if business rules change.

    """

    if pd.isna(plan_id):
        return False

    s = str(plan_id).strip().upper()

    return s.startswith("300005") or s.endswith("R")



def run_age_taxcode_analysis(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
) -> pd.DataFrame:
    
    """
    
    Apply age-based 1099-R tax-code rules to Matrix distributions using DOB and termination data from Relius.

    Business rules:

        1) If participant was >= 59.5 on the Matrix transaction date -> code 7.

        2) If participant was < 59.5 on the transaction date:
            2.1) If we have a term date for that plan:
                - age at termination >= 55 -> code 2
                - age at termination < 55 -> code 1
            2.2) If we do NOT have a term date:
                - if age at distribution (2025) < 55  -> code 1
                - if age at distribution (2025) >= 55 -> code 2

    Additional Roth rules:

        - Roth plans are those where:
            * plan_id starts with '300005', OR
            * plan_id ends with 'R'.
        
        - For Roth plans, expected codes are:
            * Tax code 1: 'B'
            * Tax code 2: '1', '2', or '7' (based on the same logic above).
        
    Additional constraints specific to this engine:

        - Exclude Matrix rows where tax_code_1 indicates a rollover (G,H).
        - Exclude plans that are in INHERITED_PLAN_IDS, since those are
          handled by the inherited-plan engine (code 4 / 4+G).
        
    Returns a DataFrame that is compatible with build_correction_dataframe():
        - includes 'tax_code_1'
        - includes 'suggested_tax_code_1' / 'suggested_tax_code_2'
        - includes 'match_status', 'action', 'correction_reason'
    
    """

    cfg = AGE_TAXCODE_CONFIG

    # 1) Attached demographics (DOB, termm_date, names) to Matrix data
    df = attach_demo_to_matrix(matrix_df, relius_demo_df)

    # Normalize tax codes defensively to ensure 1–2 character codes
    for col in ["tax_code_1", "tax_code_2"]:
        if col in df.columns:
            df[col] = normalize_tax_code_series(df[col])
            lengths = df[col].str.len()
            invalid_tax = df[col].notna() & lengths.gt(2)
            invalid_tax_count = int(invalid_tax.sum())
            if invalid_tax_count > 0:
                warnings.warn(
                    f"Age tax code normalization produced {invalid_tax_count} values longer than 2 characters.",
                    stacklevel=2,
                )



    # 3) Flags: rollover, inherited, Roth
    mask_rollover_code = df["tax_code_1"].isin(cfg.excluded_codes)   # G, H
    mask_inherited_plan = df["plan_id"].isin(INHERITED_PLAN_IDS)
    df["is_roth_plan"] = df["plan_id"].apply(_is_roth_plan_id)

    # Exclude rollover and inherited plans from this engine
    df["age_engine_excluded"] = mask_rollover_code | mask_inherited_plan

    # 4) Compute ages
    df["age_at_distribution"] = _compute_age_years(df["dob"], df["txn_date"])
    df["age_at_termination"] = _compute_age_years(df["dob"], df["term_date"])

    has_dob = df["age_at_distribution"].notna()
    has_term = df["age_at_termination"].notna()
    eligible_any = ~df["age_engine_excluded"] & has_dob

    eligible_roth = eligible_any & df["is_roth_plan"]
    eligible_non_roth = eligible_any & ~df["is_roth_plan"]

    # 5) Initialize expected codes and metadata
    df["expected_tax_code_1"] = pd.NA
    df["expected_tax_code_2"] = pd.NA
    df["correction_reason"] = pd.NA
    df["action"] = pd.NA

    # Default match_status
    df["match_status"] = "age_rule_insufficient_data"
    df.loc[df["age_engine_excluded"], "match_status"] = (
        "excluded_from_age_engine_rollover_or_inherited"
    )


    # ------------------------------------------------------------
    # NON-ROTH AGE RULES (7 / 2 / 1 in tax_code_1)
    # ------------------------------------------------------------
    # Rule 1: age >= 59.5 at distribution → 7
    mask_normal_non_roth = (
        eligible_non_roth & (df["age_at_distribution"] >= cfg.normal_age_years)
    )
    df.loc[mask_normal_non_roth, "expected_tax_code_1"] = cfg.normal_dist_code
    df.loc[mask_normal_non_roth, "correction_reason"] = "age_59_5_or_over_normal_distribution"

    # Rule 2: age < 59.5
    mask_under_595_non_roth = eligible_non_roth & ~mask_normal_non_roth

    # 2.1 with term date
    mask_under_595_with_term_non = mask_under_595_non_roth & has_term

    # Term age >= 55 → 2
    mask_term_55_plus_non = mask_under_595_with_term_non & (
        df["age_at_termination"] >= cfg.term_rule_age_years
    )
    df.loc[mask_term_55_plus_non, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_term_55_plus_non, "correction_reason"] = "terminated_at_or_after_55"

    # Term age < 55 → 1
    mask_term_under_55_non = mask_under_595_with_term_non & (
        df["age_at_termination"] < cfg.term_rule_age_years
    )
    df.loc[mask_term_under_55_non, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_term_under_55_non, "correction_reason"] = "terminated_before_55"

    # 2.2 no term date → use age at distribution vs 55
    mask_under_595_no_term_non = mask_under_595_non_roth & ~has_term

    # <55 → 1
    mask_dist_under_55_non = mask_under_595_no_term_non & (
        df["age_at_distribution"] < cfg.term_rule_age_years
    )
    df.loc[mask_dist_under_55_non, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_dist_under_55_non, "correction_reason"] = "no_term_date_under_55_in_2025"

    # >=55 → 2
    mask_dist_55_plus_non = mask_under_595_no_term_non & (
        df["age_at_distribution"] >= cfg.term_rule_age_years
    )
    df.loc[mask_dist_55_plus_non, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_dist_55_plus_non, "correction_reason"] = "no_term_date_55_plus_in_2025"


    # ------------------------------------------------------------
    # ROTH AGE RULES (B in tax_code_1, 1/2/7 in tax_code_2)
    # ------------------------------------------------------------
    # Rule 1: age >= 59.5 at distribution → B / 7
    mask_normal_roth = (
        eligible_roth & (df["age_at_distribution"] >= cfg.normal_age_years)
    )
    df.loc[mask_normal_roth, "expected_tax_code_1"] = "B"
    df.loc[mask_normal_roth, "expected_tax_code_2"] = "7"
    df.loc[mask_normal_roth, "correction_reason"] = "roth_age_59_5_or_over_qualified"

    # Rule 2: age < 59.5
    mask_under_595_roth = eligible_roth & ~mask_normal_roth

    # 2.1 with term date
    mask_under_595_with_term_roth = mask_under_595_roth & has_term

    # Term age >= 55 → B / 2
    mask_term_55_plus_roth = mask_under_595_with_term_roth & (
        df["age_at_termination"] >= cfg.term_rule_age_years
    )
    df.loc[mask_term_55_plus_roth, "expected_tax_code_1"] = "B"
    df.loc[mask_term_55_plus_roth, "expected_tax_code_2"] = "2"
    df.loc[mask_term_55_plus_roth, "correction_reason"] = "roth_terminated_at_or_after_55"

    # Term age < 55 → B / 1
    mask_term_under_55_roth = mask_under_595_with_term_roth & (
        df["age_at_termination"] < cfg.term_rule_age_years
    )
    df.loc[mask_term_under_55_roth, "expected_tax_code_1"] = "B"
    df.loc[mask_term_under_55_roth, "expected_tax_code_2"] = "1"
    df.loc[mask_term_under_55_roth, "correction_reason"] = "roth_terminated_before_55"

    # 2.2 no term date → use age at distribution vs 55
    mask_under_595_no_term_roth = mask_under_595_roth & ~has_term

    # <55 → B / 1
    mask_dist_under_55_roth = mask_under_595_no_term_roth & (
        df["age_at_distribution"] < cfg.term_rule_age_years
    )
    df.loc[mask_dist_under_55_roth, "expected_tax_code_1"] = "B"
    df.loc[mask_dist_under_55_roth, "expected_tax_code_2"] = "1"
    df.loc[mask_dist_under_55_roth, "correction_reason"] = "roth_no_term_date_under_55_in_2025"

    # >=55 → B / 2
    mask_dist_55_plus_roth = mask_under_595_no_term_roth & (
        df["age_at_distribution"] >= cfg.term_rule_age_years
    )
    df.loc[mask_dist_55_plus_roth, "expected_tax_code_1"] = "B"
    df.loc[mask_dist_55_plus_roth, "expected_tax_code_2"] = "2"
    df.loc[mask_dist_55_plus_roth, "correction_reason"] = "roth_no_term_date_55_plus_in_2025"


    # ------------------------------------------------------------
    # 6) Compare expected vs current codes
    #    - Non-Roth: compare tax_code_1 only
    #    - Roth: compare both tax_code_1 AND tax_code_2
    # ------------------------------------------------------------
    code1 = df["tax_code_1"].fillna("")
    code2 = df["tax_code_2"].fillna("")

    exp1 = df["expected_tax_code_1"].fillna("")
    exp2 = df["expected_tax_code_2"].fillna("")

    has_expected = df["expected_tax_code_1"].notna()

    matches_non_roth = (
        has_expected
        & ~df["is_roth_plan"]
        & (code1 == exp1)
    )

    matches_roth = (
        has_expected
        & df["is_roth_plan"]
        & (code1 == exp1)
        & (code2 == exp2)
    )

    df["code_matches_expected"] = matches_non_roth | matches_roth

    # perfect_match where codes match expectation
    df.loc[df["code_matches_expected"], "match_status"] = "perfect_match"

    # needs correction where we have expected code but Matrix differs
    need_corr_mask = has_expected & ~df["code_matches_expected"] & ~df["age_engine_excluded"]
    df.loc[need_corr_mask, "match_status"] = "match_needs_correction"
    df.loc[need_corr_mask, "action"] = "UPDATE_1099"

    # 7) Suggested codes for correction file builder
    df["suggested_tax_code_1"] = df["expected_tax_code_1"]
    df["suggested_tax_code_2"] = df["expected_tax_code_2"]

    return df

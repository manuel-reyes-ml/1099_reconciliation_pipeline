# Docstring for src/age_taxcode_analysis module


from __future__ import annotations
from pathlib import Path

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .config import AGE_TAXCODE_CONFIG, INHERITED_PLAN_IDS



RELIUS_DEMO_COLUMN_MAP = {
    "PLANID": "plan_id",
    "SSNUM" : "ssn",
    "FIRSTNAM" : "first_name",
    "LASTNAM" : "last_name",
    "BIRTHDATE" : "dob",
    "TERM_DATE" : "term_date"
}



def _normalize_ssn(val: str) -> Optional[str]:

    """
    
    Normalize SSN to a 9-digit string:
    - strip non-digits
    - if > 9 digits, keep first 9(handles Excel '194562032.0')
    - left pad with zeros to length 9
    - return <NA> if nothing usable
    
    """
    
    if pd.isna(val):
        return pd.NA
    
    digits = "".join(ch for ch in str(val) if ch.isdigit())
    if not digits:
        return pd.NA
        
    # If Excel stored as float, it may have an extra digit from ".0"
    if len(digits) > 9:
        digits = digits[:9]
        
    return digits.zfill(9)




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
    df["ssn"] = df["ssn"].apply(_normalize_ssn)

    # Normalize DOB and term_date to date objects
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce").dt.date
    df["term_date"] = pd.to_datetime(df["term_date"], errors="coerce").dt.date

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

    # Defensive: make sure these are ates
    merged["dob"] = pd.to_datetime(merged["dob"], errors="coerce").dt.date
    merged["term_date"] = pd.to_datetime(merged["term_date"], errors="coerce").dt.date

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


    # -----------------------------------------------------------------------------
    # A) Determine which rows are EXCLUDED from this age-based engine:
    #   - rollovers cods G / H
    #   - inherited-plan IDs (handled by other engine)
    # -----------------------------------------------------------------------------

    # tax_code_1 should already be normalized by clean_matrix, but we
    # enforce string + strip defensively.
    tax1_norm = df["tax_code_1"].astype(str).str.strip().fillna("")
    df["tax_code_1"] = tax1_norm                                                   # keep normalized verion

    mask_rollover_code = tax1_norm.isin(cfg.excluded_codes)
    mask_inherited_plan = df["plan_id"].isin(INHERITED_PLAN_IDS)

    df["age_engine_excluded"] = mask_rollover_code | mask_inherited_plan


    # 2) Compute ages
    df["age_at_distribution"] = _compute_age_years(df["dob"], df["txn_date"])
    df["age_at_termination"] = _compute_age_years(df["dob"], df["term_date"])

    # 3) Initialize expected codes and metadata
    df["expected_tax_code_1"] = pd.NA
    df["expected_tax_code_2"] = pd.NA
    df["correction_reason"] = pd.NA
    df["action"] = pd.NA


    # This is an independent engine, so we defined a local-style "match_satus"
    #   that build_correction_dataframe can use just in the inherited flow.
    #
    # Default match_status:
    # - excluded rows: explicit explanation
    # - others: "insufficient_data" unless rules assign something better
    df["match_status"] = "age_rule_insufficient_date"
    df.loc[df["age_engine_excluded"], "match_status"] = (
        "excluded_from_age_engine_rollover_or_inherited"
    )


    has_dob = df["age_at_distribution"].notna()
    has_term = df["age_at_termination"].notna()
    eligible = ~df["age_engine_excluded"]                                           # rows we are allowed to modify

    # 5) Rule 1 - age > 59.5 at distribution -> code 7 (normal)
    mask_normal = (
        eligible 
        & has_dob
        & (df["age_at_distribution"] >= cfg.normal_age_years)
    )
    df.loc[mask_normal, "expected_tax_code_1"] = cfg.normal_dist_code
    df.loc[mask_normal, "correction_reason"] = "age_59_5_or_over_normal_distribution"

    # 5) Rule 2 - age < 59.5 at distribution
    mask_under_595 = eligible & has_dob & ~mask_normal

    # 5.1) with term date (termination rule)
    mask_under_595_with_term = mask_under_595 & has_term

    # age at termination >= 55 -> code 2
    mask_term_55_plus = mask_under_595_with_term & (
        df["age_at_termination"] >= cfg.term_rule_age_years
    )

    df.loc[mask_term_55_plus, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_term_55_plus, "correction_reason"] = "terminated_at_or_after_55"

    # age at termination < 55 -> code 1
    mask_term_under_55 = mask_under_595_with_term & (
        df["age_at_termination"] < cfg.term_rule_age_years
    )
    df.loc[mask_term_under_55, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_term_under_55, "correction_reason"] = "terminated_before_55"

    # 5.2) No term date -> fallback based on age at distribuion (2025)
    mask_under_595_no_term = mask_under_595 & ~has_term

    # age < 55 -> code 1
    mask_dist_under_55 = mask_under_595_no_term & (
        df["age_at_distribution"] < cfg.term_rule_age_years
    )
    df.loc[mask_dist_under_55, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_dist_under_55, "correction_reason"] = "no_term_date_under_55_in_2025"

    # age >= 55 -> code 2
    mask_dist_55_plus = mask_under_595_no_term & (
        df["age_at_distribution"] >= cfg.term_rule_age_years
    )
    df.loc[mask_dist_55_plus, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_dist_55_plus, "correction_reason"] = "no_term_date_55_plus_in_2025"

    # 6) Compare expected vs actual Matrix tax code
    code1 = df["tax_code_1"].fillna("")
    exp1 = df["expected_tax_code_1"].fillna("")
    has_expected = df["expected_tax_code_1"].notna()

    df["code_matches_expected"] = has_expected & (code1 == exp1)

    # perfect_match where we *have* an expected code and it matches
    df.loc[has_expected & df["code_matches_expected"], "match_status"] = "perfect_match"

    # needs correction where we have expected code but Matrix differs
    need_corr_mask = has_expected & ~df["code_matches_expected"]
    df.loc[need_corr_mask, "match_status"] = "match_needs_correction"
    df.loc[need_corr_mask, "action"] = "UPDATE_1099"

    # 7) Suggested tax codes (for correction file builder)
    df["suggested_tax_code_1"] = df["expected_tax_code_1"]
    df["suggested_tax_code_2"] = df["expected_tax_code_2"]

    return df


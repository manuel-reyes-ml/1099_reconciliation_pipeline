# Docstring for src/age_taxcode_analysis module
"""
age_taxcode_analysis.py

Age-based 1099-R tax-code correction engine for Matrix distribution exports
(non-Roth only).

This module implements a standalone analysis flow that recommends 1099-R tax code
updates for 2025 Matrix distributions using participant demographic and
employment/termination data sourced from a separate Relius “participant master”
export (DOB, termination date by plan, participant identifiers).

Design goals
------------
- Operate independently from the inherited-plan matching engine
  (`match_planid.py`) so the two workflows can be run separately.
- Reuse the existing correction-file builder (`build_correction_file.py`) by
  producing a DataFrame that contains:
    - match_status (e.g., "match_needs_correction")
    - action (e.g., "UPDATE_1099")
    - suggested_tax_code_1 / suggested_tax_code_2
    - new_tax_code
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

2) Relius participant master / demographics file
   (load with `load_data.load_relius_demo_excel()`, then clean via
   `clean_relius_demo()`):
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

Roth plans (identified by config-driven prefixes/suffixes; defaults to plan_id
starting with "300005" or ending in "R"):
  - Tax Code 1 must be "B"
  - Tax Code 2 is computed using the same age logic as above (1 / 2 / 7)

Exclusions (not processed by this engine)
----------------------------------------
- Roth plans (config-driven prefixes/suffixes; defaults to plan_id starting with
  "300005" or ending in "R") are handled by `roth_taxable_analysis` (Engine C).
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
- Age thresholds are evaluated by attainment as of 12/31 of the transaction
  year (and term year when applicable).
- All repository data should remain synthetic or masked when published. Never
  commit real SSNs, DOBs, or termination dates.

Public API
----------
- load_relius_demo_excel(path) -> pd.DataFrame
- clean_relius_demo(raw_df) -> pd.DataFrame
- run_age_taxcode_analysis(matrix_df, relius_demo_df) -> pd.DataFrame
"""


from __future__ import annotations
import pandas as pd
from ..config import (
    AGE_TAXCODE_CONFIG,
    DateFilterConfig,
    INHERITED_PLAN_IDS,
    MATCH_STATUS_CONFIG,
    ROTH_TAXABLE_CONFIG,
)

from ..core.normalizers import (
    apply_date_filter,
    attained_age_by_year_end,
    normalize_tax_code_series,
    _is_roth_plan,
    to_date_series,
)

import warnings



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



def run_age_taxcode_analysis(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
        date_filter: DateFilterConfig | None = None,
) -> pd.DataFrame:
    
    """
    
    Apply age-based 1099-R tax-code rules to Matrix distributions using DOB and
    termination data from Relius. Roth plans are excluded; Roth tax-code logic
    lives in Engine C (roth_taxable_analysis).

    Business rules:

        1) If participant attains age 59.5 at any point in the transaction year
           -> code 7.

        2) If participant does NOT attain 59.5 in the transaction year:
            2.1) If we have a term date for that plan:
                - age at termination >= 55 -> code 2
                - age at termination < 55 -> code 1
            2.2) If we do NOT have a term date:
                - if will NOT reach 55 in txn year    -> code 1
                - if will reach 55 in txn year        -> code 2

    Additional Roth rules:

        - Roth plans are those where plan_id matches configured prefixes/suffixes
          (default: prefix '300005' or suffix 'R'), using case-insensitive
          matching after stripping whitespace.
        
        - For Roth plans, expected codes are:
            * Tax code 1: 'B'
            * Tax code 2: '1', '2', or '7' (based on the same logic above).
        
    Additional constraints specific to this engine:

        - Exclude Matrix rows where tax_code_1 indicates a rollover (G,H).
        - Exclude plans that are in INHERITED_PLAN_IDS, since those are
          handled by the inherited-plan engine (code 4 / 4+G).
        - Apply optional date filtering on Matrix txn_date (guardrail).
        
    Returns a DataFrame that is compatible with build_correction_dataframe():
        - includes 'tax_code_1'
        - includes 'suggested_tax_code_1' / 'suggested_tax_code_2'
        - includes 'new_tax_code'
        - includes 'match_status', 'action', 'correction_reason'
    
    """

    cfg = AGE_TAXCODE_CONFIG
    status_cfg = MATCH_STATUS_CONFIG

    # 1) Apply optional date filter, then attach demographics (DOB, term_date)
    matrix_filtered = apply_date_filter(matrix_df, "txn_date", date_filter=date_filter)
    df = attach_demo_to_matrix(matrix_filtered, relius_demo_df)

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

    # 3) Flags: rollover, inherited, Roth (Roth handled by Engine C)
    mask_rollover_code = df["tax_code_1"].isin(cfg.excluded_codes)   # G, H
    mask_inherited_plan = df["plan_id"].isin(INHERITED_PLAN_IDS)
    is_roth_plan = _is_roth_plan(
        df["plan_id"],
        ROTH_TAXABLE_CONFIG,
        case_insensitive=True,
    )

    # Filter out Roth rows entirely
    df = df[~is_roth_plan].copy()

    # Exclude rollover and inherited plans from this engine
    df["age_engine_excluded"] = mask_rollover_code | mask_inherited_plan

    # 4) Compute year fields and attained-age flags
    dob_dt = pd.to_datetime(df["dob"], errors="coerce")
    txn_dt = pd.to_datetime(df["txn_date"], errors="coerce")
    term_dt = pd.to_datetime(df["term_date"], errors="coerce")
    txn_year = txn_dt.dt.year
    term_year = term_dt.dt.year
    dob_year = dob_dt.dt.year

    attained_59_5 = attained_age_by_year_end(df["dob"], txn_year, years=59, months=6)
    attained_55_term = attained_age_by_year_end(df["dob"], term_year, years=55)
    attained_55_txn = attained_age_by_year_end(df["dob"], txn_year, years=55)

    # Diagnostics for notebooks (year-based ages)
    df["dob_year"] = dob_year.astype("Int64")
    df["txn_year"] = txn_year.astype("Int64")
    df["term_year"] = term_year.astype("Int64")
    df["age_at_distribution_year"] = (txn_year - dob_year).astype("Float64")
    df["age_at_termination_year"] = (term_year - dob_year).astype("Float64")
    df["attained_59_5_in_txn_year"] = attained_59_5
    df["attained_55_in_txn_year"] = attained_55_txn
    df["attained_55_in_term_year"] = attained_55_term

    has_dob = dob_dt.notna()
    has_txn_year = txn_year.notna()
    has_term_year = term_year.notna()
    eligible_any = ~df["age_engine_excluded"] & has_dob & has_txn_year

    # 5) Initialize expected codes and metadata
    df["expected_tax_code_1"] = pd.NA
    df["expected_tax_code_2"] = pd.NA
    df["correction_reason"] = pd.NA
    df["action"] = pd.NA

    # Default match_status
    df["match_status"] = status_cfg.insufficient_data
    df.loc[df["age_engine_excluded"], "match_status"] = (
        status_cfg.excluded_age_engine
    )


    # ------------------------------------------------------------
    # NON-ROTH AGE RULES (7 / 2 / 1 in tax_code_1)
    # ------------------------------------------------------------
    # Rule 1: age >= 59.5 at distribution → 7
    mask_normal_non_roth = eligible_any & attained_59_5
    df.loc[mask_normal_non_roth, "expected_tax_code_1"] = cfg.normal_dist_code
    df.loc[mask_normal_non_roth, "correction_reason"] = "age_59_5_or_over_normal_distribution"

    # Rule 2: age < 59.5
    mask_under_595_non_roth = eligible_any & ~mask_normal_non_roth

    # 2.1 with term date
    mask_under_595_with_term_non = mask_under_595_non_roth & has_term_year

    # Term age >= 55 → 2
    mask_term_55_plus_non = mask_under_595_with_term_non & attained_55_term
    df.loc[mask_term_55_plus_non, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_term_55_plus_non, "correction_reason"] = "terminated_at_or_after_55"

    # Term age < 55 → 1
    mask_term_under_55_non = mask_under_595_with_term_non & ~attained_55_term
    df.loc[mask_term_under_55_non, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_term_under_55_non, "correction_reason"] = "terminated_before_55"

    # 2.2 no term date → use age at distribution vs 55
    mask_under_595_no_term_non = mask_under_595_non_roth & ~has_term_year

    # <55 → 1
    mask_dist_under_55_non = mask_under_595_no_term_non & ~attained_55_txn
    df.loc[mask_dist_under_55_non, "expected_tax_code_1"] = cfg.under_55_code
    df.loc[mask_dist_under_55_non, "correction_reason"] = "no_term_date_under_55_in_txn_year"

    # >=55 → 2
    mask_dist_55_plus_non = mask_under_595_no_term_non & attained_55_txn
    df.loc[mask_dist_55_plus_non, "expected_tax_code_1"] = cfg.age_55_plus_code
    df.loc[mask_dist_55_plus_non, "correction_reason"] = "no_term_date_55_plus_in_txn_year"


    # ------------------------------------------------------------
    # 6) Compare expected vs current codes
    #    - Non-Roth: compare tax_code_1 only
    # ------------------------------------------------------------
    code1 = df["tax_code_1"].fillna("")
    code2 = df["tax_code_2"].fillna("")

    exp1 = df["expected_tax_code_1"].fillna("")
    exp2 = df["expected_tax_code_2"].fillna("")

    has_expected = df["expected_tax_code_1"].notna()

    matches_non_roth = has_expected & (code1 == exp1)

    df["code_matches_expected"] = matches_non_roth

    # no-action where codes match expectation
    df.loc[df["code_matches_expected"], "match_status"] = status_cfg.no_action

    # needs correction where we have expected code but Matrix differs
    need_corr_mask = has_expected & ~df["code_matches_expected"] & ~df["age_engine_excluded"]
    df.loc[need_corr_mask, "match_status"] = status_cfg.needs_correction
    df.loc[need_corr_mask, "action"] = "UPDATE_1099"
    df.loc[df["match_status"] == status_cfg.no_action, "correction_reason"] = pd.NA

    # 7) Suggested codes for correction file builder
    df["suggested_tax_code_1"] = df["expected_tax_code_1"]
    df["suggested_tax_code_2"] = df["expected_tax_code_2"]
    mask_no_action = df["match_status"] == status_cfg.no_action
    df.loc[mask_no_action, ["suggested_tax_code_1", "suggested_tax_code_2"]] = pd.NA

    # Compose combined new tax code (e.g., 7, B2) from suggested codes.
    s1 = df["suggested_tax_code_1"].astype("string").str.strip().str.upper().replace("", pd.NA)
    s2 = df["suggested_tax_code_2"].astype("string").str.strip().str.upper().replace("", pd.NA)
    df["new_tax_code"] = pd.NA
    df.loc[s1.notna() & s2.isna(), "new_tax_code"] = s1
    df.loc[s1.notna() & s2.notna(), "new_tax_code"] = (s1 + s2)
    df["new_tax_code"] = df["new_tax_code"].astype("string")

    return df

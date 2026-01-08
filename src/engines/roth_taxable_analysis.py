# Docstring for src/roth_taxable_analysis module
"""
roth_taxable_analysis.py

Engine C: Roth-taxable and age-based tax-code analysis for Matrix distributions.
Flags Roth transactions that may need 1099-R taxable adjustments, Roth
initial-year updates, or age-based tax-code corrections, producing a canonical
DataFrame compatible with `build_correction_dataframe()`
(match_status/action/suggested_tax_code_* present even if NA).

Design goals
------------
- Reuse correction builder: emit the fields build_correction_dataframe expects.
- Avoid false positives: only mark UPDATE when taxable differs beyond tolerance.
- Transparency: accumulate all triggered reasons in `correction_reason` for
  notebook review (newline-bullet joined).
- Configurable: thresholds/labels pulled from `RothTaxableConfig`
  (default `ROTH_TAXABLE_CONFIG`).

Inputs
------
1) Matrix distributions (cleaned):
   plan_id, ssn, txn_date, transaction_id, participant_name, matrix_account,
   gross_amt, fed_taxable_amt, roth_initial_contribution_year,
   tax_code_1, tax_code_2
2) Relius demographics:
   plan_id, ssn, dob (optional: term_date for age-based Roth tax codes)
3) Relius Roth basis:
   plan_id, ssn, first_roth_tax_year, roth_basis_amt

Scope
-----
- Roth plans only (config-driven prefixes/suffixes; defaults to plan_id starting
  with "300005" or ending in "R")
- Exclude inherited plans (INHERITED_PLAN_IDS)
- Do NOT exclude rollovers

Core rules (priority)
---------------------
1) Basis coverage:
   - If roth_basis_amt >= gross_2025_total (per plan_id+ssn, txn_year==2025)
     suggest taxable = 0 (no UPDATE if Matrix already 0).
2) Qualified Roth:
   - Attained 59.5 within txn_year (dob + 59y6m on/before 12/31 of txn_year)
     AND (txn_year - valid_start_year) >= 5
     suggest taxable = 0 (no UPDATE if Matrix already 0).
3) Roth initial year mismatch:
   - If valid basis year differs from Matrix initial year -> suggest basis year
     and flag correction.
4) Invalid/missing basis year (0/NA):
   - Flag for review (no suggested year).
5) 15% proximity:
   - If fed_taxable_amt > 0 and gross_amt <= fed_taxable_amt * 1.15 and no prior
     correction/review -> flag for review.
6) Roth age-based tax codes:
   - Non-rollover Roth distributions: tax code 1 must be "B"; tax code 2 follows
     year-end attainment rules:
     * attained 59.5 within txn_year -> "7"
     * else if term_date exists -> "2" if attained 55 within term_year else "1"
     * else -> "2" if attained 55 within txn_year else "1"
   - Roth rollovers: normalized to tax code 1 = "H" (examples: B+G => H,
     G+blank => H, blank+G => H), and Roth rollovers are NOT evaluated under
     the non-rollover age-based B* expectations. Engine C still runs taxable,
     basis, and year checks on rollover-coded rows.

Correction vs review handling
-----------------------------
- Uses abs diff > 0.01 to decide if taxable needs UPDATE.
- Missing fed_taxable_amt -> review (INVESTIGATE) rather than UPDATE.
- Multiple reasons may apply; `correction_reason` joins all reason tokens.

Public API
----------
- run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df) -> pd.DataFrame
"""

from __future__ import annotations   # makes type hints ("annotations") be stored as strings.

import pandas as pd

from ..core.config import (                # Relative import from the core config module.
    AGE_TAXCODE_CONFIG,
    INHERITED_PLAN_IDS,
    MATCH_STATUS_CONFIG,
    ROTH_TAXCODE_CONFIG,
    ROTH_TAXABLE_CONFIG,
    RothTaxableConfig,
)
from ..core.normalizers import (
    _append_action,
    _append_reason,
    _compute_age_years,
    _compute_start_year,
    _is_roth_plan,
    _to_datetime,
    attained_age_by_year_end,
    normalize_plan_id_series,
    normalize_tax_code_series,
    to_numeric_series,
)


def run_roth_taxable_analysis(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
        relius_roth_basis_df: pd.DataFrame,
        cfg: RothTaxableConfig = ROTH_TAXABLE_CONFIG,
) -> pd.DataFrame:
    """
    Run Engine C Roth taxable analysis and return a canonical DataFrame with
    correction flags/suggestions.
    
    Note: `correction_reason` joins all triggered reasons with '; ' for quick notebook review.
    """
    
    df = matrix_df.copy()
    status_cfg = MATCH_STATUS_CONFIG
    df["plan_id"] = normalize_plan_id_series(df["plan_id"], string_dtype=False)

    mask_roth = _is_roth_plan(df["plan_id"], cfg)
    mask_not_inherited = ~df["plan_id"].isin(INHERITED_PLAN_IDS)
    df = df[mask_roth & mask_not_inherited].copy()

    demo_cols = [c for c in ["plan_id", "ssn", "dob", "term_date"] if c in relius_demo_df.columns]
    basis_cols = ["plan_id", "ssn", "first_roth_tax_year", "roth_basis_amt"]

    df = df.merge(relius_demo_df[demo_cols], on=["plan_id", "ssn"], how="left")
    df = df.merge(relius_roth_basis_df[basis_cols], on=["plan_id", "ssn"], how="left")

    df["txn_date"] = _to_datetime(df["txn_date"])
    df["dob"] = _to_datetime(df["dob"])
    if "term_date" not in df.columns:
        df["term_date"] = pd.NaT
    df["term_date"] = _to_datetime(df["term_date"])
    df["correction_reasons"] = [[] for _ in range(len(df))]  # collect all triggered reasons per row
    df["actions"] = [[] for _ in range(len(df))]

    df["txn_year"] = df["txn_date"].dt.year
    df["term_year"] = df["term_date"].dt.year
    df["age_at_txn"] = _compute_age_years(df["dob"], df["txn_date"])
    df["age_at_termination"] = _compute_age_years(df["dob"], df["term_date"])

    df["gross_amt"] = to_numeric_series(df["gross_amt"])
    df["fed_taxable_amt"] = to_numeric_series(df.get("fed_taxable_amt", pd.Series(pd.NA, index=df.index)))
    df["roth_basis_amt"] = to_numeric_series(df["roth_basis_amt"])

    df["first_roth_tax_year"] = to_numeric_series(df["first_roth_tax_year"])
    df["roth_initial_contribution_year"] = to_numeric_series(df["roth_initial_contribution_year"])

    first_year_valid = (
        df["first_roth_tax_year"].notna()
        & df["first_roth_tax_year"].gt(0)
        & df["first_roth_tax_year"].between(cfg.valid_year_min, cfg.valid_year_max)
    )

    start_year = _compute_start_year(df)
    start_year_valid = (
        start_year.notna()
        & start_year.gt(0)
        & start_year.between(cfg.valid_year_min, cfg.valid_year_max)
    )
    df["start_roth_year"] = start_year.where(start_year_valid)

    mask_2025 = df["txn_year"] == cfg.basis_coverage_year
    gross_2025 = (
        df.loc[mask_2025]
        .groupby(["plan_id", "ssn"])["gross_amt"]
        .sum(min_count=1)
    )
    df["gross_2025_total"] = df.set_index(["plan_id", "ssn"]).index.map(gross_2025)

    df["suggested_tax_code_1"] = pd.NA
    df["suggested_tax_code_2"] = pd.NA
    df["suggested_taxable_amt"] = pd.NA
    df["action"] = pd.NA
    df["match_status"] = pd.NA
    df["correction_reason"] = pd.NA
    raw_missing_first_year = ~first_year_valid

    # Normalize current tax codes
    current_code1 = normalize_tax_code_series(df.get("tax_code_1", pd.Series(pd.NA, index=df.index))).fillna("")
    current_code2 = normalize_tax_code_series(df.get("tax_code_2", pd.Series(pd.NA, index=df.index))).fillna("")

    tc_cfg = ROTH_TAXCODE_CONFIG
    mask_engine_excluded = current_code1.isin(tc_cfg.excluded_codes_taxcode)
    mask_taxcode_locked = (current_code1 == tc_cfg.roth_rollover_code) | (
        (current_code1 == tc_cfg.roth_code) & (current_code2 == tc_cfg.death_code)
    )
    df["tax_code_locked"] = mask_taxcode_locked

    # Roth tax-code correction rules (pre-taxable)
    mask_fix_b_g = (current_code1 == tc_cfg.roth_code) & (current_code2 == tc_cfg.rollover_code) & ~mask_engine_excluded
    mask_fix_g_4 = (current_code1 == tc_cfg.rollover_code) & (current_code2 == tc_cfg.death_code) & ~mask_engine_excluded
    mask_fix_4_blank = (current_code1 == tc_cfg.death_code) & (current_code2 == "") & ~mask_engine_excluded
    mask_fix_blank_4 = (current_code2 == tc_cfg.death_code) & (current_code1 == "") & ~mask_engine_excluded
    mask_fix_g_blank = (current_code1 == tc_cfg.rollover_code) & (current_code2 == "") & ~mask_engine_excluded
    mask_fix_blank_g = (current_code1 == "") & (current_code2 == tc_cfg.rollover_code) & ~mask_engine_excluded

    df.loc[mask_fix_b_g, "suggested_tax_code_1"] = tc_cfg.roth_rollover_code
    _append_reason(df, mask_fix_b_g, "roth_rollover_code_fix_B_G_to_H")
    _append_action(df, mask_fix_b_g, tc_cfg.action_update)

    df.loc[mask_fix_g_4, "suggested_tax_code_1"] = tc_cfg.roth_rollover_code
    df.loc[mask_fix_g_4, "suggested_tax_code_2"] = tc_cfg.death_code
    _append_reason(df, mask_fix_g_4, "roth_rollover_code_fix_G_4_to_H_4")
    _append_action(df, mask_fix_g_4, tc_cfg.action_update)

    df.loc[mask_fix_4_blank, "suggested_tax_code_1"] = tc_cfg.roth_code
    df.loc[mask_fix_4_blank, "suggested_tax_code_2"] = tc_cfg.death_code
    _append_reason(df, mask_fix_4_blank, "roth_death_code_fix_4_to_B_4")
    _append_action(df, mask_fix_4_blank, tc_cfg.action_update)

    df.loc[mask_fix_blank_4, "suggested_tax_code_1"] = tc_cfg.roth_code
    df.loc[mask_fix_blank_4, "suggested_tax_code_2"] = tc_cfg.death_code
    _append_reason(df, mask_fix_blank_4, "roth_death_code_fix_blank_4_to_B_4")
    _append_action(df, mask_fix_blank_4, tc_cfg.action_update)

    df.loc[mask_fix_g_blank, "suggested_tax_code_1"] = tc_cfg.roth_rollover_code
    df.loc[mask_fix_g_blank, "suggested_tax_code_2"] = pd.NA
    _append_reason(df, mask_fix_g_blank, "roth_rollover_code_fix_G_blank_to_H")
    _append_action(df, mask_fix_g_blank, tc_cfg.action_update)

    df.loc[mask_fix_blank_g, "suggested_tax_code_1"] = tc_cfg.roth_rollover_code
    df.loc[mask_fix_blank_g, "suggested_tax_code_2"] = pd.NA
    _append_reason(df, mask_fix_blank_g, "roth_rollover_code_fix_blank_G_to_H")
    _append_action(df, mask_fix_blank_g, tc_cfg.action_update)

    mask_taxcode_override = (
        mask_fix_b_g
        | mask_fix_g_4
        | mask_fix_4_blank
        | mask_fix_blank_4
        | mask_fix_g_blank
        | mask_fix_blank_g
    )

    # Taxable / basis / year logic (active rows only)
    active_mask = ~mask_engine_excluded

    basis_mask = (
        active_mask
        & df["roth_basis_amt"].notna()
        & df["gross_2025_total"].notna()
        & (df["roth_basis_amt"] >= df["gross_2025_total"])
    )
    df.loc[basis_mask, "suggested_taxable_amt"] = 0.0

    age_cfg = AGE_TAXCODE_CONFIG
    normal_age_years = int(age_cfg.normal_age_years)
    normal_age_months = int(round((age_cfg.normal_age_years - normal_age_years) * 12))
    term_rule_years = int(age_cfg.term_rule_age_years)
    term_rule_months = int(round((age_cfg.term_rule_age_years - term_rule_years) * 12))
    qualified_age_years = int(cfg.qualified_age_years)
    qualified_age_months = int(round((cfg.qualified_age_years - qualified_age_years) * 12))

    attained_59_5_in_txn_year = attained_age_by_year_end(
        df["dob"], df["txn_year"], years=normal_age_years, months=normal_age_months
    )
    attained_qualified_in_txn_year = attained_age_by_year_end(
        df["dob"], df["txn_year"], years=qualified_age_years, months=qualified_age_months
    )
    attained_55_in_txn_year = attained_age_by_year_end(
        df["dob"], df["txn_year"], years=term_rule_years, months=term_rule_months
    )
    attained_55_in_term_year = attained_age_by_year_end(
        df["dob"], df["term_year"], years=term_rule_years, months=term_rule_months
    )
    raw_qualified_mask = (
        active_mask
        & attained_qualified_in_txn_year
        & start_year_valid
        & (df["txn_year"] - start_year).ge(cfg.qualified_years_since_first)
    )
    qualified_mask = df["suggested_taxable_amt"].isna() & raw_qualified_mask
    df.loc[qualified_mask, "suggested_taxable_amt"] = 0.0

    taxable_suggested = active_mask & df["suggested_taxable_amt"].notna()
    taxable_missing_current = taxable_suggested & df["fed_taxable_amt"].isna()
    taxable_change_required = taxable_suggested & df["fed_taxable_amt"].notna() & (
        (df["fed_taxable_amt"] - df["suggested_taxable_amt"]).abs() > 0.01
    )
    roth_year_change_required = (
        active_mask
        & first_year_valid
        & (df["roth_initial_contribution_year"].isna() | df["roth_initial_contribution_year"].ne(df["first_roth_tax_year"]))
    )

    df["suggested_first_roth_tax_year"] = pd.NA
    df.loc[roth_year_change_required, "suggested_first_roth_tax_year"] = df.loc[
        roth_year_change_required, "first_roth_tax_year"
    ]

    df["match_status"] = status_cfg.no_action
    df["action"] = pd.NA

    _append_action(df, roth_year_change_required, cfg.action_update)
    _append_action(df, taxable_missing_current, cfg.action_investigate)
    missing_first_year_mask = active_mask & raw_missing_first_year
    _append_action(df, missing_first_year_mask, cfg.action_investigate)
    _append_action(df, taxable_change_required, cfg.action_update)

    raw_proximity_mask = (
        active_mask
        & df["fed_taxable_amt"].gt(0)
        & df["gross_amt"].le(df["fed_taxable_amt"] * (1 + cfg.taxable_proximity_pct))
    )
    proximity_mask = raw_proximity_mask
    _append_action(df, proximity_mask, cfg.action_investigate)

    # Roth age-based tax code expectations (Engine C now owns Roth tax codes)
    df["expected_tax_code_1"] = tc_cfg.roth_code
    df["expected_tax_code_2"] = pd.NA

    has_dob = df["dob"].notna()
    has_txn_year = df["txn_year"].notna()
    has_term_year = df["term_year"].notna()
    mask_age_applicable = (
        active_mask
        & ~mask_taxcode_override
        & ~mask_taxcode_locked
        & has_dob
        & has_txn_year
    )
    mask_age_normal = mask_age_applicable & attained_59_5_in_txn_year
    mask_under_normal = mask_age_applicable & ~mask_age_normal

    mask_under_with_term = mask_under_normal & has_term_year
    mask_term_55_plus = mask_under_with_term & attained_55_in_term_year
    mask_term_under_55 = mask_under_with_term & ~attained_55_in_term_year

    mask_under_no_term = mask_under_normal & ~has_term_year
    mask_dist_under_55 = mask_under_no_term & ~attained_55_in_txn_year
    mask_dist_55_plus = mask_under_no_term & attained_55_in_txn_year

    df.loc[mask_age_normal, "expected_tax_code_2"] = "7"
    df.loc[mask_term_55_plus, "expected_tax_code_2"] = "2"
    df.loc[mask_term_under_55, "expected_tax_code_2"] = "1"
    df.loc[mask_dist_under_55, "expected_tax_code_2"] = "1"
    df.loc[mask_dist_55_plus, "expected_tax_code_2"] = "2"

    expected_code2 = df["expected_tax_code_2"].fillna("")

    age_code_mismatch = mask_age_applicable & (
        (current_code1 != tc_cfg.roth_code)
        | (df["expected_tax_code_2"].notna() & (current_code2 != expected_code2))
    )

    # Only set age-based suggestions when not already set by tax-code overrides
    s1_age_mask = age_code_mismatch & df["suggested_tax_code_1"].isna()
    s2_age_mask = age_code_mismatch & df["expected_tax_code_2"].notna() & df["suggested_tax_code_2"].isna()
    df.loc[s1_age_mask, "suggested_tax_code_1"] = tc_cfg.roth_code
    df.loc[s2_age_mask, "suggested_tax_code_2"] = df["expected_tax_code_2"]
    _append_action(df, age_code_mismatch, cfg.action_update)

    # Collect all triggered reasons so notebooks can see combined context.
    _append_reason(df, roth_year_change_required, "roth_initial_year_mismatch")
    _append_reason(df, raw_missing_first_year & active_mask, "missing_first_roth_tax_year")
    _append_reason(df, basis_mask, "roth_basis_covers_2025_total")
    _append_reason(df, raw_qualified_mask, "qualified_roth_distribution")
    _append_reason(df, taxable_missing_current, "missing_fed_taxable_amt")
    _append_reason(df, raw_proximity_mask, "taxable_within_15pct_of_gross")
    _append_reason(df, age_code_mismatch, "roth_age_tax_code_mismatch")
    age_update_mask = age_code_mismatch & df["expected_tax_code_2"].notna()
    _append_reason(
        df,
        age_update_mask & attained_59_5_in_txn_year,
        "roth_age_rule_attained_59_5_in_txn_year_expect_B7",
    )
    _append_reason(
        df,
        age_update_mask & ~attained_59_5_in_txn_year & has_term_year & attained_55_in_term_year,
        "roth_age_rule_attained_55_in_term_year_expect_B2",
    )
    _append_reason(
        df,
        age_update_mask & ~attained_59_5_in_txn_year & has_term_year & ~attained_55_in_term_year,
        "roth_age_rule_under_55_in_term_year_expect_B1",
    )
    _append_reason(
        df,
        age_update_mask & ~attained_59_5_in_txn_year & ~has_term_year & attained_55_in_txn_year,
        "roth_age_rule_attained_55_in_txn_year_no_term_expect_B2",
    )
    _append_reason(
        df,
        age_update_mask & ~attained_59_5_in_txn_year & ~has_term_year & ~attained_55_in_txn_year,
        "roth_age_rule_under_55_in_txn_year_no_term_expect_B1",
    )

    # Exclusion handling
    df.loc[mask_engine_excluded, "match_status"] = tc_cfg.status_excluded

    # Finalize actions and match_status precedence
    action_joiner = tc_cfg.action_joiner
    df["action"] = df["actions"].apply(lambda acts: action_joiner.join(acts) if acts else pd.NA)

    has_update = df["actions"].apply(lambda a: tc_cfg.action_update in a if a is not None else False)
    has_investigate = df["actions"].apply(lambda a: tc_cfg.action_investigate in a if a is not None else False)

    df.loc[~mask_engine_excluded & has_update, "match_status"] = status_cfg.needs_correction
    df.loc[~mask_engine_excluded & ~has_update & has_investigate, "match_status"] = status_cfg.needs_review
    df.loc[~mask_engine_excluded & ~has_update & ~has_investigate, "match_status"] = status_cfg.no_action

    # Correction reasons with bullet + newline
    reason_joiner = tc_cfg.reason_joiner
    bullet = tc_cfg.reason_bullet
    df["correction_reason"] = df["correction_reasons"].apply(
        lambda reasons: reason_joiner.join(f"{bullet}{r}" for r in reasons) if reasons else pd.NA
    )
    df.loc[df["match_status"] == status_cfg.no_action, "correction_reason"] = pd.NA
    df.loc[
        df["match_status"] == status_cfg.no_action,
        ["suggested_tax_code_1", "suggested_tax_code_2"],
    ] = pd.NA
    df.loc[
        (df["match_status"] == status_cfg.no_action)
        & df["fed_taxable_amt"].eq(0),
        "suggested_taxable_amt",
    ] = pd.NA

    # Compose combined tax code for convenience (B7, H4, etc.)
    s1 = df["suggested_tax_code_1"].astype("string").str.strip().str.upper().replace("", pd.NA)
    s2 = df["suggested_tax_code_2"].astype("string").str.strip().str.upper().replace("", pd.NA)
    df["new_tax_code"] = pd.NA
    df.loc[s1.notna() & s2.isna(), "new_tax_code"] = s1
    df.loc[s1.notna() & s2.notna(), "new_tax_code"] = (s1 + s2)
    df["new_tax_code"] = df["new_tax_code"].astype("string")

    out_cols = [
        "transaction_id",
        "txn_date",
        "ssn",
        "participant_name",
        "matrix_account",
        "plan_id",
        "tax_code_1",
        "tax_code_2",
        "suggested_tax_code_1",
        "suggested_tax_code_2",
        "new_tax_code",
        "fed_taxable_amt",
        "gross_amt",
        "roth_initial_contribution_year",
        "first_roth_tax_year",
        "start_roth_year",
        "roth_basis_amt",
        "age_at_txn",
        "suggested_taxable_amt",
        "suggested_first_roth_tax_year",
        "correction_reason",
        "action",
        "match_status",
    ]

    return df[out_cols].copy()

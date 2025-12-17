# Docstring for src/roth_taxable_analysis module
"""
roth_taxable_analysis.py

Engine C: Roth-taxable analysis for Matrix distributions. Flags Roth
transactions that may need 1099-R taxable adjustments or Roth initial-year
updates, producing a canonical DataFrame compatible with
`build_correction_dataframe()` (match_status/action/suggested_tax_code_* present
even if NA).

Design goals
------------
- Reuse correction builder: emit the fields build_correction_dataframe expects.
- Avoid false positives: only mark UPDATE when taxable differs beyond tolerance.
- Transparency: accumulate all triggered reasons in `correction_reason` for
  notebook review (`; ` joined).

Inputs
------
1) Matrix distributions (cleaned):
   plan_id, ssn, txn_date, transaction_id, participant_name, matrix_account,
   gross_amt, fed_taxable_amt, roth_initial_contribution_year,
   tax_code_1, tax_code_2
2) Relius demographics:
   plan_id, ssn, dob
3) Relius Roth basis:
   plan_id, ssn, first_roth_tax_year, roth_basis_amt

Scope
-----
- Roth plans only (plan_id startswith "300005" OR endswith "R")
- Exclude inherited plans (INHERITED_PLAN_IDS)
- Do NOT exclude rollovers

Core rules (priority)
---------------------
1) Basis coverage:
   - If roth_basis_amt >= gross_2025_total (per plan_id+ssn, txn_year==2025)
     suggest taxable = 0 (no UPDATE if Matrix already 0).
2) Qualified Roth:
   - age_at_txn >= 59.5 AND (txn_year - valid_start_year) >= 5
     suggest taxable = 0 (no UPDATE if Matrix already 0).
3) Roth initial year mismatch:
   - If valid basis year differs from Matrix initial year -> suggest basis year
     and flag correction.
4) Invalid/missing basis year (0/NA):
   - Flag for review (no suggested year).
5) 15% proximity:
   - If fed_taxable_amt > 0 and gross_amt <= fed_taxable_amt * 1.15 and no prior
     correction/review -> flag for review.

Correction vs review handling
-----------------------------
- Uses abs diff > 0.01 to decide if taxable needs UPDATE.
- Missing fed_taxable_amt -> review (INVESTIGATE) rather than UPDATE.
- Multiple reasons may apply; `correction_reason` joins all reason tokens.

Public API
----------
- run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df) -> pd.DataFrame
"""

from __future__ import annotations

import pandas as pd

from .config import INHERITED_PLAN_IDS


ROTH_PLAN_PREFIX = "300005"


def _normalize_plan_id(plan_id: pd.Series) -> pd.Series:
    return plan_id.astype(str).str.strip()


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _compute_age_years(dob: pd.Series, asof: pd.Series) -> pd.Series:
    return (asof - dob).dt.days / 365.25


def _is_roth_plan(series: pd.Series) -> pd.Series:
    filled = series.fillna("")
    return filled.str.startswith(ROTH_PLAN_PREFIX) | filled.str.endswith("R")


def _compute_start_year(df: pd.DataFrame) -> pd.Series:
    return df["first_roth_tax_year"].combine_first(df["roth_initial_contribution_year"])


def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
    """Append a reason token to per-row reason lists for rows where mask is True."""
    idx = mask[mask].index
    for i in idx:
        if reason not in df.at[i, "correction_reasons"]:
            df.at[i, "correction_reasons"].append(reason)


def run_roth_taxable_analysis(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
        relius_roth_basis_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Run Engine C Roth taxable analysis and return a canonical DataFrame with
    correction flags/suggestions.
    
    Note: `correction_reason` joins all triggered reasons with '; ' for quick notebook review.
    """
    
    df = matrix_df.copy()
    df["plan_id"] = _normalize_plan_id(df["plan_id"])

    mask_roth = _is_roth_plan(df["plan_id"])
    mask_not_inherited = ~df["plan_id"].isin(INHERITED_PLAN_IDS)
    df = df[mask_roth & mask_not_inherited].copy()

    demo_cols = ["plan_id", "ssn", "dob"]
    basis_cols = ["plan_id", "ssn", "first_roth_tax_year", "roth_basis_amt"]

    df = df.merge(relius_demo_df[demo_cols], on=["plan_id", "ssn"], how="left")
    df = df.merge(relius_roth_basis_df[basis_cols], on=["plan_id", "ssn"], how="left")

    df["txn_date"] = _to_datetime(df["txn_date"])
    df["dob"] = _to_datetime(df["dob"])
    df["correction_reasons"] = [[] for _ in range(len(df))]  # collect all triggered reasons per row

    df["txn_year"] = df["txn_date"].dt.year
    df["age_at_txn"] = _compute_age_years(df["dob"], df["txn_date"])

    df["gross_amt"] = _to_numeric(df["gross_amt"])
    df["fed_taxable_amt"] = _to_numeric(df.get("fed_taxable_amt"))
    df["roth_basis_amt"] = _to_numeric(df["roth_basis_amt"])

    df["first_roth_tax_year"] = _to_numeric(df["first_roth_tax_year"])
    df["roth_initial_contribution_year"] = _to_numeric(df["roth_initial_contribution_year"])

    first_year_valid = (
        df["first_roth_tax_year"].notna()
        & df["first_roth_tax_year"].gt(0)
        & df["first_roth_tax_year"].between(1900, 2100)
    )

    start_year = df["first_roth_tax_year"].where(first_year_valid, df["roth_initial_contribution_year"])
    start_year_valid = (
        start_year.notna()
        & start_year.gt(0)
        & start_year.between(1900, 2100)
    )
    df["start_roth_year"] = start_year

    mask_2025 = df["txn_year"] == 2025
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

    basis_mask = (
        df["roth_basis_amt"].notna()
        & df["gross_2025_total"].notna()
        & (df["roth_basis_amt"] >= df["gross_2025_total"])
    )
    df.loc[basis_mask, "suggested_taxable_amt"] = 0.0

    raw_qualified_mask = (
        df["age_at_txn"].ge(59.5)
        & start_year_valid
        & (df["txn_year"] - start_year).ge(5)
    )
    qualified_mask = df["suggested_taxable_amt"].isna() & raw_qualified_mask
    df.loc[qualified_mask, "suggested_taxable_amt"] = 0.0

    taxable_suggested = df["suggested_taxable_amt"].notna()
    taxable_missing_current = taxable_suggested & df["fed_taxable_amt"].isna()
    taxable_change_required = taxable_suggested & df["fed_taxable_amt"].notna() & (
        (df["fed_taxable_amt"] - df["suggested_taxable_amt"]).abs() > 0.01
    )
    roth_year_change_required = first_year_valid & (
        df["roth_initial_contribution_year"].isna()
        | df["roth_initial_contribution_year"].ne(df["first_roth_tax_year"])
    )

    df["suggested_first_roth_tax_year"] = pd.NA
    df.loc[roth_year_change_required, "suggested_first_roth_tax_year"] = df.loc[
        roth_year_change_required, "first_roth_tax_year"
    ]

    df["match_status"] = "match_no_action"
    df["action"] = pd.NA

    df.loc[roth_year_change_required, ["match_status", "action"]] = [
        "match_needs_correction",
        "UPDATE_1099",
    ]

    missing_mask = taxable_missing_current & df["match_status"].eq("match_no_action")
    df.loc[missing_mask, ["match_status", "action"]] = [
        "match_needs_review",
        "INVESTIGATE",
    ]

    missing_first_year_mask = raw_missing_first_year & df["match_status"].eq("match_no_action")
    df.loc[missing_first_year_mask, ["match_status", "action"]] = [
        "match_needs_review",
        "INVESTIGATE",
    ]

    change_mask = taxable_change_required & df["match_status"].eq("match_no_action")
    df.loc[change_mask, ["match_status", "action"]] = [
        "match_needs_correction",
        "UPDATE_1099",
    ]

    raw_proximity_mask = df["fed_taxable_amt"].gt(0) & df["gross_amt"].le(df["fed_taxable_amt"] * 1.15)
    proximity_mask = df["match_status"].eq("match_no_action") & raw_proximity_mask
    df.loc[proximity_mask, ["match_status", "action"]] = [
        "match_needs_review",
        "INVESTIGATE",
    ]

    # Collect all triggered reasons so notebooks can see combined context.
    _append_reason(df, roth_year_change_required, "roth_initial_year_mismatch")
    _append_reason(df, raw_missing_first_year, "missing_first_roth_tax_year")
    _append_reason(df, basis_mask, "roth_basis_covers_2025_total")
    _append_reason(df, raw_qualified_mask, "qualified_roth_distribution")
    _append_reason(df, taxable_missing_current, "missing_fed_taxable_amt")
    _append_reason(df, raw_proximity_mask, "taxable_within_15pct_of_gross")

    df["correction_reason"] = df["correction_reasons"].apply(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )

    # Collect all triggered reasons so notebooks can see combined context.
    _append_reason(df, roth_year_change_required, "roth_initial_year_mismatch")
    _append_reason(df, missing_first_year_mask, "missing_first_roth_tax_year")
    _append_reason(df, basis_mask, "roth_basis_covers_2025_total")
    _append_reason(df, qualified_mask, "qualified_roth_distribution")
    _append_reason(df, missing_mask, "missing_fed_taxable_amt")
    _append_reason(df, proximity_mask, "taxable_within_15pct_of_gross")

    df["correction_reason"] = df["correction_reasons"].apply(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )

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
        "fed_taxable_amt",
        "gross_amt",
        "roth_initial_contribution_year",
        "first_roth_tax_year",
        "roth_basis_amt",
        "age_at_txn",
        "suggested_taxable_amt",
        "suggested_first_roth_tax_year",
        "correction_reason",
        "action",
        "match_status",
    ]

    return df[out_cols].copy()

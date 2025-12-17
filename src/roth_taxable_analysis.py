# Docstring for src/roth_taxable_analysis module
"""
roth_taxable_analysis.py

Engine C: Analyze Roth distributions to flag taxable amounts that may need
correction. Produces a canonical DataFrame compatible with
`build_correction_dataframe()` (expects match_status, action, suggested_tax_code_*
even if suggestions are NA for now).

Inputs (canonical):
- matrix_df (cleaned Matrix): plan_id, ssn, txn_date, transaction_id,
  participant_name, matrix_account, gross_amt, fed_taxable_amt,
  roth_initial_contribution_year, tax_code_1, tax_code_2
- relius_demo_df: plan_id, ssn, dob
- relius_roth_basis_df: plan_id, ssn, first_roth_tax_year, roth_basis_amt

Scope:
- Only Roth plans (plan_id startswith "300005" OR endswith "R")
- Exclude inherited plans (INHERITED_PLAN_IDS); do NOT exclude rollovers

Rules (priority order):
1) Basis coverage: if roth_basis_amt >= gross_2025_total (per plan_id+ssn for
   txn_year==2025) -> suggested_taxable_amt = 0, match_status/action set.
2) Qualified Roth: if age_at_txn >= 59.5 AND (txn_year - first_roth_tax_year) >= 5
   -> suggested_taxable_amt = 0, match_status/action set if not already set.
3) 15% proximity: if fed_taxable_amt > 0 and gross_amt <= fed_taxable_amt * 1.15
   and no prior rule applied -> match_status/action set to review.
Also suggests first_roth_tax_year when Matrix value is missing/different.
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


def run_roth_taxable_analysis(
        matrix_df: pd.DataFrame,
        relius_demo_df: pd.DataFrame,
        relius_roth_basis_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Run Engine C Roth taxable analysis and return a canonical DataFrame with
    correction flags/suggestions.
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
    df["correction_reason"] = pd.NA
    df["action"] = pd.NA
    df["match_status"] = pd.NA

    basis_mask = (
        df["roth_basis_amt"].notna()
        & df["gross_2025_total"].notna()
        & (df["roth_basis_amt"] >= df["gross_2025_total"])
    )
    df.loc[basis_mask, "suggested_taxable_amt"] = 0.0
    df.loc[basis_mask, "correction_reason"] = "roth_basis_covers_2025_total"

    qualified_mask = (
        df["suggested_taxable_amt"].isna()
        & df["age_at_txn"].ge(59.5)
        & start_year_valid
        & (df["txn_year"] - start_year).ge(5)
    )
    df.loc[qualified_mask, "suggested_taxable_amt"] = 0.0
    df.loc[qualified_mask, "correction_reason"] = "qualified_roth_distribution"

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

    df.loc[roth_year_change_required, ["match_status", "action", "correction_reason"]] = [
        "match_needs_correction",
        "UPDATE_1099",
        "roth_initial_year_mismatch",
    ]

    missing_mask = taxable_missing_current & df["match_status"].eq("match_no_action")
    df.loc[missing_mask, ["match_status", "action", "correction_reason"]] = [
        "match_needs_review",
        "INVESTIGATE",
        "missing_fed_taxable_amt",
    ]

    missing_first_year_mask = ~first_year_valid & df["match_status"].eq("match_no_action")
    df.loc[missing_first_year_mask, ["match_status", "action", "correction_reason"]] = [
        "match_needs_review",
        "INVESTIGATE",
        "missing_first_roth_tax_year",
    ]

    change_mask = taxable_change_required & df["match_status"].eq("match_no_action")
    df.loc[change_mask, ["match_status", "action"]] = [
        "match_needs_correction",
        "UPDATE_1099",
    ]

    proximity_mask = (
        df["match_status"].eq("match_no_action")
        & df["fed_taxable_amt"].gt(0)
        & df["gross_amt"].le(df["fed_taxable_amt"] * 1.15)
    )
    df.loc[proximity_mask, ["match_status", "action", "correction_reason"]] = [
        "match_needs_review",
        "INVESTIGATE",
        "taxable_within_15pct_of_gross",
    ]

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

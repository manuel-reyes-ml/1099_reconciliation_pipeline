# Docstring for src/ira_rollover_analysis module
"""
ira_rollover_analysis.py

Engine D: IRA rollover tax-form audit for Matrix-only distributions.

This engine filters Matrix distributions to IRA plans with check distributions,
then evaluates the federal taxing method and tax form:
  - Federal Taxing Method = Rollover
    - Tax Form "No Tax"  -> match_no_action
    - Tax Form "1099-R"  -> match_needs_correction (new_tax_code="0")
  - Missing/unknown Federal Taxing Method or Tax Form -> match_needs_review

Plan detection is config-driven:
  - plan_id starts with configured prefixes (default: 300001, 300005), OR
  - plan_id contains configured substrings (default: "IRA")
Normalization helpers are shared in `core.normalizers` to keep behavior
consistent across engines.

Public API
----------
- run_ira_rollover_analysis(matrix_df, cfg=IRA_ROLLOVER_CONFIG, date_filter=None)
    -> pd.DataFrame
"""


from __future__ import annotations

import pandas as pd

from ..config import (
    DateFilterConfig,
    IraRolloverConfig,
    IRA_ROLLOVER_CONFIG,
    MATCH_STATUS_CONFIG,
)
from ..core.normalizers import (
    _append_reason,
    _is_ira_plan,
    _normalize_compact_upper,
    _normalize_space_lower,
    apply_date_filter,
    normalize_tax_code_series,
)


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_list}")


def run_ira_rollover_analysis(
    matrix_df: pd.DataFrame,
    cfg: IraRolloverConfig = IRA_ROLLOVER_CONFIG,
    date_filter: DateFilterConfig | None = None,
) -> pd.DataFrame:
    """
    Run the IRA rollover tax-form audit and return a correction-ready DataFrame.
    """

    required_cols = [
        "plan_id",
        "txn_method",
        "federal_taxing_method",
        "tax_form",
        "transaction_id",
        "txn_date",
        "ssn",
        "matrix_account",
    ]
    _validate_required_columns(matrix_df, required_cols)
    if "participant_name" not in matrix_df.columns and "full_name" not in matrix_df.columns:
        raise ValueError("Expected participant_name or full_name column for correction outputs.")

    status_cfg = MATCH_STATUS_CONFIG

    df = apply_date_filter(matrix_df.copy(), "txn_date", date_filter=date_filter)

    ira_mask = _is_ira_plan(df["plan_id"], cfg)
    txn_method_norm = _normalize_space_lower(df["txn_method"])
    check_distribution_mask = txn_method_norm == "check distribution"

    df = df[ira_mask & check_distribution_mask].copy()
    tax_code_1 = normalize_tax_code_series(
        df.get("tax_code_1", pd.Series(pd.NA, index=df.index))
    ).fillna("")
    tax_code_2 = normalize_tax_code_series(
        df.get("tax_code_2", pd.Series(pd.NA, index=df.index))
    ).fillna("")
    rollover_tax_code_mask = tax_code_1.isin(["G", "H"]) | tax_code_2.isin(["G", "H"])
    df = df[rollover_tax_code_mask].copy()

    df["match_status"] = status_cfg.needs_review
    df["action"] = pd.NA
    df["suggested_tax_code_1"] = pd.NA
    df["suggested_tax_code_2"] = pd.NA
    df["new_tax_code"] = pd.NA
    df["correction_reason"] = pd.NA

    if df.empty:
        return df

    ftm_norm = _normalize_compact_upper(df["federal_taxing_method"])
    tax_form_norm = _normalize_compact_upper(df["tax_form"])

    missing_ftm = ftm_norm.isna() | ftm_norm.eq("")
    missing_tax_form = tax_form_norm.isna() | tax_form_norm.eq("")

    df["correction_reasons"] = [[] for _ in range(len(df))]
    _append_reason(df, missing_ftm, "missing_federal_taxing_method")
    _append_reason(df, missing_tax_form, "missing_tax_form")

    ftm_rollover = ftm_norm == "ROLLOVER"
    ftm_non_rollover = ~missing_ftm & ~ftm_rollover
    _append_reason(df, ftm_non_rollover, "federal_taxing_method_not_rollover")

    tax_form_no_tax = tax_form_norm == "NOTAX"
    tax_form_1099r = tax_form_norm == "1099R"
    tax_form_unknown = ~missing_tax_form & ~tax_form_no_tax & ~tax_form_1099r
    _append_reason(df, tax_form_unknown, "unrecognized_tax_form")

    mask_no_action = ftm_rollover & tax_form_no_tax
    mask_correction = ftm_rollover & tax_form_1099r

    df.loc[mask_no_action, "match_status"] = status_cfg.no_action
    df.loc[mask_correction, "match_status"] = status_cfg.needs_correction
    df.loc[mask_correction, "action"] = "UPDATE_1099"
    df.loc[mask_correction, "suggested_tax_code_1"] = "0"
    df.loc[mask_correction, "new_tax_code"] = "0"
    df.loc[df["match_status"] == status_cfg.needs_review, "action"] = "INVESTIGATE"
    df["correction_reason"] = df["correction_reasons"].apply(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )
    df.loc[mask_correction, "correction_reason"] = (
        "ira_rollover_tax_form_1099r_expected_no_tax"
    )
    df.loc[mask_no_action, "correction_reason"] = pd.NA

    df.drop(columns=["correction_reasons"], inplace=True)

    return df

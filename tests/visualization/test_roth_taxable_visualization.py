import pandas as pd
import pytest

from src.roth_taxable_visualization import (
    build_roth_kpi_summary,
    build_roth_action_mix,
    build_roth_correction_reason_summary,
    build_taxable_delta_distribution,
    build_roth_tax_code_crosstab,
)


def test_build_roth_kpi_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_no_action",
                "match_needs_correction",
                "match_needs_review",
                "excluded_from_age_engine_rollover_or_inherited",
                "match_no_action",
            ]
        }
    )

    summary = build_roth_kpi_summary(df).set_index("status_group")

    assert summary.loc["no_action", "count"] == 2
    assert summary.loc["needs_correction", "count"] == 1
    assert summary.loc["needs_review", "count"] == 1
    assert summary.loc["excluded_rollover_or_inherited", "count"] == 1
    assert summary.loc["no_action", "percent"] == pytest.approx(2 / 5)


def test_build_roth_kpi_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status"])
    summary = build_roth_kpi_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["status_group", "count", "percent"]


def test_build_roth_kpi_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_roth_kpi_summary(pd.DataFrame({"status": ["match_no_action"]}))


def test_build_roth_action_mix_counts() -> None:
    df = pd.DataFrame(
        {
            "action": [
                "UPDATE_1099",
                "INVESTIGATE",
                "UPDATE_1099\nINVESTIGATE",
                pd.NA,
            ]
        }
    )

    summary = build_roth_action_mix(df).set_index("action")

    assert summary.loc["UPDATE_1099", "count"] == 2
    assert summary.loc["INVESTIGATE", "count"] == 2
    assert summary.loc["UPDATE_1099", "percent"] == pytest.approx(2 / 4)


def test_build_roth_action_mix_empty() -> None:
    df = pd.DataFrame(columns=["action"])
    summary = build_roth_action_mix(df)

    assert summary.empty is True
    assert list(summary.columns) == ["action", "count", "percent"]


def test_build_roth_action_mix_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_roth_action_mix(pd.DataFrame({"actions": ["UPDATE_1099"]}))


def test_build_roth_correction_reason_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_needs_correction",
                "match_needs_review",
                "match_no_action",
                "match_needs_review",
            ],
            "correction_reason": [
                "- reason_a\n- reason_b",
                "- reason_a",
                "- reason_c",
                None,
            ],
        }
    )

    summary = build_roth_correction_reason_summary(df).set_index(
        "correction_reason"
    )

    assert summary.loc["reason_a", "count"] == 2
    assert summary.loc["reason_b", "count"] == 1


def test_build_roth_correction_reason_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status", "correction_reason"])
    summary = build_roth_correction_reason_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["correction_reason", "count", "percent"]


def test_build_roth_correction_reason_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_roth_correction_reason_summary(
            pd.DataFrame({"match_status": ["match_needs_correction"]})
        )


def test_build_taxable_delta_distribution_counts() -> None:
    df = pd.DataFrame(
        {
            "fed_taxable_amt": [100, 0, 50],
            "suggested_taxable_amt": [0, 0, None],
        }
    )

    metrics = build_taxable_delta_distribution(df).set_index("taxable_delta")

    assert metrics.loc[-100.0, "count"] == 1
    assert metrics.loc[0.0, "count"] == 1


def test_build_taxable_delta_distribution_empty() -> None:
    df = pd.DataFrame(columns=["fed_taxable_amt", "suggested_taxable_amt"])
    metrics = build_taxable_delta_distribution(df)

    assert metrics.empty is True
    assert list(metrics.columns) == ["taxable_delta", "count"]


def test_build_taxable_delta_distribution_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_taxable_delta_distribution(pd.DataFrame({"fed_taxable_amt": [0]}))


def test_build_roth_tax_code_crosstab_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_needs_correction",
                "match_needs_correction",
                "match_no_action",
            ],
            "tax_code_1": ["B", "H", "B"],
            "tax_code_2": ["", "4", ""],
            "suggested_tax_code_1": ["H", "H", None],
            "suggested_tax_code_2": ["", "4", None],
        }
    )

    crosstab = build_roth_tax_code_crosstab(df)

    assert crosstab.loc["B", "H"] == 1
    assert crosstab.loc["H4", "H4"] == 1


def test_build_roth_tax_code_crosstab_empty() -> None:
    df = pd.DataFrame(
        columns=[
            "match_status",
            "tax_code_1",
            "tax_code_2",
            "suggested_tax_code_1",
            "suggested_tax_code_2",
        ]
    )
    crosstab = build_roth_tax_code_crosstab(df)

    assert crosstab.empty is True
    assert crosstab.index.name == "current_tax_code"
    assert crosstab.columns.name == "suggested_tax_code"


def test_build_roth_tax_code_crosstab_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_roth_tax_code_crosstab(pd.DataFrame({"match_status": []}))

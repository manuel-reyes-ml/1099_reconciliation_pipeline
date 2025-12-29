import pandas as pd
import pytest

from src.age_taxcode_visualization import (
    build_age_taxcode_kpi_summary,
    build_term_date_correction_metrics,
    build_correction_reason_crosstab,
)


def test_build_age_taxcode_kpi_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "perfect_match",
                "match_needs_correction",
                "age_rule_insufficient_data",
                "excluded_from_age_engine_rollover_or_inherited",
                "perfect_match",
            ]
        }
    )

    summary = build_age_taxcode_kpi_summary(df).set_index("status_group")

    assert summary.loc["perfect_match", "count"] == 2
    assert summary.loc["needs_correction", "count"] == 1
    assert summary.loc["insufficient_data", "count"] == 1
    assert summary.loc["excluded_rollover_or_inherited", "count"] == 1
    assert summary.loc["perfect_match", "percent"] == pytest.approx(2 / 5)


def test_build_age_taxcode_kpi_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status"])
    summary = build_age_taxcode_kpi_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["status_group", "count", "percent"]


def test_build_age_taxcode_kpi_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_age_taxcode_kpi_summary(pd.DataFrame({"status": ["perfect_match"]}))


def test_build_term_date_correction_metrics_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_needs_correction",
                "perfect_match",
                "match_needs_correction",
                "perfect_match",
            ],
            "term_date": ["2023-01-01", None, "", "2024-06-01"],
        }
    )

    metrics = build_term_date_correction_metrics(df).set_index("term_date_group")

    assert metrics.loc["with_term_date", "total_txns"] == 2
    assert metrics.loc["with_term_date", "correction_count"] == 1
    assert metrics.loc["with_term_date", "correction_rate"] == pytest.approx(0.5)
    assert metrics.loc["without_term_date", "total_txns"] == 2
    assert metrics.loc["without_term_date", "correction_count"] == 1
    assert metrics.loc["without_term_date", "correction_rate"] == pytest.approx(0.5)


def test_build_term_date_correction_metrics_empty() -> None:
    df = pd.DataFrame(columns=["match_status", "term_date"])
    metrics = build_term_date_correction_metrics(df)

    assert metrics.empty is True
    assert list(metrics.columns) == [
        "term_date_group",
        "total_txns",
        "correction_count",
        "correction_rate",
    ]


def test_build_term_date_correction_metrics_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_term_date_correction_metrics(
            pd.DataFrame({"match_status": ["perfect_match"]})
        )


def test_build_correction_reason_crosstab_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_needs_correction",
                "match_needs_correction",
                "perfect_match",
                "match_needs_correction",
            ],
            "tax_code_1": ["1", "1", "2", None],
            "correction_reason": [
                "terminated_before_55",
                "terminated_before_55",
                "no_term_date_55_plus_in_txn_year",
                None,
            ],
        }
    )

    crosstab = build_correction_reason_crosstab(df)

    assert crosstab.loc["1", "terminated_before_55"] == 2
    assert crosstab.loc["Unknown", "Unknown"] == 1


def test_build_correction_reason_crosstab_empty() -> None:
    df = pd.DataFrame(columns=["match_status", "tax_code_1", "correction_reason"])
    crosstab = build_correction_reason_crosstab(df)

    assert crosstab.empty is True
    assert crosstab.index.name == "tax_code_1"
    assert crosstab.columns.name == "correction_reason"


def test_build_correction_reason_crosstab_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_correction_reason_crosstab(
            pd.DataFrame({"match_status": ["match_needs_correction"]})
        )

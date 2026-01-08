import pandas as pd
import pytest

from src.visualization.match_planid_visualization import (
    build_match_kpi_summary,
    build_unmatched_summary,
    build_date_lag_distribution,
    build_correction_reason_summary,
)


def test_build_match_kpi_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_no_action",
                "match_needs_correction",
                "match_needs_review",
                "date_out_of_range",
                "unmatched_relius",
                "unmatched_matrix",
                "match_no_action",
            ]
        }
    )

    summary = build_match_kpi_summary(df).set_index("status_group")

    assert summary.loc["no_action", "count"] == 2
    assert summary.loc["needs_correction", "count"] == 1
    assert summary.loc["needs_review", "count"] == 1
    assert summary.loc["date_out_of_range", "count"] == 1
    assert summary.loc["unmatched_relius", "count"] == 1
    assert summary.loc["unmatched_matrix", "count"] == 1
    assert summary.loc["no_action", "percent"] == pytest.approx(2 / 7)


def test_build_match_kpi_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status"])
    summary = build_match_kpi_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["status_group", "count", "percent"]


def test_build_match_kpi_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_match_kpi_summary(pd.DataFrame({"status": ["match_no_action"]}))


def test_build_unmatched_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "unmatched_relius",
                "unmatched_matrix",
                "unmatched_relius",
                "match_no_action",
            ]
        }
    )

    summary = build_unmatched_summary(df).set_index("unmatched_group")

    assert summary.loc["unmatched_relius", "count"] == 2
    assert summary.loc["unmatched_matrix", "count"] == 1
    assert summary.loc["unmatched_relius", "percent"] == pytest.approx(2 / 4)


def test_build_unmatched_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status"])
    summary = build_unmatched_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["unmatched_group", "count", "percent"]


def test_build_unmatched_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_unmatched_summary(pd.DataFrame({"status": ["unmatched_relius"]}))


def test_build_date_lag_distribution_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_no_action",
                "match_needs_correction",
                "date_out_of_range",
            ],
            "exported_date": ["2024-01-01", "2024-01-01", "2024-01-05"],
            "txn_date": ["2024-01-03", "2024-01-01", "2024-01-09"],
        }
    )

    metrics = build_date_lag_distribution(df).set_index("date_lag_days")

    assert metrics.loc[0, "count"] == 1
    assert metrics.loc[2, "count"] == 1
    assert metrics.loc[4, "count"] == 1


def test_build_date_lag_distribution_empty() -> None:
    df = pd.DataFrame(columns=["match_status", "exported_date", "txn_date"])
    metrics = build_date_lag_distribution(df)

    assert metrics.empty is True
    assert list(metrics.columns) == ["date_lag_days", "count"]


def test_build_date_lag_distribution_invalid_dates_raise() -> None:
    df = pd.DataFrame(
        {
            "match_status": ["match_no_action"],
            "exported_date": [None],
            "txn_date": ["2024-01-01"],
        }
    )

    with pytest.raises(ValueError, match="missing or malformed exported_date/txn_date"):
        build_date_lag_distribution(df)


def test_build_date_lag_distribution_all_unmatched_allows_missing() -> None:
    df = pd.DataFrame(
        {
            "match_status": ["unmatched_relius"],
            "exported_date": [None],
            "txn_date": [None],
        }
    )

    metrics = build_date_lag_distribution(df)

    assert metrics.empty is True
    assert list(metrics.columns) == ["date_lag_days", "count"]


def test_build_date_lag_distribution_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_date_lag_distribution(
            pd.DataFrame({"match_status": ["match_no_action"]})
        )


def test_build_correction_reason_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_needs_correction",
                "match_needs_correction",
                "match_no_action",
                "match_needs_correction",
            ],
            "correction_reason": ["reason_a", None, "reason_b", "reason_a"],
        }
    )

    summary = build_correction_reason_summary(df).set_index("correction_reason")

    assert summary.loc["reason_a", "count"] == 2
    assert summary.loc["Unknown", "count"] == 1


def test_build_correction_reason_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status", "correction_reason"])
    summary = build_correction_reason_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["correction_reason", "count", "percent"]


def test_build_correction_reason_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_correction_reason_summary(
            pd.DataFrame({"match_status": ["match_needs_correction"]})
        )

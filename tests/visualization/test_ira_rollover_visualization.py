import pandas as pd
import pytest

from src.visualization.ira_rollover_visualization import (
    build_ira_rollover_kpi_summary,
    build_ira_rollover_metrics,
)


def test_build_ira_rollover_kpi_summary_counts() -> None:
    df = pd.DataFrame(
        {
            "match_status": [
                "match_no_action",
                "match_needs_correction",
                "match_needs_review",
                "match_no_action",
            ]
        }
    )

    summary = build_ira_rollover_kpi_summary(df).set_index("status_group")

    assert summary.loc["no_action", "count"] == 2
    assert summary.loc["needs_correction", "count"] == 1
    assert summary.loc["needs_review", "count"] == 1
    assert summary.loc["no_action", "percent"] == pytest.approx(2 / 4)


def test_build_ira_rollover_kpi_summary_empty() -> None:
    df = pd.DataFrame(columns=["match_status"])
    summary = build_ira_rollover_kpi_summary(df)

    assert summary.empty is True
    assert list(summary.columns) == ["status_group", "count", "percent"]


def test_build_ira_rollover_kpi_summary_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_ira_rollover_kpi_summary(pd.DataFrame({"status": ["match_no_action"]}))


def test_build_ira_rollover_metrics_counts() -> None:
    df = pd.DataFrame(
        {
            "txn_date": ["2025-01-15", "2025-01-20", "2025-02-05"],
            "match_status": [
                "match_needs_correction",
                "match_no_action",
                "match_needs_correction",
            ],
        }
    )

    metrics = build_ira_rollover_metrics(df).set_index("txn_month")

    jan = pd.Timestamp("2025-01-01")
    feb = pd.Timestamp("2025-02-01")
    assert metrics.loc[jan, "total_txns"] == 2
    assert metrics.loc[jan, "correction_count"] == 1
    assert metrics.loc[jan, "correction_rate"] == pytest.approx(0.5)
    assert metrics.loc[feb, "total_txns"] == 1
    assert metrics.loc[feb, "correction_count"] == 1
    assert metrics.loc[feb, "correction_rate"] == pytest.approx(1.0)


def test_build_ira_rollover_metrics_empty() -> None:
    df = pd.DataFrame(columns=["txn_date", "match_status"])
    metrics = build_ira_rollover_metrics(df)

    assert metrics.empty is True
    assert list(metrics.columns) == [
        "txn_month",
        "total_txns",
        "correction_count",
        "correction_rate",
    ]


def test_build_ira_rollover_metrics_missing_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        build_ira_rollover_metrics(pd.DataFrame({"txn_date": ["2025-01-01"]}))

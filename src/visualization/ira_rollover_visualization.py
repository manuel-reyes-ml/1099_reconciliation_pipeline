"""
ira_rollover_visualization.py

Helpers for summarizing and visualizing Engine D (ira_rollover_analysis) output
filtered to G/H tax-code rows.
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt

from ..config import MATCH_STATUS_CONFIG


STATUS_CFG = MATCH_STATUS_CONFIG
IRA_STATUS_GROUPS = [
    ("no_action", STATUS_CFG.no_action),
    ("needs_correction", STATUS_CFG.needs_correction),
    ("needs_review", STATUS_CFG.needs_review),
]


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_list}")


def build_ira_rollover_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute counts and percentages for key match_status categories.

    Required columns:
      - match_status

    Assumes rows are already filtered to G/H tax-code entries.
    """

    _validate_required_columns(df, ["match_status"])

    columns = ["status_group", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    total = int(df.shape[0])
    rows = []
    for group_label, status_value in IRA_STATUS_GROUPS:
        count = int((df["match_status"] == status_value).sum())
        percent = count / total if total else 0.0
        rows.append(
            {
                "status_group": group_label,
                "count": count,
                "percent": percent,
            }
        )

    return pd.DataFrame(rows, columns=columns)


def plot_ira_rollover_kpi_summary(
    summary_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot a KPI summary of match_status categories as percent of records.
    """

    _validate_required_columns(summary_df, ["status_group", "count", "percent"])

    fig, ax = plt.subplots(figsize=(8, 4))
    if summary_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    order = [group_label for group_label, _ in IRA_STATUS_GROUPS]
    data = summary_df.set_index("status_group").reindex(order).fillna(0)
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.barh(order, percents, color="#72B7B2")
    ax.set_xlabel("Percent of Records")
    ax.set_title("Engine D Match Status Summary (G/H tax codes)")

    max_pct = float(percents.max() if len(percents) else 0)
    ax.set_xlim(0, max(10.0, max_pct * 1.15))

    for idx, (pct, count) in enumerate(zip(percents, counts)):
        ax.text(
            pct + 0.5,
            idx,
            f"{pct:.1f}% ({count})",
            va="center",
        )

    return fig, ax


def build_ira_rollover_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly totals, correction counts, and correction rate.

    Required columns:
      - txn_date
      - match_status

    Assumes rows are already filtered to G/H tax-code entries.
    """

    _validate_required_columns(df, ["txn_date", "match_status"])

    columns = ["txn_month", "total_txns", "correction_count", "correction_rate"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    txn_dt = pd.to_datetime(df["txn_date"], errors="coerce")
    invalid_txn_dates = int(txn_dt.isna().sum())
    if invalid_txn_dates:
        raise ValueError(
            f"Found {invalid_txn_dates} rows with missing or malformed txn_date."
        )

    working = df.copy()
    working["txn_month"] = txn_dt.dt.to_period("M").dt.to_timestamp()
    working["is_correction"] = working["match_status"] == STATUS_CFG.needs_correction

    metrics = (
        working.groupby("txn_month", dropna=False)
        .agg(
            total_txns=("match_status", "size"),
            correction_count=("is_correction", "sum"),
        )
        .sort_index()
        .reset_index()
    )
    metrics["correction_rate"] = (
        metrics["correction_count"] / metrics["total_txns"]
    )

    return metrics[columns]


def plot_ira_rollover_correction_counts(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot monthly correction counts alongside total transactions.
    """

    _validate_required_columns(metrics_df, ["txn_month", "total_txns", "correction_count"])

    fig, ax = plt.subplots(figsize=(9, 4))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = metrics_df.sort_values("txn_month")
    months = pd.to_datetime(data["txn_month"])
    total = data["total_txns"].astype(int)
    corrections = data["correction_count"].astype(int)

    ax.plot(months, total, marker="o", linewidth=2, color="#4C78A8", label="Total")
    ax.plot(
        months,
        corrections,
        marker="o",
        linewidth=2,
        color="#F58518",
        label="Corrections",
    )
    ax.set_xlabel("Transaction Month")
    ax.set_ylabel("Count")
    ax.set_title("Engine D Monthly Corrections vs Total (G/H tax codes)")
    ax.legend()
    fig.autofmt_xdate()

    return fig, ax


def plot_ira_rollover_correction_rate(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot monthly correction rate.
    """

    _validate_required_columns(metrics_df, ["txn_month", "correction_rate"])

    fig, ax = plt.subplots(figsize=(9, 4))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = metrics_df.sort_values("txn_month")
    months = pd.to_datetime(data["txn_month"])
    rate = (data["correction_rate"] * 100).astype(float)

    ax.plot(months, rate, marker="o", linewidth=2, color="#54A24B")
    ax.set_xlabel("Transaction Month")
    ax.set_ylabel("Correction Rate (%)")
    ax.set_title("Engine D Correction Rate Over Time (G/H tax codes)")

    max_rate = float(rate.max() if len(rate) else 0)
    ax.set_ylim(0, max(5.0, max_rate * 1.2))
    fig.autofmt_xdate()

    return fig, ax

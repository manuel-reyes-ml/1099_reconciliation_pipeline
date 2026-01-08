"""
match_visualization.py

Helpers for summarizing and visualizing Engine A (match_planid) output.
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt

from ..core.config import MATCHING_CONFIG, MATCH_STATUS_CONFIG


STATUS_CFG = MATCH_STATUS_CONFIG
MATCH_STATUS_GROUPS = [
    ("no_action", STATUS_CFG.no_action),
    ("needs_correction", STATUS_CFG.needs_correction),
    ("needs_review", STATUS_CFG.needs_review),
    ("date_out_of_range", STATUS_CFG.date_out_of_range),
    ("unmatched_relius", STATUS_CFG.unmatched_relius),
    ("unmatched_matrix", STATUS_CFG.unmatched_matrix),
]
UNMATCHED_GROUPS = [
    ("unmatched_relius", STATUS_CFG.unmatched_relius),
    ("unmatched_matrix", STATUS_CFG.unmatched_matrix),
]


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_list}")


def build_match_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute counts and percentages for key match_status categories.

    Required columns:
      - match_status
    """

    _validate_required_columns(df, ["match_status"])

    columns = ["status_group", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    total = int(df.shape[0])
    rows = []
    for group_label, status_value in MATCH_STATUS_GROUPS:
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


def plot_match_kpi_summary(
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

    order = [group_label for group_label, _ in MATCH_STATUS_GROUPS]
    data = summary_df.set_index("status_group").reindex(order).fillna(0)
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.barh(order, percents, color="#72B7B2")
    ax.set_xlabel("Percent of Records")
    ax.set_title("Engine A Match Status Summary")

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


def build_unmatched_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize unmatched Relius vs Matrix counts.

    Required columns:
      - match_status
    """

    _validate_required_columns(df, ["match_status"])

    columns = ["unmatched_group", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    total = int(df.shape[0])
    rows = []
    for group_label, status_value in UNMATCHED_GROUPS:
        count = int((df["match_status"] == status_value).sum())
        percent = count / total if total else 0.0
        rows.append(
            {
                "unmatched_group": group_label,
                "count": count,
                "percent": percent,
            }
        )

    return pd.DataFrame(rows, columns=columns)


def plot_unmatched_summary(
    summary_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot unmatched Relius vs Matrix counts.
    """

    _validate_required_columns(summary_df, ["unmatched_group", "count", "percent"])

    fig, ax = plt.subplots(figsize=(6, 4))
    if summary_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    order = [group_label for group_label, _ in UNMATCHED_GROUPS]
    data = summary_df.set_index("unmatched_group").reindex(order).fillna(0)
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.bar(order, counts, color="#F58518")
    ax.set_ylabel("Count")
    ax.set_title("Engine A Unmatched Counts")

    max_count = float(counts.max() if len(counts) else 0)
    ax.set_ylim(0, max(1.0, max_count * 1.2))

    for idx, (count, pct) in enumerate(zip(counts, percents)):
        ax.text(
            idx,
            count + max(0.5, max_count * 0.03),
            f"{count} ({pct:.1f}%)",
            ha="center",
            va="bottom",
        )

    return fig, ax


def build_date_lag_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a date-lag distribution for matched transactions.

    Required columns:
      - match_status
      - exported_date
      - txn_date
    """

    _validate_required_columns(df, ["match_status", "exported_date", "txn_date"])

    columns = ["date_lag_days", "count"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    exported_dt = pd.to_datetime(df["exported_date"], errors="coerce")
    txn_dt = pd.to_datetime(df["txn_date"], errors="coerce")

    unmatched_statuses = {STATUS_CFG.unmatched_relius, STATUS_CFG.unmatched_matrix}
    expected_mask = ~df["match_status"].isin(unmatched_statuses)

    invalid_mask = expected_mask & (exported_dt.isna() | txn_dt.isna())
    invalid_count = int(invalid_mask.sum())
    if invalid_count:
        raise ValueError(
            f"Found {invalid_count} rows with missing or malformed exported_date/txn_date."
        )

    lag_series = (txn_dt - exported_dt).dt.days
    lag_series = lag_series[expected_mask & lag_series.notna()]

    if lag_series.empty:
        return pd.DataFrame(columns=columns)

    counts = lag_series.astype(int).value_counts().sort_index()
    return pd.DataFrame(
        {
            "date_lag_days": counts.index.astype(int),
            "count": counts.values.astype(int),
        },
        columns=columns,
    )


def plot_date_lag_distribution(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot date-lag distribution with the configured tolerance line.
    """

    _validate_required_columns(metrics_df, ["date_lag_days", "count"])

    fig, ax = plt.subplots(figsize=(8, 4))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = metrics_df.sort_values("date_lag_days")
    ax.bar(
        data["date_lag_days"].astype(int),
        data["count"].astype(int),
        color="#4C78A8",
        alpha=0.9,
    )
    ax.axvline(
        MATCHING_CONFIG.max_date_lag_days,
        color="#E45756",
        linestyle="--",
        linewidth=2,
        label="Max tolerance",
    )
    ax.set_xlabel("Date Lag (Days)")
    ax.set_ylabel("Count")
    ax.set_title("Engine A Date Lag Distribution")
    ax.legend()

    return fig, ax


def build_correction_reason_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize correction reasons for match_needs_correction rows.

    Required columns:
      - match_status
      - correction_reason
    """

    _validate_required_columns(df, ["match_status", "correction_reason"])

    columns = ["correction_reason", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    corrections = df[df["match_status"] == STATUS_CFG.needs_correction].copy()
    if corrections.empty:
        return pd.DataFrame(columns=columns)

    counts = (
        corrections["correction_reason"]
        .fillna("Unknown")
        .astype("string")
        .value_counts()
        .sort_values(ascending=False)
    )
    total = int(counts.sum())

    summary = pd.DataFrame(
        {
            "correction_reason": counts.index.astype(str),
            "count": counts.values.astype(int),
        }
    )
    summary["percent"] = summary["count"] / total if total else 0.0

    return summary[columns]


def plot_correction_reason_summary(
    summary_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot correction reasons for match_needs_correction rows.
    """

    _validate_required_columns(summary_df, ["correction_reason", "count", "percent"])

    fig, ax = plt.subplots(figsize=(8, 4))
    if summary_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = summary_df.sort_values("count", ascending=True)
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.barh(data["correction_reason"], counts, color="#54A24B")
    ax.set_xlabel("Count")
    ax.set_title("Engine A Correction Reasons")

    max_count = float(counts.max() if len(counts) else 0)
    ax.set_xlim(0, max(1.0, max_count * 1.2))

    for idx, (count, pct) in enumerate(zip(counts, percents)):
        ax.text(
            count + max(0.5, max_count * 0.03),
            idx,
            f"{count} ({pct:.1f}%)",
            va="center",
        )

    return fig, ax


def build_correction_reason_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build month-over-month counts by correction_reason.

    Required columns:
      - match_status
      - correction_reason
      - txn_date
    """

    _validate_required_columns(df, ["match_status", "correction_reason", "txn_date"])

    columns = ["txn_month", "correction_reason", "count"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    corrections = df[df["match_status"] == STATUS_CFG.needs_correction].copy()
    if corrections.empty:
        return pd.DataFrame(columns=columns)

    txn_dt = pd.to_datetime(corrections["txn_date"], errors="coerce")
    invalid_count = int(txn_dt.isna().sum())
    if invalid_count:
        raise ValueError(
            f"Found {invalid_count} rows with missing or malformed txn_date."
        )

    corrections["txn_month"] = txn_dt.dt.to_period("M").dt.to_timestamp()
    corrections["correction_reason"] = (
        corrections["correction_reason"]
        .fillna("Unknown")
        .astype("string")
    )

    metrics = (
        corrections.groupby(["txn_month", "correction_reason"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["txn_month", "count"], ascending=[True, False])
    )

    return metrics[columns]


def plot_correction_reason_trends(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot month-over-month correction_reason trends.
    """

    _validate_required_columns(
        metrics_df, ["txn_month", "correction_reason", "count"]
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = (
        metrics_df.pivot_table(
            index="txn_month",
            columns="correction_reason",
            values="count",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )

    reasons = list(data.columns)
    color_map = plt.get_cmap("tab20", max(len(reasons), 1))
    for idx, reason in enumerate(reasons):
        ax.plot(
            pd.to_datetime(data.index),
            data[reason].astype(int),
            marker="o",
            linewidth=2,
            color=color_map(idx),
            label=str(reason),
        )

    ax.set_xlabel("Transaction Month")
    ax.set_ylabel("Count")
    ax.set_title("Engine A Correction Reasons Over Time")
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        borderaxespad=0,
    )
    fig.autofmt_xdate()
    fig.subplots_adjust(right=0.75)

    return fig, ax

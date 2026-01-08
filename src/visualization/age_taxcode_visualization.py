"""
age_taxcode_visualization.py

Helpers for summarizing and visualizing Engine B (age_taxcode_analysis) output.
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt


from ..config import MATCH_STATUS_CONFIG


STATUS_CFG = MATCH_STATUS_CONFIG
CORRECTION_STATUS = STATUS_CFG.needs_correction
MATCH_STATUS_GROUPS = [
    ("excluded_rollover_or_inherited", STATUS_CFG.excluded_age_engine),
    ("insufficient_data", STATUS_CFG.insufficient_data),
    ("no_action", STATUS_CFG.no_action),
    ("needs_correction", CORRECTION_STATUS),
]


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_list}")


def build_age_taxcode_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly totals, correction counts, and correction rate.

    Required columns:
      - txn_date
      - match_status
    """

    _validate_required_columns(df, ["txn_date", "match_status"])

    if df.empty:
        return pd.DataFrame(
            columns=["txn_month", "total_txns", "correction_count", "correction_rate"]
        )

    txn_dt = pd.to_datetime(df["txn_date"], errors="coerce")
    invalid_txn_dates = int(txn_dt.isna().sum())
    if invalid_txn_dates:
        raise ValueError(
            f"Found {invalid_txn_dates} rows with missing or malformed txn_date."
        )

    working = df.copy()
    working["txn_month"] = txn_dt.dt.to_period("M").dt.to_timestamp()
    working["is_correction"] = working["match_status"] == CORRECTION_STATUS

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

    return metrics


def build_age_taxcode_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
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


def plot_age_taxcode_kpi_summary(
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
    ax.set_title("Engine B Match Status Summary")

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


def build_term_date_correction_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute correction rates grouped by term_date availability.

    Required columns:
      - match_status
      - term_date
    """

    _validate_required_columns(df, ["match_status", "term_date"])

    columns = ["term_date_group", "total_txns", "correction_count", "correction_rate"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    term_dt = pd.to_datetime(df["term_date"], errors="coerce")
    working = df.copy()
    working["term_date_group"] = term_dt.notna().map(
        {True: "with_term_date", False: "without_term_date"}
    )
    working["is_correction"] = working["match_status"] == CORRECTION_STATUS

    metrics = (
        working.groupby("term_date_group", dropna=False)
        .agg(
            total_txns=("match_status", "size"),
            correction_count=("is_correction", "sum"),
        )
        .reset_index()
    )
    metrics["correction_rate"] = (
        metrics["correction_count"] / metrics["total_txns"]
    )

    return metrics[columns]


def plot_term_date_correction_rates(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot correction rate comparison for records with vs without term_date.
    """

    _validate_required_columns(
        metrics_df,
        ["term_date_group", "total_txns", "correction_count", "correction_rate"],
    )

    fig, ax = plt.subplots(figsize=(6, 4))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    order = ["with_term_date", "without_term_date"]
    data = metrics_df.set_index("term_date_group").reindex(order).fillna(0)
    rates = (data["correction_rate"] * 100).astype(float)
    counts = data["correction_count"].astype(int)
    totals = data["total_txns"].astype(int)

    ax.bar(order, rates, color="#4C78A8")
    ax.set_ylabel("Correction Rate (%)")
    ax.set_title("Engine B Correction Rate by Term Date Presence")
    ax.set_ylim(0, max(5.0, float(rates.max() if len(rates) else 0) * 1.2))

    for idx, (rate, count, total) in enumerate(zip(rates, counts, totals)):
        ax.text(
            idx,
            rate + 0.3,
            f"{rate:.1f}% ({count}/{total})",
            ha="center",
            va="bottom",
        )

    return fig, ax


def build_correction_reason_crosstab(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a correction-only crosstab of tax_code_1 vs correction_reason.

    Required columns:
      - match_status
      - tax_code_1
      - correction_reason
    """

    _validate_required_columns(df, ["match_status", "tax_code_1", "correction_reason"])

    if df.empty:
        empty = pd.DataFrame()
        empty.index.name = "tax_code_1"
        empty.columns.name = "correction_reason"
        return empty

    corrections = df[df["match_status"] == CORRECTION_STATUS].copy()
    if corrections.empty:
        empty = pd.DataFrame()
        empty.index.name = "tax_code_1"
        empty.columns.name = "correction_reason"
        return empty

    crosstab = pd.crosstab(
        corrections["tax_code_1"].fillna("Unknown").astype("string"),
        corrections["correction_reason"].fillna("Unknown").astype("string"),
        dropna=False,
    )
    crosstab.index.name = "tax_code_1"
    crosstab.columns.name = "correction_reason"
    return crosstab


def plot_correction_reason_crosstab(
    crosstab_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot a correction-only tax_code_1 vs correction_reason cross-breakdown.
    """

    fig, ax = plt.subplots(figsize=(10, 6))

    if crosstab_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = crosstab_df.copy()
    ax.imshow(data.values, cmap="Blues")

    ax.set_xticks(range(len(data.columns)))
    ax.set_yticks(range(len(data.index)))
    ax.set_xticklabels(data.columns, rotation=45, ha="right")
    ax.set_yticklabels(data.index)
    ax.set_title("Corrections: Tax Code 1 x Correction Reason")

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            ax.text(
                j,
                i,
                str(int(data.iloc[i, j])),
                ha="center",
                va="center",
                color="black",
            )

    fig.tight_layout()
    return fig, ax


def plot_corrections_over_time(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, Tuple[plt.Axes, plt.Axes]]:
    """
    Plot monthly total transactions and correction rate trend.
    """

    _validate_required_columns(
        metrics_df, ["txn_month", "total_txns", "correction_count", "correction_rate"]
    )

    fig, ax_left = plt.subplots(figsize=(10, 5))
    ax_right = ax_left.twinx()

    if metrics_df.empty:
        ax_left.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax_left.set_axis_off()
        return fig, (ax_left, ax_right)

    months = pd.to_datetime(metrics_df["txn_month"])
    ax_left.bar(months, metrics_df["total_txns"], color="#4C78A8", alpha=0.8)
    ax_left.set_ylabel("Total Transactions")
    ax_left.set_xlabel("Transaction Month")
    ax_left.set_title("Engine B: Corrections vs Total Transactions")

    ax_right.plot(
        months,
        metrics_df["correction_rate"] * 100,
        color="#F58518",
        marker="o",
        linewidth=2,
    )
    ax_right.set_ylabel("Correction Rate (%)")

    fig.autofmt_xdate()
    return fig, (ax_left, ax_right)


def plot_mistake_breakdown(
    df: pd.DataFrame,
) -> Tuple[plt.Figure, Tuple[plt.Axes, plt.Axes]]:
    """
    Plot mistake breakdowns by tax_code_1 and correction_reason.
    """

    _validate_required_columns(df, ["match_status"])

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    ax_tax, ax_reason = axes

    if df.empty:
        for ax in axes:
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            ax.set_axis_off()
        return fig, (ax_tax, ax_reason)

    corrections = df[df["match_status"] == CORRECTION_STATUS].copy()
    if corrections.empty:
        for ax in axes:
            ax.text(0.5, 0.5, "No corrections to display", ha="center", va="center")
            ax.set_axis_off()
        return fig, (ax_tax, ax_reason)

    if "tax_code_1" in corrections.columns:
        tax_counts = (
            corrections["tax_code_1"]
            .fillna("Unknown")
            .astype("string")
            .value_counts()
            .sort_values(ascending=False)
        )
        ax_tax.bar(tax_counts.index.astype(str), tax_counts.values, color="#54A24B")
        ax_tax.set_title("Corrections by Tax Code 1")
        ax_tax.set_ylabel("Count")
        ax_tax.set_xlabel("Tax Code 1")
    else:
        ax_tax.text(
            0.5,
            0.5,
            "Missing tax_code_1 column",
            ha="center",
            va="center",
        )
        ax_tax.set_axis_off()

    if "correction_reason" in corrections.columns:
        reason_counts = (
            corrections["correction_reason"]
            .fillna("Unknown")
            .astype("string")
            .value_counts()
            .sort_values(ascending=False)
        )
        ax_reason.bar(
            reason_counts.index.astype(str),
            reason_counts.values,
            color="#E45756",
        )
        ax_reason.set_title("Corrections by Reason")
        ax_reason.set_ylabel("Count")
        ax_reason.set_xlabel("Correction Reason")
        ax_reason.tick_params(axis="x", rotation=45)
    else:
        ax_reason.text(
            0.5,
            0.5,
            "Missing correction_reason column",
            ha="center",
            va="center",
        )
        ax_reason.set_axis_off()

    fig.tight_layout()
    return fig, (ax_tax, ax_reason)

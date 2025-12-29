"""
age_taxcode_visualization.py

Helpers for summarizing and visualizing Engine B (age_taxcode_analysis) output.
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt


CORRECTION_STATUS = "match_needs_correction"


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

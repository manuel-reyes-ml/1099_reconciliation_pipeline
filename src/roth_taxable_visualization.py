"""
roth_taxable_visualization.py

Helpers for summarizing and visualizing Engine C (roth_taxable_analysis) output.
"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt

from .config import MATCH_STATUS_CONFIG, ROTH_TAXCODE_CONFIG


STATUS_CFG = MATCH_STATUS_CONFIG
ROTH_CFG = ROTH_TAXCODE_CONFIG
ROTH_STATUS_GROUPS = [
    ("no_action", STATUS_CFG.no_action),
    ("needs_correction", STATUS_CFG.needs_correction),
    ("needs_review", STATUS_CFG.needs_review),
    ("excluded_rollover_or_inherited", STATUS_CFG.excluded_age_engine),
]


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_list}")


def build_roth_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
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
    for group_label, status_value in ROTH_STATUS_GROUPS:
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


def plot_roth_kpi_summary(
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

    order = [group_label for group_label, _ in ROTH_STATUS_GROUPS]
    data = summary_df.set_index("status_group").reindex(order).fillna(0)
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.barh(order, percents, color="#72B7B2")
    ax.set_xlabel("Percent of Records")
    ax.set_title("Engine C Match Status Summary")

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


def build_roth_action_mix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize action mix for UPDATE_1099 vs INVESTIGATE.

    Required columns:
      - action
    """

    _validate_required_columns(df, ["action"])

    columns = ["action", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    total = int(df.shape[0])
    action_joiner = ROTH_CFG.action_joiner
    update_action = ROTH_CFG.action_update
    investigate_action = ROTH_CFG.action_investigate

    def _split_actions(value: object) -> set[str]:
        if pd.isna(value):
            return set()
        text = str(value)
        if action_joiner:
            parts = text.split(action_joiner)
        else:
            parts = text.splitlines()
        return {part.strip() for part in parts if part.strip()}

    actions = df["action"].apply(_split_actions)
    update_count = int(actions.apply(lambda acts: update_action in acts).sum())
    investigate_count = int(
        actions.apply(lambda acts: investigate_action in acts).sum()
    )

    rows = [
        {
            "action": update_action,
            "count": update_count,
            "percent": update_count / total if total else 0.0,
        },
        {
            "action": investigate_action,
            "count": investigate_count,
            "percent": investigate_count / total if total else 0.0,
        },
    ]

    return pd.DataFrame(rows, columns=columns)


def plot_roth_action_mix(
    summary_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot action mix for UPDATE_1099 vs INVESTIGATE.
    """

    _validate_required_columns(summary_df, ["action", "count", "percent"])

    fig, ax = plt.subplots(figsize=(6, 4))
    if summary_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = summary_df.set_index("action")
    counts = data["count"].astype(int)
    percents = data["percent"] * 100

    ax.bar(counts.index.astype(str), counts.values, color="#F58518")
    ax.set_ylabel("Count")
    ax.set_title("Engine C Action Mix")

    max_count = float(counts.max() if len(counts) else 0)
    ax.set_ylim(0, max(1.0, max_count * 1.2))

    for idx, (count, pct) in enumerate(zip(counts.values, percents.values)):
        ax.text(
            idx,
            count + max(0.5, max_count * 0.03),
            f"{count} ({pct:.1f}%)",
            ha="center",
            va="bottom",
        )

    return fig, ax


def build_roth_correction_reason_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize correction reasons for review/correction rows.

    Required columns:
      - match_status
      - correction_reason
    """

    _validate_required_columns(df, ["match_status", "correction_reason"])

    columns = ["correction_reason", "count", "percent"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    relevant = df["match_status"].isin(
        [STATUS_CFG.needs_correction, STATUS_CFG.needs_review]
    )
    reasons = df.loc[relevant, "correction_reason"].dropna()
    if reasons.empty:
        return pd.DataFrame(columns=columns)

    bullet = ROTH_CFG.reason_bullet.strip()

    def _split_reasons(value: object) -> list[str]:
        text = str(value)
        parts = [part.strip() for part in text.splitlines() if part.strip()]
        cleaned = []
        for part in parts:
            if bullet and part.startswith(bullet):
                cleaned.append(part[len(bullet):].strip())
            else:
                cleaned.append(part)
        return [part for part in cleaned if part]

    exploded = reasons.apply(_split_reasons).explode()
    exploded = exploded.dropna()
    if exploded.empty:
        return pd.DataFrame(columns=columns)

    counts = exploded.value_counts().sort_values(ascending=False)
    total = int(counts.sum())

    summary = pd.DataFrame(
        {
            "correction_reason": counts.index.astype(str),
            "count": counts.values.astype(int),
        }
    )
    summary["percent"] = summary["count"] / total if total else 0.0

    return summary[columns]


def plot_roth_correction_reason_summary(
    summary_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot correction reasons for review/correction rows.
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
    ax.set_title("Engine C Correction Reasons")

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


def build_taxable_delta_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a taxable amount delta distribution (suggested - current).

    Required columns:
      - fed_taxable_amt
      - suggested_taxable_amt
    """

    _validate_required_columns(df, ["fed_taxable_amt", "suggested_taxable_amt"])

    columns = ["taxable_delta", "count"]
    if df.empty:
        return pd.DataFrame(columns=columns)

    current = pd.to_numeric(df["fed_taxable_amt"], errors="coerce")
    suggested = pd.to_numeric(df["suggested_taxable_amt"], errors="coerce")
    mask = current.notna() & suggested.notna()

    if not mask.any():
        return pd.DataFrame(columns=columns)

    delta = (suggested[mask] - current[mask]).round(2)
    counts = delta.value_counts().sort_index()

    return pd.DataFrame(
        {
            "taxable_delta": counts.index.astype(float),
            "count": counts.values.astype(int),
        },
        columns=columns,
    )


def plot_taxable_delta_distribution(
    metrics_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot taxable amount delta distribution.
    """

    _validate_required_columns(metrics_df, ["taxable_delta", "count"])

    fig, ax = plt.subplots(figsize=(8, 4))
    if metrics_df.empty:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        ax.set_axis_off()
        return fig, ax

    data = metrics_df.sort_values("taxable_delta")
    ax.bar(
        data["taxable_delta"].astype(float),
        data["count"].astype(int),
        color="#4C78A8",
        alpha=0.9,
    )
    ax.axvline(0, color="#E45756", linestyle="--", linewidth=2)
    ax.set_xlabel("Suggested - Current Taxable Amount")
    ax.set_ylabel("Count")
    ax.set_title("Engine C Taxable Delta Distribution")

    return fig, ax


def build_roth_tax_code_crosstab(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a correction-only crosstab of current vs suggested Roth tax codes.

    Required columns:
      - match_status
      - tax_code_1
      - tax_code_2
      - suggested_tax_code_1
      - suggested_tax_code_2
    """

    _validate_required_columns(
        df,
        [
            "match_status",
            "tax_code_1",
            "tax_code_2",
            "suggested_tax_code_1",
            "suggested_tax_code_2",
        ],
    )

    if df.empty:
        empty = pd.DataFrame()
        empty.index.name = "current_tax_code"
        empty.columns.name = "suggested_tax_code"
        return empty

    corrections = df[df["match_status"] == STATUS_CFG.needs_correction].copy()
    if corrections.empty:
        empty = pd.DataFrame()
        empty.index.name = "current_tax_code"
        empty.columns.name = "suggested_tax_code"
        return empty

    current_code = (
        corrections["tax_code_1"].fillna("").astype("string").str.strip()
        + corrections["tax_code_2"].fillna("").astype("string").str.strip()
    )
    suggested_code = (
        corrections["suggested_tax_code_1"].fillna("").astype("string").str.strip()
        + corrections["suggested_tax_code_2"].fillna("").astype("string").str.strip()
    )

    mask_suggested = suggested_code.ne("")
    if not mask_suggested.any():
        empty = pd.DataFrame()
        empty.index.name = "current_tax_code"
        empty.columns.name = "suggested_tax_code"
        return empty

    current_code = current_code.where(current_code.ne(""), other="Unknown")
    suggested_code = suggested_code.where(suggested_code.ne(""), other="Unknown")

    crosstab = pd.crosstab(
        current_code[mask_suggested],
        suggested_code[mask_suggested],
        dropna=False,
    )
    crosstab.index.name = "current_tax_code"
    crosstab.columns.name = "suggested_tax_code"
    return crosstab


def plot_roth_tax_code_crosstab(
    crosstab_df: pd.DataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    """
    Plot a correction-only current vs suggested Roth tax code cross-breakdown.
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
    ax.set_title("Engine C Corrections: Current vs Suggested Tax Codes")

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

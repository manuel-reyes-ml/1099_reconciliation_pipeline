"""

Matching and reconciliation between cleaned Relius and Matrix data.

This module expects:
- Relius data cleaned by src.clean_relius.clean_relius
- Matrix data cleaned by src.clean_matrix.clean_matrix

Main responsabilities:
- Optionally filter by plan_id(user_specified)
- Match Relius <-> Matrix on (plan_id, ssn, gross_amt)
- Apply date lag rule: 0 <= txn_date - exported_date <= max_date_lag_days
- Classify match status:
    - perfect_match
    - match_needs_correction (business rules)
    - date_out_of_range
    - unmatched_relius
    - unmatched_matrix
- Apply inherited-plan tax code rules:
    - cash-like distributions -> tax_code_1 should be 4
    - rollover distributions -> tax_code_1 'G', tax_code_2 '4'

"""

from __future__ import annotations

from email import errors
from typing import Iterable, Optional

import pandas as pd

from .config import (
    MATCHING_CONFIG,
    INHERITED_PLAN_IDS,
)



def _apply_inherited_tax_code_rules(df: pd.DataFrame) -> pd.DataFrame:

    """
    
    Apply inherited-plan tax code rules.

    Assumptions (based on current business logic):
    - INHERITED_PLAN_IDS contains plan_ids that represent inherited plans.
    - dist_category_relius comes from clean_relius (cash / rollover)
    - Matrix provides tax_code_1 and tax_code_2.

    Rules:
    - For inherited plans with 'rollover' or 'partial_rollover' category:
        expected tax_code_1 = 'G'
        expected tax_code_2 = '4'
    - For inherite plans with *other* categories (treated as cash-like):
        expected tax_code_1 = '4'
        expected tax_code_2 = <empty>
    
    This function:
    - Sets expected_tax_code_1 / expected_tax_code_2 where applicable
    - Compares Matrix tax_code_1 / tax_code_2 agains expectation
    - Flags rows that need correction
        
    """

    # Work on a copy so we don't mutate the cleaned pandas DataFrame
    df = df.copy()

    # Initialize expectation columns
    df["expected_tax_code_1"] = pd.NA
    df["expected_tax_code_2"] = pd.NA

    # Helper flags
    is_inherited = df["plan_id"].isin(INHERITED_PLAN_IDS)
    # Treat rollovers based on Relius distribution category
    is_rollover = df.get("dist_category_relius", "").isin(
        ["rollover", "partial_rollover"]
    )

    # Cash-like = inherited but not classified rollover
    is_cash_like = is_inherited & ~is_rollover

    # Apply expectation (only for rows that matched on both systems)
    both_mask = df["_merge"].eq("both")

    # Inherited rollover -> G / 4
    mask_rollover = both_mask & is_inherited & is_rollover
    df.loc[mask_rollover, "expected_tax_code_1"] = "4"
    df.loc[mask_rollover, "expected_tax_code_2"] = "G"

    # Inherited cash-like -> 4
    mask_cash = both_mask & is_cash_like
    df.loc[mask_cash, "expected_tax_code_1"] = "4"
    df.loc[mask_cash, "expected_tax_code_2"] = pd.NA

    # Compare expectations vs current Matrix codes
    code1 = df.get("tax_code_1", pd.Series(pd.NA, index=df.index)).fillna("")
    code2 = df.get("tax_code_2", pd.Series(pd.NA, index=df.index)).fillna("")
    exp1 = df["expected_tax_code_1"].fillna("")
    exp2 = df["expected_tax_code_2"].fillna("")

    df["code_matches_expected"] = (code1 == exp1) & (code2 == exp2)

    # Needs correction if we *have* an expectation and the current codes do not match
    df["needs_correction"] = (
        both_mask
        & df["expected_tax_code_1"].notna()
        & ~df["code_matches_expected"]
    )

    #Suggested codes (only where we have expectations)
    df["suggested_tax_code_1"] = df["expected_tax_code_1"].where(
        df["needs_correction"], other=pd.NA
    )
    df["suggested_tax_code_2"] = df["expected_tax_code_2"].where(
        df["needs_correction"], other=pd.NA
    )

    # Reason strings: differenciate cash vs rollover for easier debugging
    df["correction_reason"] = pd.NA
    df.loc[
        df["needs_correction"] & mask_rollover,
        "correction_reason",
    ] = "inherited_rollover_expected_G_and_4"
    df.loc[
        df["needs_correction"] & mask_cash,
        "correction_reason",
    ] = "inherited_cash_expected_4"

    # Default action for now: UPDATE_1099 on Matrix
    df["action"] = pd.NA
    df.loc[df["needs_correction"], "action"] = "UPDATE_1099"

    return df



def reconcile_relius_matrix(
        relius_clean: pd.DataFrame,
        matrix_clean: pd.DataFrame,
        plan_ids: Optional[Iterable[str]] = None,
        apply_business_rules: bool = True,
) -> pd.DataFrame:
    
    """

    Reconcile cleaned Relius and Matrix data.

    Args:
        relius_clean:
            Cleaned Relius DataFrame from clean_relius.clean_relius.
            Must contain at least:
                plan_id, ssn, gross_amt, exported_date,
                dist_code_1, dist_category_relius
        matrix_clean:
            Cleaned Matrix DataFrame from clean_matrix.clean_matrix.
            Must contain at least:
                plan_id, ssn, gross_amt, txn_date,
                tax_code_1, tax_code_2, matrix_account, transaction_id
        plan_ids:
            Optional iterable of plan_ids to restrict the reconciliation to.
            If provided, both datasets will be filtered to these plans
            BEFORE matching. This is ideal for your current case where:
                - Matrix contains ALL plans
                - Relius contains only inherited plans (subset)
        apply_business_rules:
            If True, apply inherited-plan tax code rules and flag
            corrections. If False, skip that step (just match & classify).

    Returns:
        A merged DataFrame with:
            - All matched and unmatched rows
            - Match status classification
            - Date lag information
            - Expected / suggested tax codes (if business rules applied)

    """

    # Work on copies
    r = relius_clean.copy()
    m = matrix_clean.copy()

    # Optionally filter by selected plan_ids (user-driven, not hard-coded)
    if plan_ids is not None:
        plan_ids = set(plan_ids)
        r = r[r["plan_id"].isin(plan_ids)].copy()
        m = m[m["plan_id"].isin(plan_ids)].copy()

    # Join keys: distribution should be uniquely identified by these
    join_keys = ["plan_id", "ssn", "gross_amt"]

    # Outer join to see unmatched records from both sides
    merged = r.merge(
        m,
        on=join_keys,
        how="outer",
        suffixes=("_relius", "_matrix"), # mostly used for overlapping names
        indicator=True,  # If True, adds a columns to the output DataFrame called "_merge" with information
                        # on the source of each row.
    )

    # Normalize dates and compute date lag
    if "exported_date" in merged.columns:
        merged["exported_date"] = pd.to_datetime(
            merged["exported_date"], errors="coerce"
        )
    if "txn_date" in merged.columns:
        merged["txn_date"] = pd.to_datetime(
            merged["txn_date"], errors="coerce"
        )

    if "exported_date" in merged.columns and "txn_date" in merged.columns:
        merged["date_lag_days"] = (
            merged["txn_date"] - merged["exported_date"]
        ).dt.days
        merged["date_within_tolerance"] = (
            merged["date_lag_days"].ge(0) #.ge is greater than or equal to '0' in this case
            & merged["date_lag_days"].le(MATCHING_CONFIG.max_date_lag_days) #.le is less than or equal to...
        )
    else:
        merged["date_lag_days"] = pd.NA
        merged["date_within_tolerance"] = False
    
    # Initialize match_status
    merged["match_status"] = pd.NA

    # Unmatched left / right
    merged.loc[merged["_merge"] == "left_only", "match_status"] = "unmatched_relius"
    merged.loc[merged["_merge"] == "right_only", "match_status"] = "unmatched_matrix"

    # Rows that exist in both systems
    both_mask = merged["_merge"] == "both"

    # Date out of range
    out_of_range = both_mask & ~merged["date_within_tolerance"]
    merged.loc[out_of_range, "match_status"] = "date_out_of_range"

    # For rows that are in both & within date tolerance,
    # we will decide between perfect_match vs match_needs_correction
    within_range = both_mask & merged["date_within_tolerance"]

    # Apply business rules (inherited plan tax-code logic)
    if apply_business_rules:
        merged = _apply_inherited_tax_code_rules(merged)
    else:
        merged["needs_correction"] = False
        merged["expected_tax_code_1"] = pd.NA
        merged["expected_tax_code_2"] = pd.NA
        merged["suggested_tax_code_1"] = pd.NA
        merged["suggested_tax_code_2"] = pd.NA
        merged["correction_reason"] = pd.NA
        merged["action"] = pd.NA
        merged["code_matches_expected"] = pd.NA

    # Perfect matches (within date range, no corrections needed)
    merged.loc[
        within_range & ~merged["needs_correction"],
        "match_status",
    ] = "perfect_match"

    # Matches that require correction (within date range, but codes wrong)
    merged.loc[
        within_range & merged["needs_correction"],
        "match_status",
    ] = "match_needs_correction"

    return merged
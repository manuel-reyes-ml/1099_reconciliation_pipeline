# Docstring for src/match_transactions module
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

from __future__ import annotations # Makes type hints like '-> pd.DataFrame', 'Optional[Iterable[str]] be stored as 
                                   #    strings and evaluated later.

from typing import Iterable, Optional  # Type hints helpers

import pandas as pd

from .config import (
    MATCHING_CONFIG,       # A dataclass instance with parameters such as max_date_lag_days.
    INHERITED_PLAN_IDS,    # A collecion (Set) of plan IDs that are considered "inherited".
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
        expected tax_code_1 = '4'
        expected tax_code_2 = 'G'
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

    # Initialize expectation columns with pandas missing value (<NA>)
    df["expected_tax_code_1"] = pd.NA
    df["expected_tax_code_2"] = pd.NA

    # Helper flags

    # Evaluate each row in 'plan_id' Series to check if it is in INHERITED_PLAN_IDS Set
    # Returns a boolean Series: If yes returns True -- If not returns False
    is_inherited = df["plan_id"].isin(INHERITED_PLAN_IDS)

    # Treat rollovers based on Relius distribution category
    # df.get("..") If columns exists returns a Series, If not returns the default value ""(single string)
    is_rollover = df.get("dist_category_relius", "").isin(
        ["rollover", "partial_rollover"]
    )

    # Cash-like = inherited but not classified rollover
    # & elementwise AND (for pandas Series)
    # ~ elementwise NOT (flips True <-> False)
    is_cash_like = is_inherited & ~is_rollover

    # Apply expectation (only for rows that matched on both systems)
    # .eq("..") applies 'is the single value equal to the df[".."] Series (for each row)
    # Returns a Series -> True or False
    both_mask = df["_merge"].eq("both")

    
    # Inherited rollover -> 4 / G
    # All three masks need to be True so mask_rollover is True, if not it returns False as a pandas Series (1 columns)
    mask_rollover = both_mask & is_inherited & is_rollover

    # .loc[mask, column] = value -> assign 'value' to the given column(Series) but only where mask(Series) is True
    df.loc[mask_rollover, "expected_tax_code_1"] = "4"
    df.loc[mask_rollover, "expected_tax_code_2"] = "G"

    
    # Inherited cash-like -> 4
    mask_cash = both_mask & is_cash_like

    # .loc[mask, column] = value -> assign 'value' to the given column(Series) but only where mask(Series) is True
    df.loc[mask_cash, "expected_tax_code_1"] = "4"
    df.loc[mask_cash, "expected_tax_code_2"] = pd.NA

    
    # Compare expectations vs current Matrix codes.
    # df.get(column, "if not exists use this") -> if column exists use it, if not, use a new Series filled with
    #  pd.NA for each row = (pd.Series(pd.NA, index=df.index).
    # .fillna("") -> replaces missing values (NaN/NA) with ""(empty string) so string comparisons don't break.
    code1 = df.get("tax_code_1", pd.Series(pd.NA, index=df.index)).fillna("")
    code2 = df.get("tax_code_2", pd.Series(pd.NA, index=df.index)).fillna("")
    exp1 = df["expected_tax_code_1"].fillna("")
    exp2 = df["expected_tax_code_2"].fillna("")

    # code1, code2, exp1 and exp2 are all Series (1 colum) and they can be compared using '=='
    # The comparison returns a boolean Series to a new column 'code_matches_expected'
    df["code_matches_expected"] = (code1 == exp1) & (code2 == exp2)

    # Needs correction if we *have* an expectation and the current codes do not match.
    # df["expected_tax_code_1"].notna() -> True for rows where we actually have an expected code, also meaning that
    #  the row is in an inherited plan where rules apply.
    # Again using pandas NOT operator (~) to reflect True the ones the codes didn't match
    df["needs_correction"] = (
        both_mask
        & df["expected_tax_code_1"].notna()
        & ~df["code_matches_expected"]
    )

    # Suggested codes (only where we have expectations)
    #
    # .where(cond, other=..) -> if cond is True keep original Value, if cond is False use 'other='
    df["suggested_tax_code_1"] = df["expected_tax_code_1"].where(
        df["needs_correction"], other=pd.NA
    )
    df["suggested_tax_code_2"] = df["expected_tax_code_2"].where(
        df["needs_correction"], other=pd.NA
    )

    # Reason strings: differenciate cash vs rollover for easier debugging
    # 1) Initialize column correction_reason with pandas NA
    # 2) For rows where mask_rollover and then were mask_cash is True their specific reason will be assigned to correction_reason
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
    # 1) Initialize column action with pandas NA
    # 2) For rows where needs_correction is True the string 'UPDATE_1099 will be assigned to 'action' column
    df["action"] = pd.NA
    df.loc[df["needs_correction"], "action"] = "UPDATE_1099"

    # Return DataFrame enriched with: expected codes, flags, suggested codes, reasons and action
    return df



def reconcile_relius_matrix(
        relius_clean: pd.DataFrame,                 # Type hint to expect DataFrame
        matrix_clean: pd.DataFrame,                 # Type hint to expect DataFrame
        plan_ids: Optional[Iterable[str]] = None,   # Type hint -> Can be a list/tuple/set/etc of strings or None
        apply_business_rules: bool = True,          # Type hint to expect boolean (True or False)
) -> pd.DataFrame:                                  # Type hint to expect the funtion returns a DataFrame
    
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
    # set() -> converts to iterable Set for faster plan ID checks.
    # r["plan_id"].isin(plan_ids) -> returns True on the rows when df plan_id is found in our Set plan_ids
    # Then the r[r[..]] filters and keep only the rows where True was returned
    # .copy() creates a copy of the DataFrame to be assigned to 'r' and 'm' in the next line
    # This allows you to reconcile only a subset of plans (e.g. inherited plans) without changing the matching logic
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
        suffixes=("_relius", "_matrix"), # mostly used for overlapping names in non-key columns
        indicator=True,  # If True, adds a columns to the output DataFrame called "_merge" with information
    )                    # on the source of each row: 'left_only'  -> row only in r(Relius)
                         #                            'right_only' -> row only in m(Matrix)
                         #                            'both'       -> rowmatched between both
    # how="outer" -> outer join keeps all rows from both tables:
    #   rows that only exist in Relius have NaNs on Matrix columns
    #   rows that only exist in Matrix have NaNs on Relius columns
    #   rows that exist in both are matched into one row                                                          
    

    # Normalize dates and compute date lag
    # pd.to_datetime(Series, errors="coerce") -> converts many date formats to pandas datetime, and assing NaT on invalid entries
    # Ensures both date columns are proper datetime type
    if "exported_date" in merged.columns:
        merged["exported_date"] = pd.to_datetime(
            merged["exported_date"], errors="coerce"
        )
    if "txn_date" in merged.columns:
        merged["txn_date"] = pd.to_datetime(
            merged["txn_date"], errors="coerce"
        )

    # If both columns are in merged DataFrame:
    #   1) Substract two datetime series
    #   2) .dt.days -> extract the difference in whole days(integer) 
    if "exported_date" in merged.columns and "txn_date" in merged.columns:
        merged["date_lag_days"] = (
            merged["txn_date"] - merged["exported_date"]
        ).dt.days
    #   3) date_lag_days must be between 0 and max tolerance (from MATCHING_CONFIG class)
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
    # We use "_merge" values to define "match_status":
    #   left_only   -> present only in Relius
    #   right_only  -> present only in Matrix
    merged.loc[merged["_merge"] == "left_only", "match_status"] = "unmatched_relius"
    merged.loc[merged["_merge"] == "right_only", "match_status"] = "unmatched_matrix"

    # Rows that exist in both systems
    both_mask = merged["_merge"] == "both"

    # Date out of range
    # for rows in both systems, but date outside of tolerance
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
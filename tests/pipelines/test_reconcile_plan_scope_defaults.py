import pandas as pd

from src.config import DEFAULT_RECONCILIATION_PLAN_IDS, MATCH_STATUS_CONFIG
from src.match_planid import reconcile_relius_matrix


def test_reconcile_defaults_filter_out_of_scope_plans() -> None:
    in_scope_plan = next(iter(DEFAULT_RECONCILIATION_PLAN_IDS))
    out_of_scope_plan = "999999ABC"

    relius_df = pd.DataFrame(
        {
            "plan_id": [in_scope_plan, out_of_scope_plan],
            "ssn": ["123456789", "555555555"],
            "gross_amt": [100.0, 200.0],
            "exported_date": ["2025-01-01", "2025-01-02"],
            "dist_category_relius": ["rollover", "cash"],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "plan_id": [in_scope_plan, out_of_scope_plan],
            "ssn": ["123456789", "999999999"],
            "gross_amt": [100.0, 300.0],
            "txn_date": ["2025-01-05", "2025-01-07"],
            "transaction_id": ["tx1", "tx2"],
            "tax_code_1": ["7", "7"],
            "tax_code_2": ["", ""],
        }
    )

    result = reconcile_relius_matrix(
        relius_df,
        matrix_df,
        plan_ids=None,
        apply_business_rules=False,
    )

    assert DEFAULT_RECONCILIATION_PLAN_IDS is not None
    assert out_of_scope_plan not in DEFAULT_RECONCILIATION_PLAN_IDS
    assert result["plan_id"].isin(DEFAULT_RECONCILIATION_PLAN_IDS).all()
    assert (
        result["match_status"] == MATCH_STATUS_CONFIG.unmatched_matrix
    ).sum() == 0


def test_reconcile_explicit_plan_ids_override_default() -> None:
    in_scope_plan = next(iter(DEFAULT_RECONCILIATION_PLAN_IDS))
    out_of_scope_plan = "999999ABC"

    relius_df = pd.DataFrame(
        {
            "plan_id": [in_scope_plan],
            "ssn": ["123456789"],
            "gross_amt": [100.0],
            "exported_date": ["2025-01-01"],
            "dist_category_relius": ["rollover"],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "plan_id": [out_of_scope_plan],
            "ssn": ["999999999"],
            "gross_amt": [300.0],
            "txn_date": ["2025-01-07"],
            "transaction_id": ["tx2"],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
        }
    )

    result = reconcile_relius_matrix(
        relius_df,
        matrix_df,
        plan_ids=[out_of_scope_plan],
        apply_business_rules=False,
    )

    assert result.shape[0] == 1
    assert result.loc[0, "plan_id"] == out_of_scope_plan
    assert result.loc[0, "match_status"] == MATCH_STATUS_CONFIG.unmatched_matrix

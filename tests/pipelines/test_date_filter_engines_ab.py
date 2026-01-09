import pandas as pd

from src.config import DateFilterConfig, DEFAULT_RECONCILIATION_PLAN_IDS, MATCH_STATUS_CONFIG
from src.engines.age_taxcode_analysis import run_age_taxcode_analysis
from src.engines.match_planid import reconcile_relius_matrix


def test_engine_a_date_filter_range() -> None:
    plan_id = next(iter(DEFAULT_RECONCILIATION_PLAN_IDS))
    relius_df = pd.DataFrame(
        {
            "plan_id": [plan_id, plan_id],
            "ssn": ["123456780", "123456781"],
            "gross_amt": [100.0, 200.0],
            "exported_date": ["2025-01-05", "2025-02-05"],
            "dist_category_relius": ["rollover", "cash"],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "plan_id": [plan_id, plan_id],
            "ssn": ["123456780", "123456781"],
            "gross_amt": [100.0, 200.0],
            "txn_date": ["2025-01-07", "2025-02-07"],
            "transaction_id": ["tx-jan", "tx-feb"],
            "tax_code_1": ["7", "7"],
            "tax_code_2": ["", ""],
        }
    )

    date_filter = DateFilterConfig(date_start="2025-01-01", date_end="2025-01-31")
    result = reconcile_relius_matrix(
        relius_df,
        matrix_df,
        plan_ids=[plan_id],
        apply_business_rules=False,
        date_filter=date_filter,
    )

    assert result.shape[0] == 1
    assert result.iloc[0]["ssn"] == "123456780"
    assert result.iloc[0]["match_status"] == MATCH_STATUS_CONFIG.no_action


def test_engine_b_month_filter() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["200001A", "200001A"],
            "ssn": ["123456780", "123456780"],
            "txn_date": [pd.Timestamp("2025-07-15"), pd.Timestamp("2025-08-15")],
            "transaction_id": ["tx-jul", "tx-aug"],
            "participant_name": ["Alex", "Alex"],
            "matrix_account": ["acct", "acct"],
            "tax_code_1": ["7", "7"],
            "tax_code_2": ["", ""],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["200001A"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1971-07-01")],
            "term_date": [pd.NaT],
            "first_name": ["Alex"],
            "last_name": ["Tester"],
        }
    )

    date_filter = DateFilterConfig(months=["July"])
    result = run_age_taxcode_analysis(matrix_df, relius_demo_df, date_filter=date_filter)

    assert result.shape[0] == 1
    assert result["transaction_id"].tolist() == ["tx-jul"]

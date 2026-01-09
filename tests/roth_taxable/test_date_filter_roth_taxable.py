import pandas as pd

from src.config import DateFilterConfig
from src.engines.roth_taxable_analysis import run_roth_taxable_analysis


def test_roth_taxable_date_filter_combined_range_and_month() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300005ABC", "300005ABC"],
            "ssn": ["123456780", "123456780"],
            "txn_date": [pd.Timestamp("2025-07-10"), pd.Timestamp("2025-08-10")],
            "transaction_id": ["tx-july", "tx-aug"],
            "participant_name": ["Alex", "Alex"],
            "matrix_account": ["acct", "acct"],
            "gross_amt": [100.0, 200.0],
            "fed_taxable_amt": [0.0, 0.0],
            "roth_initial_contribution_year": [2019, 2019],
            "tax_code_1": ["B", "B"],
            "tax_code_2": ["7", "7"],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["300005ABC"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1960-01-01")],
            "term_date": [pd.NaT],
        }
    )
    relius_roth_basis_df = pd.DataFrame(
        {
            "plan_id": ["300005ABC"],
            "ssn": ["123456780"],
            "first_roth_tax_year": [2019],
            "roth_basis_amt": [1000.0],
        }
    )

    date_filter = DateFilterConfig(
        date_start="2025-07-01",
        date_end="2025-08-31",
        months=["July"],
    )
    result = run_roth_taxable_analysis(
        matrix_df,
        relius_demo_df,
        relius_roth_basis_df,
        date_filter=date_filter,
    )

    assert result.shape[0] == 1
    assert result["transaction_id"].tolist() == ["tx-july"]

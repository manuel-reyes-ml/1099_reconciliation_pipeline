import pandas as pd

from src.roth_taxable_analysis import run_roth_taxable_analysis


def test_run_roth_taxable_masks_invalid_start_years() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300005A", "300005B"],
            "ssn": ["111111111", "222222222"],
            "txn_date": [pd.Timestamp("2024-01-15"), pd.Timestamp("2024-02-20")],
            "transaction_id": ["t1", "t2"],
            "participant_name": ["Alice", "Bob"],
            "matrix_account": ["acct1", "acct2"],
            "gross_amt": [100.0, 200.0],
            "fed_taxable_amt": [50.0, 100.0],
            "roth_initial_contribution_year": [2010, 0],
            "tax_code_1": ["B", "B"],
            "tax_code_2": ["", ""],
        }
    )

    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["300005A", "300005B"],
            "ssn": ["111111111", "222222222"],
            "dob": [pd.Timestamp("1970-01-01"), pd.Timestamp("1975-01-01")],
        }
    )

    relius_roth_basis_df = pd.DataFrame(
        {
            "plan_id": ["300005A", "300005B"],
            "ssn": ["111111111", "222222222"],
            "first_roth_tax_year": [2012, 0],
            "roth_basis_amt": [500.0, 400.0],
        }
    )

    result = run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df)

    expected = pd.Series([2012, pd.NA], name="start_roth_year", dtype="Int64")
    pd.testing.assert_series_equal(result["start_roth_year"], expected)
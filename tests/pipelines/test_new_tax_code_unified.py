import pandas as pd

from src.age_taxcode_analysis import run_age_taxcode_analysis
from src.build_correction_file import build_correction_dataframe
from src.match_planid import reconcile_relius_matrix
from src.roth_taxable_analysis import run_roth_taxable_analysis


# Engine A
def test_match_planid_sets_new_tax_code() -> None:
    relius_df = pd.DataFrame(
        {
            "plan_id": ["300004PLAT"],
            "ssn": ["123456780"],
            "gross_amt": [100.0],
            "exported_date": ["2025-01-01"],
            "dist_category_relius": ["rollover"],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300004PLAT"],
            "ssn": ["123456780"],
            "gross_amt": [100.0],
            "txn_date": ["2025-01-05"],
            "transaction_id": ["tx1"],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
        }
    )

    result = reconcile_relius_matrix(relius_df, matrix_df, apply_business_rules=True)

    assert result.loc[0, "new_tax_code"] == "4G"


def test_match_planid_no_action_clears_suggested_codes() -> None:
    relius_df = pd.DataFrame(
        {
            "plan_id": ["300004PLAT"],
            "ssn": ["123456780"],
            "gross_amt": [100.0],
            "exported_date": ["2025-01-01"],
            "dist_category_relius": ["rollover"],
        }
    )
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300004PLAT"],
            "ssn": ["123456780"],
            "gross_amt": [100.0],
            "txn_date": ["2025-01-05"],
            "transaction_id": ["tx1b"],
            "tax_code_1": ["4"],
            "tax_code_2": ["G"],
        }
    )

    result = reconcile_relius_matrix(relius_df, matrix_df, apply_business_rules=True)

    assert result.loc[0, "match_status"] == "match_no_action"
    assert pd.isna(result.loc[0, "suggested_tax_code_1"])
    assert pd.isna(result.loc[0, "suggested_tax_code_2"])
    assert pd.isna(result.loc[0, "new_tax_code"])


# Engine B
def test_age_taxcode_analysis_sets_new_tax_code() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["200001A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-06-01")],
            "transaction_id": ["tx2"],
            "participant_name": ["Alex"],
            "matrix_account": ["acct2"],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
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

    result = run_age_taxcode_analysis(matrix_df, relius_demo_df)

    assert result.loc[0, "new_tax_code"] == "1"


def test_age_taxcode_no_action_clears_correction_reason() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["200001A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-06-01")],
            "transaction_id": ["tx2b"],
            "participant_name": ["Alex"],
            "matrix_account": ["acct2b"],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["200001A"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1960-01-01")],
            "term_date": [pd.NaT],
            "first_name": ["Alex"],
            "last_name": ["Tester"],
        }
    )

    result = run_age_taxcode_analysis(matrix_df, relius_demo_df)

    assert result.loc[0, "match_status"] == "match_no_action"
    assert pd.isna(result.loc[0, "correction_reason"])
    assert pd.isna(result.loc[0, "suggested_tax_code_1"])
    assert pd.isna(result.loc[0, "suggested_tax_code_2"])
    assert pd.isna(result.loc[0, "new_tax_code"])


# Engine C
def test_roth_taxable_analysis_sets_new_tax_code() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-03-01")],
            "transaction_id": ["tx3"],
            "participant_name": ["Roth"],
            "matrix_account": ["acct3"],
            "gross_amt": [150.0],
            "fed_taxable_amt": [50.0],
            "roth_initial_contribution_year": [2015],
            "tax_code_1": ["B"],
            "tax_code_2": ["G"],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1970-01-01")],
            "term_date": [pd.NaT],
        }
    )
    relius_roth_basis_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "first_roth_tax_year": [2010],
            "roth_basis_amt": [500.0],
        }
    )

    result = run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df)

    assert result.loc[0, "new_tax_code"] == "H"


def test_roth_taxable_no_action_clears_correction_reason() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-03-01")],
            "transaction_id": ["tx3b"],
            "participant_name": ["Roth"],
            "matrix_account": ["acct3b"],
            "gross_amt": [100.0],
            "fed_taxable_amt": [0.0],
            "roth_initial_contribution_year": [2010],
            "tax_code_1": ["B"],
            "tax_code_2": ["1"],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1980-01-01")],
            "term_date": [pd.NaT],
        }
    )
    relius_roth_basis_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "first_roth_tax_year": [2010],
            "roth_basis_amt": [200.0],
        }
    )

    result = run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df)

    assert result.loc[0, "match_status"] == "match_no_action"
    assert pd.isna(result.loc[0, "correction_reason"])
    assert pd.isna(result.loc[0, "suggested_tax_code_1"])
    assert pd.isna(result.loc[0, "suggested_tax_code_2"])
    assert pd.isna(result.loc[0, "new_tax_code"])
    assert pd.isna(result.loc[0, "suggested_taxable_amt"])


def test_build_correction_dataframe_exports_new_tax_code() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["200001A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-06-01")],
            "transaction_id": ["tx4"],
            "participant_name": ["Alex"],
            "matrix_account": ["acct4"],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
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

    analysis_df = run_age_taxcode_analysis(matrix_df, relius_demo_df)
    corrections_df = build_correction_dataframe(analysis_df)

    assert "New Tax Code" in corrections_df.columns
    assert "New Tax Code 1" not in corrections_df.columns
    assert corrections_df.loc[0, "New Tax Code"] == "1"


def test_build_correction_dataframe_exports_combined_new_tax_code() -> None:
    matrix_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "txn_date": [pd.Timestamp("2025-03-01")],
            "transaction_id": ["tx5"],
            "participant_name": ["Roth"],
            "matrix_account": ["acct5"],
            "gross_amt": [150.0],
            "fed_taxable_amt": [50.0],
            "roth_initial_contribution_year": [2015],
            "tax_code_1": ["7"],
            "tax_code_2": [""],
        }
    )
    relius_demo_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "dob": [pd.Timestamp("1978-01-01")],
            "term_date": [pd.NaT],
        }
    )
    relius_roth_basis_df = pd.DataFrame(
        {
            "plan_id": ["300005A"],
            "ssn": ["123456780"],
            "first_roth_tax_year": [2010],
            "roth_basis_amt": [500.0],
        }
    )

    result = run_roth_taxable_analysis(matrix_df, relius_demo_df, relius_roth_basis_df)
    corrections_df = build_correction_dataframe(result)

    assert result.loc[0, "new_tax_code"] == "B1"
    assert corrections_df.loc[0, "New Tax Code"] == "B1"

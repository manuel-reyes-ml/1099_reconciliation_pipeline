import pandas as pd

from src.cleaning.clean_matrix import clean_matrix
from src.config import DateFilterConfig


def test_clean_matrix_cross_field_issue_added() -> None:
    raw_df = pd.DataFrame(
        {
            "Matrix Account": ["07A0001"],
            "Client Account": ["PLAN1"],
            "Participant SSN": ["123456780"],
            "Participant Name": ["Jane Doe"],
            "Participant State": ["CA"],
            "Gross Amount": [1000.0],
            "Fed Taxable Amount": [200.0],
            "Transaction Date": ["2020-01-02"],
            "Transaction Type": ["Check"],
            "Tax Code": ["G"],
            "Tax Code 2": [pd.NA],
            "Tax Form": ["1099-R"],
            "Distribution Type": ["Rollover"],
            "Transaction Id": [123450.0],
            "Roth Initial Contribution Year": [2010],
        }
    )

    cleaned = clean_matrix(raw_df, drop_rows_missing_keys=False)

    assert cleaned["ssn_valid"].tolist() == [True]
    assert cleaned["amount_valid"].tolist() == [True]
    assert cleaned["date_valid"].tolist() == [True]
    assert cleaned["code_1099r_valid"].tolist() == [True]
    assert cleaned["validation_issues"].tolist() == [
        ["cross_code_g_taxable_over_10pct"],
    ]


def test_clean_matrix_missing_txn_date_skips_date_filter() -> None:
    raw_df = pd.DataFrame(
        {
            "Matrix Account": ["07A0001"],
            "Client Account": ["PLAN1"],
            "Participant SSN": ["123456780"],
            "Participant Name": ["Jane Doe"],
            "Participant State": ["CA"],
            "Gross Amount": [1000.0],
            "Fed Taxable Amount": [200.0],
            "Transaction Type": ["Check"],
            "Tax Code": ["7"],
            "Tax Code 2": [pd.NA],
            "Tax Form": ["1099-R"],
            "Distribution Type": ["Rollover"],
            "Transaction Id": [123450.0],
            "Roth Initial Contribution Year": [2010],
        }
    )

    date_filter = DateFilterConfig(date_start="2025-01-01", date_end="2025-01-31")
    cleaned = clean_matrix(raw_df, drop_rows_missing_keys=False, date_filter=date_filter)

    assert cleaned.shape[0] == 1
    assert "txn_date" not in cleaned.columns
    assert cleaned["date_valid"].isna().all()


def test_clean_matrix_normalizes_participant_name() -> None:
    raw_df = pd.DataFrame(
        {
            "Matrix Account": ["07A0001"],
            "Client Account": ["PLAN1"],
            "Participant SSN": ["123456780"],
            "Participant Name": ["  Jane Doe  "],
            "Gross Amount": [1000.0],
            "Transaction Date": ["2020-01-02"],
        }
    )

    cleaned = clean_matrix(raw_df, drop_rows_missing_keys=False)

    assert cleaned.loc[0, "participant_name"] == "Jane Doe"
    assert "partipant_name" not in cleaned.columns


def test_clean_matrix_preserves_string_transaction_id() -> None:
    raw_df = pd.DataFrame(
        {
            "Transaction Id": ["44324568"],
            "Transaction Date": ["2025-01-01"],
            "Client Account": ["PLAN1"],
            "Participant SSN": ["123456780"],
            "Gross Amount": [100.0],
        }
    )

    cleaned = clean_matrix(raw_df, drop_rows_missing_keys=False)

    assert cleaned.loc[0, "transaction_id"] == "44324568"

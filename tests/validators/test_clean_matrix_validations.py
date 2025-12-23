import pandas as pd

from src.clean_matrix import clean_matrix


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

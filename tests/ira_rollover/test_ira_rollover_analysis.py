import pandas as pd
import pytest

from src.engines.ira_rollover_analysis import run_ira_rollover_analysis


def test_run_ira_rollover_analysis_filters_and_classifies() -> None:
    df = pd.DataFrame(
        {
            "plan_id": ["300001XYZ", "acct-ira-1", "300002ABC", "300001XYZ"],
            "txn_method": [
                "Check Distribution",
                "Check Distribution ",
                "Check Distribution",
                "ACH Distribution",
            ],
            "federal_taxing_method": ["Rollover", "rollover", "Rollover", "Rollover"],
            "tax_form": ["No Tax", "1099-R", "No Tax", "No Tax"],
            "transaction_id": ["tx1", "tx2", "tx3", "tx4"],
            "txn_date": [
                pd.Timestamp("2025-01-10"),
                pd.Timestamp("2025-01-11"),
                pd.Timestamp("2025-01-12"),
                pd.Timestamp("2025-01-13"),
            ],
            "ssn": ["123456780", "123456781", "123456782", "123456783"],
            "matrix_account": ["acct1", "acct2", "acct3", "acct4"],
            "participant_name": ["A", "B", "C", "D"],
        }
    )

    result = run_ira_rollover_analysis(df)

    assert set(result["transaction_id"]) == {"tx1", "tx2"}

    result = result.set_index("transaction_id")
    assert result.loc["tx1", "match_status"] == "match_no_action"
    assert pd.isna(result.loc["tx1", "new_tax_code"])
    assert pd.isna(result.loc["tx1", "action"])

    assert result.loc["tx2", "match_status"] == "match_needs_correction"
    assert result.loc["tx2", "action"] == "UPDATE_1099"
    assert result.loc["tx2", "suggested_tax_code_1"] == "0"
    assert result.loc["tx2", "new_tax_code"] == "0"
    assert (
        result.loc["tx2", "correction_reason"]
        == "ira_rollover_tax_form_1099r_expected_no_tax"
    )


def test_run_ira_rollover_analysis_review_reasons() -> None:
    df = pd.DataFrame(
        {
            "plan_id": ["IRA-PLAN", "300001XYZ", "300001XYZ", "IRA-PLAN2"],
            "txn_method": [
                "check distribution",
                "check distribution",
                "check distribution",
                "check distribution",
            ],
            "federal_taxing_method": [pd.NA, "Rollover", "Taxable", "Rollover"],
            "tax_form": ["No Tax", pd.NA, "No Tax", "Other Form"],
            "transaction_id": [
                "tx_missing_ftm",
                "tx_missing_tax_form",
                "tx_non_rollover",
                "tx_unknown_form",
            ],
            "txn_date": [
                pd.Timestamp("2025-02-01"),
                pd.Timestamp("2025-02-02"),
                pd.Timestamp("2025-02-03"),
                pd.Timestamp("2025-02-04"),
            ],
            "ssn": ["123456780", "123456781", "123456782", "123456783"],
            "matrix_account": ["acct1", "acct2", "acct3", "acct4"],
            "participant_name": ["A", "B", "C", "D"],
        }
    )

    result = run_ira_rollover_analysis(df).set_index("transaction_id")

    assert result.loc["tx_missing_ftm", "match_status"] == "match_needs_review"
    assert "missing_federal_taxing_method" in result.loc[
        "tx_missing_ftm", "correction_reason"
    ]

    assert result.loc["tx_missing_tax_form", "match_status"] == "match_needs_review"
    assert "missing_tax_form" in result.loc[
        "tx_missing_tax_form", "correction_reason"
    ]

    assert result.loc["tx_non_rollover", "match_status"] == "match_needs_review"
    assert "federal_taxing_method_not_rollover" in result.loc[
        "tx_non_rollover", "correction_reason"
    ]

    assert result.loc["tx_unknown_form", "match_status"] == "match_needs_review"
    assert "unrecognized_tax_form" in result.loc[
        "tx_unknown_form", "correction_reason"
    ]


def test_run_ira_rollover_analysis_missing_columns() -> None:
    df = pd.DataFrame(
        {
            "plan_id": ["300001XYZ"],
            "txn_method": ["Check Distribution"],
            "federal_taxing_method": ["Rollover"],
            "transaction_id": ["tx_missing"],
            "txn_date": [pd.Timestamp("2025-01-10")],
            "ssn": ["123456780"],
            "matrix_account": ["acct1"],
            "participant_name": ["A"],
        }
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        run_ira_rollover_analysis(df)


def test_run_ira_rollover_analysis_requires_name() -> None:
    df = pd.DataFrame(
        {
            "plan_id": ["300001XYZ"],
            "txn_method": ["Check Distribution"],
            "federal_taxing_method": ["Rollover"],
            "tax_form": ["No Tax"],
            "transaction_id": ["tx_name_missing"],
            "txn_date": [pd.Timestamp("2025-01-10")],
            "ssn": ["123456780"],
            "matrix_account": ["acct1"],
        }
    )

    with pytest.raises(ValueError, match="participant_name or full_name"):
        run_ira_rollover_analysis(df)

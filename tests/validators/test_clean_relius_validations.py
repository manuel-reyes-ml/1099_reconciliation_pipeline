import pandas as pd

from src.cleaning.clean_relius import clean_relius


def test_clean_relius_adds_validation_flags_and_issues() -> None:
    raw_df = pd.DataFrame(
        {
            "PLANID_1": ["PLAN1", "PLAN1"],
            "SSNUM_1": ["123456780", "000000000"],
            "FIRSTNAM": ["Jane", "Jim"],
            "LASTNAM": ["Doe", "Doe"],
            "STATEADDR": ["CA", "CA"],
            "GROSSDISTRAMT": [1000.0, -10.0],
            "EXPORTEDDATE": ["2020-01-01", "2020-01-02"],
            "DISTR1CD": ["7", "Z"],
            "TAXYR": [2020, 2020],
            "DISTRNAM": ["Rollover", "Cash"],
        }
    )

    cleaned = clean_relius(raw_df, drop_rows_missing_keys=False)

    assert cleaned["ssn_valid"].tolist() == [True, False]
    assert cleaned["amount_valid"].tolist() == [True, False]
    assert cleaned["date_valid"].tolist() == [True, True]
    assert cleaned["code_1099r_valid"].tolist() == [True, False]
    assert cleaned["validation_issues"].tolist() == [
        [],
        ["ssn_invalid", "amount_invalid", "code_1099r_invalid"],
    ]

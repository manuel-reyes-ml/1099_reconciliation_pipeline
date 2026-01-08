import pandas as pd
from datetime import date

from src.core.normalizers import (
    build_validation_issues,
    cross_validate_series,
    validate_1099r_code_series,
    validate_amounts_series,
    validate_dates_series,
    validate_ssn,
    validate_ssn_series,
)


def test_validate_ssn_rules() -> None:
    assert validate_ssn("123456780") is True
    assert validate_ssn("000000000") is False
    assert validate_ssn("999999999") is False
    assert validate_ssn("123456789") is False
    assert validate_ssn("012345678") is False
    assert validate_ssn("900123456") is False


def test_validate_ssn_series() -> None:
    series = pd.Series(["123456780", "000000000", pd.NA], dtype="string")
    result = validate_ssn_series(series)
    assert result.tolist() == [True, False, False]


def test_validate_amounts_series_rules() -> None:
    gross = pd.Series([1000.0, -50.0, 50.0, 20.0])
    taxable = pd.Series([500.0, 10.0, -5.0, 200.0])

    result = validate_amounts_series(gross, taxable)

    assert result.tolist() == [True, False, False, False]


def test_validate_dates_series_rules() -> None:
    today = date(2024, 1, 10)
    dist_dates = pd.Series([
        "2024-01-01",
        "1989-12-31",
        "2051-01-01",
        "2024-01-11",
    ])

    result = validate_dates_series(dist_dates, today=today)

    assert result.tolist() == [True, False, False, False]


def test_validate_1099r_code_series_rules() -> None:
    codes = pd.Series(["7", "G", "x", None, "11"])

    result = validate_1099r_code_series(codes)

    assert result.tolist() == [True, True, False, False, False]


def test_cross_validation_and_issue_builder() -> None:
    gross = pd.Series([1000.0, 1000.0, 1000.0])
    taxable = pd.Series([200.0, 2000.0, 50.0])
    codes = pd.Series(["G", "7", "1"], dtype="string")
    ages = pd.Series([pd.NA, pd.NA, 60.0])

    cross_issues = cross_validate_series(gross, taxable, codes, age=ages)

    assert cross_issues.tolist() == [
        ["cross_code_g_taxable_over_10pct"],
        ["cross_taxable_exceeds_gross_150pct"],
        ["cross_code1_age_over_59_5"],
    ]

    ssn_valid = pd.Series([True, False, True])
    amount_valid = pd.Series([True, True, False])
    date_valid = pd.Series([True, True, True])
    code_valid = pd.Series([True, True, True])

    issues = build_validation_issues(
        ssn_valid,
        amount_valid,
        date_valid,
        code_valid,
        cross_field_issues=cross_issues,
    )

    assert issues.tolist() == [
        ["cross_code_g_taxable_over_10pct"],
        ["ssn_invalid", "cross_taxable_exceeds_gross_150pct"],
        ["amount_invalid", "cross_code1_age_over_59_5"],
    ]

# Docstring for src/validators module
"""
validators.py

Shared validation helpers for canonical data checks across cleaners and engines.

This module centralizes validation logic (SSN, dates, amounts, tax codes, and
cross-field checks) to keep cleaning modules consistent. It also provides
date-filter configuration validation utilities used by cleaning/engine filters.

Public API
----------
- validate_ssn(ssn_clean) -> bool
- validate_ssn_series(series) -> pd.Series
- validate_amounts(gross, taxable=None, fed_withhold=None, is_correction=False) -> bool
- validate_amounts_series(gross, taxable=None, fed_withhold=None, is_correction=None) -> pd.Series
- validate_dates(dist_date, pay_date=None, today=None) -> bool
- validate_dates_series(dist_dates, pay_dates=None, today=None) -> pd.Series
- validate_1099r_code(code) -> bool
- validate_1099r_code_series(series) -> pd.Series
- cross_validate(gross, taxable, code, age=None) -> list[str]
- cross_validate_series(gross, taxable, code, age=None) -> pd.Series
- build_validation_issues(ssn_valid, amount_valid, date_valid, code_1099r_valid, cross_field_issues=None) -> pd.Series
- normalize_date_filter_config(date_filter=None) -> tuple[date | None, date | None, tuple[int, ...] | None]

Internal helpers
----------------
Underscore-prefixed helpers are intentionally not part of the public API.
"""

from __future__ import annotations

from datetime import date, datetime
from numbers import Integral
from typing import Any

import pandas as pd

from ..config import DATE_FILTER_ALL, DATE_FILTER_CONFIG, DateFilterConfig

VALID_1099R_CODES = {
    "1",
    "2",
    "4",
    "7",
    "8",
    "B",
    "G",
    "H",
    "L",
    "P",
    "Q",
}

_MONTH_ALIASES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _coerce_date_value(value: object, field_name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        parsed = pd.to_datetime(value, errors="raise")
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Invalid {field_name}: {value!r}. Expected a date or YYYY-MM-DD string."
        ) from exc
    if pd.isna(parsed):
        raise ValueError(
            f"Invalid {field_name}: {value!r}. Expected a date or YYYY-MM-DD string."
        )
    return parsed.date()


def _coerce_month_value(value: object) -> int:
    if value is None:
        raise ValueError("Month values must be provided as names or 1-12.")
    if isinstance(value, Integral) and not isinstance(value, bool):
        month = int(value)
    else:
        value_str = str(value).strip()
        if not value_str:
            raise ValueError("Month values must be provided as names or 1-12.")
        value_lower = value_str.lower()
        if value_lower.isdigit():
            month = int(value_lower)
        else:
            month = _MONTH_ALIASES.get(value_lower)
            if month is None:
                raise ValueError(
                    f"Invalid month value: {value!r}. Expected a month name or number."
                )
    if month < 1 or month > 12:
        raise ValueError(
            f"Invalid month value: {value!r}. Expected a month number between 1 and 12."
        )
    return month


def _normalize_months_config(months: object) -> tuple[int, ...] | None:
    if months is None:
        return None
    if isinstance(months, str):
        if months.strip().lower() == DATE_FILTER_ALL:
            return None
        month_values = [months]
    elif isinstance(months, Integral) and not isinstance(months, bool):
        month_values = [months]
    else:
        try:
            month_values = list(months)  # type: ignore[arg-type]
        except TypeError as exc:
            raise ValueError(
                f"Invalid months configuration: {months!r}. Expected a month name or iterable."
            ) from exc
    if any(
        isinstance(value, str) and value.strip().lower() == DATE_FILTER_ALL
        for value in month_values
    ):
        raise ValueError("Months cannot include 'all' alongside specific month values.")
    normalized = tuple(sorted({_coerce_month_value(value) for value in month_values}))
    if not normalized:
        raise ValueError("Months must include at least one valid month value.")
    return normalized


def normalize_date_filter_config(
    date_filter: DateFilterConfig | None = None,
) -> tuple[date | None, date | None, tuple[int, ...] | None]:
    cfg = date_filter or DATE_FILTER_CONFIG
    date_start = _coerce_date_value(cfg.date_start, "date_start")
    date_end = _coerce_date_value(cfg.date_end, "date_end")
    months = _normalize_months_config(cfg.months)
    if date_start is not None and date_end is not None and date_start > date_end:
        raise ValueError(
            f"Invalid date range: date_start {date_start} is after date_end {date_end}."
        )
    return date_start, date_end, months


def validate_ssn(ssn_clean: Any) -> bool:
    """Validate cleaned 9-digit SSN per docs/data_dictionary.md §3.6."""
    if pd.isna(ssn_clean):
        return False
    ssn_str = str(ssn_clean).strip()
    if len(ssn_str) != 9 or not ssn_str.isdigit():
        return False
    if ssn_str in {"000000000", "999999999", "012345678", "123456789"}:
        return False
    area = ssn_str[:3]
    if area in {"000", "666"} or area.startswith("9"):
        return False
    return True


def validate_ssn_series(series: pd.Series) -> pd.Series:
    """Vectorized SSN validation with boolean output."""
    return series.map(validate_ssn).astype("boolean")


def validate_amounts(
    gross: Any,
    taxable: Any | None = None,
    fed_withhold: Any | None = None,
    *,
    is_correction: bool = False,
) -> bool:
    """Validate numeric amount relationships per docs/data_dictionary.md §3.6."""
    gross_value = pd.to_numeric(gross, errors="coerce")
    if pd.isna(gross_value):
        return False
    if gross_value < 0 and not is_correction:
        return False
    if abs(gross_value) > 10_000_000:
        return False
    if taxable is not None and not pd.isna(taxable):
        taxable_value = pd.to_numeric(taxable, errors="coerce")
        if pd.isna(taxable_value):
            return False
        if taxable_value < 0:
            return False
        if taxable_value > gross_value:
            return False
    elif taxable is not None:
        return False
    if fed_withhold is not None and not pd.isna(fed_withhold):
        fed_value = pd.to_numeric(fed_withhold, errors="coerce")
        if pd.isna(fed_value):
            return False
        if fed_value > gross_value:
            return False
    elif fed_withhold is not None:
        return False
    return True


def validate_amounts_series(
    gross: pd.Series,
    taxable: pd.Series | None = None,
    fed_withhold: pd.Series | None = None,
    *,
    is_correction: pd.Series | None = None,
) -> pd.Series:
    """Vectorized amount validation with boolean output."""
    gross_series = pd.to_numeric(gross, errors="coerce")
    valid = gross_series.notna()

    if is_correction is None:
        correction = pd.Series(False, index=gross_series.index)
    else:
        correction = is_correction.fillna(False)

    valid &= ~(gross_series < 0) | correction
    valid &= gross_series.abs() <= 10_000_000

    if taxable is not None:
        taxable_series = pd.to_numeric(taxable, errors="coerce")
        valid &= taxable_series.notna()
        valid &= taxable_series >= 0
        valid &= taxable_series <= gross_series

    if fed_withhold is not None:
        fed_series = pd.to_numeric(fed_withhold, errors="coerce")
        valid &= fed_series.notna()
        valid &= fed_series <= gross_series

    return valid.astype("boolean")


def validate_dates(
    dist_date: Any,
    pay_date: Any | None = None,
    *,
    today: date | None = None,
) -> bool:
    """Validate date relationships per docs/data_dictionary.md §3.6."""
    dist_dt = pd.to_datetime(dist_date, errors="coerce")
    if pd.isna(dist_dt):
        return False
    today_value = today or date.today()
    today_ts = pd.Timestamp(today_value)
    if dist_dt.year < 1990 or dist_dt.year > 2050:
        return False
    if dist_dt > today_ts:
        return False
    if pay_date is None:
        return True
    pay_dt = pd.to_datetime(pay_date, errors="coerce")
    if pd.isna(pay_dt):
        return False
    if pay_dt > today_ts + pd.Timedelta(days=30):
        return False
    if pay_dt < dist_dt - pd.Timedelta(days=30):
        return False
    return True


def validate_dates_series(
    dist_dates: pd.Series,
    pay_dates: pd.Series | None = None,
    *,
    today: date | None = None,
) -> pd.Series:
    """Vectorized date validation with boolean output."""
    dist_dt = pd.to_datetime(dist_dates, errors="coerce")
    if pay_dates is None:
        pay_dt = dist_dt
    else:
        pay_dt = pd.to_datetime(pay_dates, errors="coerce")

    today_value = today or date.today()
    today_ts = pd.Timestamp(today_value)

    valid = dist_dt.notna()
    valid &= dist_dt.dt.year.between(1990, 2050)
    valid &= dist_dt <= today_ts

    valid &= pay_dt.notna()
    valid &= pay_dt <= today_ts + pd.Timedelta(days=30)
    valid &= pay_dt >= dist_dt - pd.Timedelta(days=30)

    return valid.astype("boolean")


def validate_1099r_code(code: Any) -> bool:
    """Validate 1099-R distribution code per docs/data_dictionary.md §3.6."""
    if pd.isna(code):
        return False
    code_clean = str(code).strip().upper()
    if not code_clean:
        return False
    return code_clean in VALID_1099R_CODES


def validate_1099r_code_series(series: pd.Series) -> pd.Series:
    """Vectorized 1099-R code validation with boolean output."""
    code_clean = series.astype("string").str.strip().str.upper()
    return code_clean.isin(VALID_1099R_CODES).astype("boolean")


def cross_validate(
    gross: Any,
    taxable: Any,
    code: Any,
    *,
    age: Any | None = None,
) -> list[str]:
    """Validate logical relationships between fields per §3.6."""
    issues: list[str] = []
    if pd.isna(gross) or pd.isna(taxable):
        return issues
    code_clean = pd.NA if pd.isna(code) else str(code).strip().upper()

    if code_clean == "G" and taxable > gross * 0.1:
        issues.append("cross_code_g_taxable_over_10pct")
    if taxable > gross * 1.5:
        issues.append("cross_taxable_exceeds_gross_150pct")
    if code_clean == "1" and age is not None and not pd.isna(age):
        if age >= 59.5:
            issues.append("cross_code1_age_over_59_5")
    return issues


def cross_validate_series(
    gross: pd.Series,
    taxable: pd.Series,
    code: pd.Series,
    *,
    age: pd.Series | None = None,
) -> pd.Series:
    """Vectorized cross-field validation returning issue lists per row."""
    gross_series = pd.to_numeric(gross, errors="coerce")
    taxable_series = pd.to_numeric(taxable, errors="coerce")
    code_clean = code.astype("string").str.strip().str.upper()
    age_series = pd.to_numeric(age, errors="coerce") if age is not None else None

    issues = pd.Series([[] for _ in range(len(gross_series))], index=gross_series.index)

    has_amounts = gross_series.notna() & taxable_series.notna()
    mask_code_g = has_amounts & code_clean.eq("G") & (taxable_series > (gross_series * 0.1))
    mask_taxable_big = has_amounts & (taxable_series > (gross_series * 1.5))

    for idx in mask_code_g[mask_code_g].index:
        issues.at[idx].append("cross_code_g_taxable_over_10pct")
    for idx in mask_taxable_big[mask_taxable_big].index:
        issues.at[idx].append("cross_taxable_exceeds_gross_150pct")

    if age_series is not None:
        mask_code1_age = code_clean.eq("1") & age_series.notna() & (age_series >= 59.5)
        for idx in mask_code1_age[mask_code1_age].index:
            issues.at[idx].append("cross_code1_age_over_59_5")

    return issues


def build_validation_issues(
    ssn_valid: pd.Series,
    amount_valid: pd.Series,
    date_valid: pd.Series,
    code_1099r_valid: pd.Series,
    *,
    cross_field_issues: pd.Series | None = None,
) -> pd.Series:
    """Build per-row validation issue lists from boolean flags."""
    issues = pd.Series([[] for _ in range(len(ssn_valid))], index=ssn_valid.index)

    invalid_ssn = ssn_valid.eq(False).fillna(False)
    invalid_amount = amount_valid.eq(False).fillna(False)
    invalid_date = date_valid.eq(False).fillna(False)
    invalid_code = code_1099r_valid.eq(False).fillna(False)

    for idx in invalid_ssn[invalid_ssn].index:
        issues.at[idx].append("ssn_invalid")
    for idx in invalid_amount[invalid_amount].index:
        issues.at[idx].append("amount_invalid")
    for idx in invalid_date[invalid_date].index:
        issues.at[idx].append("date_invalid")
    for idx in invalid_code[invalid_code].index:
        issues.at[idx].append("code_1099r_invalid")

    if cross_field_issues is not None:
        for idx, row_issues in cross_field_issues.items():
            if row_issues:
                issues.at[idx].extend(row_issues)

    return issues

# Docstring for src/normalizers module
"""
normalizers.py

Shared normalization helpers for canonical data cleaning across engines.

Includes age attainment and Roth plan detection utilities used by analysis
engines to keep business logic consistent across modules.

Design goals
------------
- Single source of truth for SSN, plan_id, date, numeric, tax code, and
  age-attainment handling.
- Preserve canonical dtypes: pandas string for text, datetime.date for dates,
  and pandas nullable integers where appropriate.
- Keep behavior consistent with existing cleaners to avoid downstream
  regressions.

Public API
----------
- normalize_ssn(value) -> str | pd.NA
- normalize_ssn_series(series) -> pd.Series
- normalize_plan_id_series(series, string_dtype=True) -> pd.Series
- to_date_series(series, errors="coerce", format=None, dayfirst=None) -> pd.Series
- year_from_date_series(date_series) -> pd.Series
- attained_age_by_year_end(dob_series, year_series, years, months=0) -> pd.Series
- to_numeric_series(series) -> pd.Series
- to_int64_nullable_series(series) -> pd.Series
- normalize_text_series(series, strip=True, upper=False) -> pd.Series
- normalize_state_series(series) -> pd.Series
- normalize_tax_code_series(series) -> pd.Series
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

Internal helpers
----------------
Underscore-prefixed helpers support analysis engines and are intentionally not
part of the public API.
"""

from __future__ import annotations       # Tells Python to store type hints as strings internally. You can use newer type syntax ('str | pd.NA')

from datetime import date
from numbers import Integral, Real       # Integral -> integer-like numeric types(int, numpy.int64)
                                         # Real -> real_valued numbers (floats and ints)
                                         # Use them to detect whether a value is an integer or float in a generic way

from typing import Any                   # Type hint meaning "this can be anything"

import pandas as pd
import re                                # Python's built-in regular expression module

from .config import RothTaxableConfig

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


def normalize_ssn(value: Any) -> str | pd.NA:                        # value can be anything(string, int, float, NaN, etc.)                             
    """Normalize SSN to a 9-digit string; return <NA> for
       invalid/unsafe inputs.
    """
    if pd.isna(value):                                               # Checks is value is missing(NaN, None, etc.), if yes we standardize to pd.NA
        return pd.NA

    if isinstance(value, Integral) and not isinstance(value, bool):  # isinstance() compares a value with the dtype -> True for Int, False for Floats or Strs
        return f"{int(value):09d}"       # 'f' -> :09d: format to 9 digits, zero-padded to the left. e.g.: 123 -> '000000123'

    if isinstance(value, Real) and not isinstance(value, Integral):  # compares if value is a float and not an integer
        if pd.isna(value):
            return pd.NA
        if value.is_integer():           # True if the float is like '123456789.0', false if 123.5, 123.1, etc. 
            return f"{int(value):09d}"   # If it's an integer-ish float -> convert to int, zero-pad to 9 digits
        return pd.NA

    value_str = str(value).strip()       # Convert any other type(string, object, etc.) to str, .strip() removes leading/tradiling whitespace
    
    # '^' start of the string -- '\d+' one or more digits -- '\.0' literally ".0" -- '$' end of the string
    # This matches strings like "123456789.0" -> if its a match strips off the final ".0"
    # Safety net for data that has been exported as strings but came from floats.
    if re.match(r"^\d+\.0$", value_str):
        value_str = value_str[:-2]

    # '\D' means any non-digit character -- replace all non-digits with "" -- e.g. "123-45-6789" -> "123456789"
    digits = re.sub(r"\D", "", value_str)
    if not digits:
        return pd.NA

    if len(digits) < 9:                  # If fewer than 9 digits, pad with leading zeros: "1234567" -> "001234567"
        digits = digits.zfill(9)

    if len(digits) != 9:
        return pd.NA

    return digits


def normalize_ssn_series(series: pd.Series) -> pd.Series:
    """Vectorized SSN normalization with pandas string dtype."""
    return series.map(normalize_ssn).astype("string")             # .map() applies normalize_ssn element-by-element to the Series. Returns normalized Series
                                                                  # .astype("string") converts to pandas' string dtype

def normalize_plan_id_series(series: pd.Series, *, string_dtype: bool = True) -> pd.Series:
    """Strip plan IDs with optional pandas string dtype output.

    Defaults to pandas' string dtype to preserve missing values as <NA>.
    Use string_dtype=False to preserve legacy object/str behavior.
    """
    if string_dtype:
        return series.astype("string").str.strip()                # '.str.strip()' vectorized string operation (in all Series)
    return series.astype(str).str.strip()


def to_date_series(
    series: pd.Series,
    errors: str = "coerce",             # With "coerce" if a value can be parsed as a date becamose NaT (not a time)
    format: str | None = None,
    dayfirst: bool | None = None,
) -> pd.Series:
    """Parse dates to datetime.date while preserving missingness."""
    # pd.to_datetime(..) converts the Series to pandas datetime dtype
    # .dt accessor is for datetime-like Series
    # .date converts each timestamp to a Python 'datetime.date' (no time-of-day) -> extracts the date
    return pd.to_datetime(series, errors=errors, format=format, dayfirst=dayfirst).dt.date


def year_from_date_series(date_series: pd.Series) -> pd.Series:
    """Extract year as pandas nullable Int64 from a datetime/date-like series."""
    dt = pd.to_datetime(date_series, errors="coerce")
    return dt.dt.year.astype("Int64")   # .dt.year extracts the year as an Integer Series. For missing dates (NaT), year will be NaN
                                        # .astype("Int64") converts to pandas' nullable integer dtype. Missing years are <NA>, not NaN floats.


def attained_age_by_year_end(
    dob_series: pd.Series,
    year_series: pd.Series,
    *,
    years: int,
    months: int = 0,
) -> pd.Series:
    """
    Determine if an attained age threshold is met by Dec 31 of the given year.

    Example: for 59.5 threshold, we check whether dob + 59 years + 6 months is
    on/before 12/31 of txn_year.

    Returns False for rows with invalid or missing dates/years.
    """
    dob_dt = pd.to_datetime(dob_series, errors="coerce")
    years_int = pd.to_numeric(year_series, errors="coerce").astype("Int64")
    year_end = pd.to_datetime(years_int.astype("string") + "-12-31", errors="coerce")
    threshold_date = dob_dt + pd.DateOffset(years=years, months=months)
    result = pd.Series(False, index=dob_series.index)
    valid = dob_dt.notna() & year_end.notna()
    result.loc[valid] = threshold_date[valid] <= year_end[valid]
    return result

def to_numeric_series(series: pd.Series) -> pd.Series:
    """Coerce values to numeric, returning floats with NaN for invalid entries."""
    # Tries to convert each value to a number. If converions fails (e.g., "abc"), result for that position is NaN (float)
    # Returns a float Series by default
    return pd.to_numeric(series, errors="coerce")


def to_int64_nullable_series(series: pd.Series) -> pd.Series:
    """Coerce to pandas nullable integer (Int64) with NA preservation."""
    # .astype("Int64") converts it to pandas nullable integer dtype. NaN becomes <NA>
    # Valid values become integers from floats
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def normalize_text_series(
    series: pd.Series,
    *,                          # '*' makes strip and upper keyword-only arguments. You must call func(series, strip=True, upper=True)
    strip: bool = True,
    upper: bool = False,
) -> pd.Series:
    """Normalize text to pandas string dtype with optional strip/upper."""
    s = series.astype("string") # -> convert to pandas string dtype
    if strip:
        s = s.str.strip()       # '.str' vectorize to the whole Series
    if upper:
        s = s.str.upper()
    return s


def normalize_state_series(series: pd.Series) -> pd.Series:
    """State abbreviations: strip + uppercase to pandas string dtype."""
    return normalize_text_series(series, strip=True, upper=True)


def normalize_tax_code_series(series: pd.Series) -> pd.Series:
    """
    Extract leading 1–2 alphanumeric tax code characters and uppercase them.

    Examples:
        '7 - Normal Distributions' -> '7'
        'G - Rollover' -> 'G'
        '11 - Loan' -> '11'
    """
    s = series.astype("string")

    # '.str.extract(pattern, expand=False) applies a regex to each string in the Series
    # Returns a Series (because expand=False) containing the first capturing group
    #
    # '^' initiate the matching on the start of the string
    # '\s*' zero or more whitespace characteres(spaces, tabs, new lines, etc.)
    # ([A-Za-z0-9]{1,2}) -> capturing group
    # [A-Za-z0-9] captures letters (upper/lower) or digits
    # {1,2} means "repeat 1 or 2 times"
    # The Group captures the first 1-2 alphanumeric character after any leading spaces
    codes = s.str.extract(r"^\s*([A-Za-z0-9]{1,2})", expand=False)
    codes = codes.str.upper()     # '.str' vectorize to the whole Series
    return codes.astype("string") # .astype("string") convert to pandas string dtype(with <NA> for missing)


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


def _to_datetime(series: pd.Series) -> pd.Series:
    """Coerce values to pandas datetime with NaT for invalid entries.

    Use when downstream logic depends on pandas datetime accessors (dt.*).
    """
    return pd.to_datetime(series, errors="coerce")


def _compute_age_years(dob: pd.Series, asof: pd.Series) -> pd.Series:
    """Compute year-based age using the year component of datetime series.

    Returns a Float64 series with missing values preserved.
    """
    dob_year = dob.dt.year
    asof_year = asof.dt.year
    return (asof_year - dob_year).astype("Float64")


def _compute_start_year(df: pd.DataFrame) -> pd.Series:
    """Choose the first non-null Roth start year across year columns."""
    first_year = pd.to_numeric(df["first_roth_tax_year"], errors="coerce")
    initial_year = pd.to_numeric(df["roth_initial_contribution_year"], errors="coerce")
    first_year = first_year.where(first_year.round().eq(first_year))
    initial_year = initial_year.where(initial_year.round().eq(initial_year))
    combined = first_year.combine_first(initial_year)
    return combined.astype("Int64")


def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
    """Append a reason token to per-row reason lists for rows where mask is True.

    Expects df["correction_reasons"] to be list-like per row and avoids duplicates.
    """
    idx = mask[mask].index
    for i in idx:
        if reason not in df.at[i, "correction_reasons"]:
            df.at[i, "correction_reasons"].append(reason)


def _append_action(df: pd.DataFrame, mask: pd.Series, action: str) -> None:
    """Append an action token to per-row action lists for rows where mask is True.

    Expects df["actions"] to be list-like per row and avoids duplicates.
    """
    idx = mask[mask].index
    for i in idx:
        if action not in df.at[i, "actions"]:
            df.at[i, "actions"].append(action)


def _is_roth_plan(
    series: pd.Series,
    cfg: RothTaxableConfig,
    *,
    case_insensitive: bool = False,
    strip: bool = True,
) -> pd.Series:
    """Return a Roth plan mask using configured prefixes/suffixes.

    Use case_insensitive=True to match normalized, uppercased plan IDs.
    """
    normalized = series.astype("string")
    if strip:
        normalized = normalized.str.strip()
    prefixes = cfg.roth_plan_prefixes
    suffixes = cfg.roth_plan_suffixes
    if case_insensitive:
        normalized = normalized.str.upper()
        prefixes = tuple(prefix.upper() for prefix in prefixes)
        suffixes = tuple(suffix.upper() for suffix in suffixes)
    filled = normalized.fillna("")
    prefix_match = pd.Series(False, index=filled.index)
    suffix_match = pd.Series(False, index=filled.index)
    if prefixes:
        prefix_match = filled.str.startswith(prefixes)
    if suffixes:
        suffix_match = filled.str.endswith(suffixes)
    return prefix_match | suffix_match

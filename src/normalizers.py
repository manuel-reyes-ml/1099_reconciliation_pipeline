# Docstring for src/normalizers module
"""
normalizers.py

Shared normalization helpers for canonical data cleaning across engines.

Design goals
------------
- Single source of truth for SSN, plan_id, date, numeric, and tax code handling.
- Preserve canonical dtypes: pandas string for text, datetime.date for dates,
  and pandas nullable integers where appropriate.
- Keep behavior consistent with existing cleaners to avoid downstream regressions.

Public API
----------
- normalize_ssn(value) -> str | pd.NA
- normalize_ssn_series(series) -> pd.Series
- normalize_plan_id_series(series) -> pd.Series
- to_date_series(series, errors="coerce", format=None, dayfirst=None) -> pd.Series
- year_from_date_series(date_series) -> pd.Series
- to_numeric_series(series) -> pd.Series
- to_int64_nullable_series(series) -> pd.Series
- normalize_text_series(series, strip=True, upper=False) -> pd.Series
- normalize_state_series(series) -> pd.Series
- normalize_tax_code_series(series) -> pd.Series
"""

from __future__ import annotations       # Tells Python to store type hints as strings internally. You can use newer type syntax ('str | pd.NA')

from numbers import Integral, Real       # Integral -> integer-like numeric types(int, numpy.int64)
                                         # Real -> real_valued numbers (floats and ints)
                                         # Use them to detect whether a value is an integer or float in a generic way

from typing import Any                   # Type hint meaning "this can be anything"

import pandas as pd
import re                                # Python's built-in regular expression module


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

def normalize_plan_id_series(series: pd.Series) -> pd.Series:
    """Strip plan IDs and return pandas string dtype."""
    return series.astype("string").str.strip()                    # '.str.strip()' vectorized string operation (in all Series) 


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

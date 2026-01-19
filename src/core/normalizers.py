# Docstring for src/normalizers module
"""
normalizers.py

Shared normalization helpers for canonical data cleaning across engines.

Includes age attainment and Roth plan detection utilities used by analysis
engines to keep business logic consistent across modules. Validation helpers
live in `core.validators`.
Correction export helpers normalize action tokens and split rows into action
groups for multi-tab outputs.

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
- apply_date_filter(df, date_col, date_filter=None) -> pd.DataFrame
- split_corrections_by_action(corrections_df) -> dict[str, pd.DataFrame]

Internal helpers
----------------
Underscore-prefixed helpers support analysis engines and are intentionally not
part of the public API.
"""

from __future__ import annotations       # Tells Python to store type hints as strings internally. You can use newer type syntax ('str | pd.NA')

from numbers import Integral, Real       # Integral -> integer-like numeric types(int, numpy.int64)
                                         # Real -> real_valued numbers (floats and ints)
                                         # Use them to detect whether a value is an integer or float in a generic way

from typing import Any                   # Type hint meaning "this can be anything"

import pandas as pd
import re                                # Python's built-in regular expression module

from ..config import DateFilterConfig, RothTaxableConfig
from .validators import normalize_date_filter_config


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


def apply_date_filter(
    df: pd.DataFrame,
    date_col: str,
    date_filter: DateFilterConfig | None = None,
) -> pd.DataFrame:
    """Filter rows by an inclusive date range and/or month set."""
    date_start, date_end, months = normalize_date_filter_config(date_filter)
    if date_start is None and date_end is None and months is None:
        return df
    if date_col not in df.columns:
        raise ValueError(f"Expected date column {date_col!r} for filtering.")
    dt = pd.to_datetime(df[date_col], errors="coerce")
    # Normalize to date to avoid time-of-day exclusions and tz-aware comparisons.
    date_values = dt.dt.date
    mask = date_values.notna()
    if date_start is not None:
        mask &= date_values >= date_start
    if date_end is not None:
        mask &= date_values <= date_end
    if months is not None:
        months_series = pd.to_datetime(date_values, errors="coerce").dt.month
        mask &= months_series.isin(months)
    return df.loc[mask].copy()


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


def _normalize_action_tokens(action_val: object) -> list[str]:
    if pd.isna(action_val):
        return []
    parts = str(action_val).splitlines()
    return [part.strip().upper() for part in parts if part.strip()]


def split_corrections_by_action(corrections_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Split corrections into separate DataFrames for UPDATE_1099 and INVESTIGATE actions.

    Rows with both actions are duplicated into both outputs.
    """
    if corrections_df.empty:
        empty = corrections_df.iloc[:0].copy()
        return {"Correction": empty.copy(), "Investigate": empty.copy()}

    action_col = "Action" if "Action" in corrections_df.columns else "action"
    if action_col not in corrections_df.columns:
        empty = corrections_df.iloc[:0].copy()
        return {"Correction": empty.copy(), "Investigate": empty.copy()}

    action_tokens = corrections_df[action_col].apply(_normalize_action_tokens)
    mask_update = action_tokens.apply(lambda tokens: "UPDATE_1099" in tokens)
    mask_investigate = action_tokens.apply(lambda tokens: "INVESTIGATE" in tokens)

    correction_df = corrections_df.loc[mask_update].copy()
    investigate_df = corrections_df.loc[mask_investigate].copy()

    if not correction_df.empty:
        correction_df.loc[:, action_col] = "UPDATE_1099"
    if not investigate_df.empty:
        investigate_df.loc[:, action_col] = "INVESTIGATE"

    return {
        "Correction": correction_df,
        "Investigate": investigate_df,
    }


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

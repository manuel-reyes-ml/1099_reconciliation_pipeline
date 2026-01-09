import pytest

from src.config import DateFilterConfig
from src.core.validators import normalize_date_filter_config


def test_date_filter_defaults_to_all() -> None:
    date_start, date_end, months = normalize_date_filter_config(DateFilterConfig())

    assert date_start is None
    assert date_end is None
    assert months is None


def test_date_filter_month_names_and_numbers() -> None:
    date_start, date_end, months = normalize_date_filter_config(
        DateFilterConfig(months=["July", 9, "Dec"])
    )

    assert date_start is None
    assert date_end is None
    assert months == (7, 9, 12)


def test_date_filter_empty_months_treated_as_all() -> None:
    date_start, date_end, months = normalize_date_filter_config(
        DateFilterConfig(months=[])
    )

    assert date_start is None
    assert date_end is None
    assert months is None


def test_date_filter_invalid_month_raises() -> None:
    with pytest.raises(ValueError, match="Invalid month value"):
        normalize_date_filter_config(DateFilterConfig(months=["NotAMonth"]))


def test_date_filter_invalid_range_raises() -> None:
    with pytest.raises(ValueError, match="Invalid date range"):
        normalize_date_filter_config(
            DateFilterConfig(date_start="2025-02-01", date_end="2025-01-01")
        )


def test_date_filter_all_conflict_raises() -> None:
    with pytest.raises(ValueError, match="Months cannot include 'all'"):
        normalize_date_filter_config(DateFilterConfig(months=["all", "July"]))


def test_date_filter_all_only_list_treated_as_all() -> None:
    date_start, date_end, months = normalize_date_filter_config(
        DateFilterConfig(months=["all"])
    )

    assert date_start is None
    assert date_end is None
    assert months is None

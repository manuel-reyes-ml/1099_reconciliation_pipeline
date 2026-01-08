import pandas as pd

from src.config import ROTH_TAXABLE_CONFIG
from src.core.normalizers import (
    _append_action,
    _append_reason,
    _compute_start_year,
    _is_roth_plan,
)


def test_is_roth_plan_prefix_suffix_case_whitespace() -> None:
    cfg = ROTH_TAXABLE_CONFIG
    series = pd.Series(
        [
            " 300005ABC",
            "11111r",
            "300005r ",
            " 300005 ",
            "not_roth",
            None,
        ],
        dtype="string",
    )

    mask = _is_roth_plan(series, cfg, case_insensitive=True, strip=True)

    assert mask.tolist() == [True, True, True, True, False, False]


def test_append_reason_suppresses_duplicates_and_respects_mask() -> None:
    df = pd.DataFrame(
        {
            "correction_reasons": [["missing_year"], []],
        }
    )
    mask = pd.Series([True, False])

    _append_reason(df, mask, "missing_year")
    _append_reason(df, mask, "needs_review")

    assert df.at[0, "correction_reasons"] == ["missing_year", "needs_review"]
    assert df.at[1, "correction_reasons"] == []


def test_append_action_suppresses_duplicates_and_respects_mask() -> None:
    df = pd.DataFrame(
        {
            "actions": [["UPDATE_1099"], []],
        }
    )
    mask = pd.Series([True, False])

    _append_action(df, mask, "UPDATE_1099")
    _append_action(df, mask, "INVESTIGATE")

    assert df.at[0, "actions"] == ["UPDATE_1099", "INVESTIGATE"]
    assert df.at[1, "actions"] == []


def test_compute_start_year_prefers_first_and_keeps_missing() -> None:
    df = pd.DataFrame(
        {
            "first_roth_tax_year": [2020, pd.NA, "bad", 2019, pd.NA],
            "roth_initial_contribution_year": [2018, 2017, 2016, pd.NA, pd.NA],
        }
    )

    start_year = _compute_start_year(df)

    expected = pd.Series([2020, 2017, 2016, 2019, pd.NA], dtype="Int64")
    expected.name = "first_roth_tax_year"

    pd.testing.assert_series_equal(start_year, expected)

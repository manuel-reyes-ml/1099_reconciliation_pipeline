import pandas as pd

from src.config import DateFilterConfig
from src.core.normalizers import apply_date_filter


def test_apply_date_filter_inclusive_end_date_with_time() -> None:
    df = pd.DataFrame(
        {
            "txn_date": [
                pd.Timestamp("2025-01-31 15:00:00"),
                pd.Timestamp("2025-02-01 00:00:00"),
            ]
        }
    )

    date_filter = DateFilterConfig(date_start="2025-01-01", date_end="2025-01-31")
    result = apply_date_filter(df, "txn_date", date_filter=date_filter)

    assert result.shape[0] == 1
    assert result.iloc[0]["txn_date"] == pd.Timestamp("2025-01-31 15:00:00")


def test_apply_date_filter_tz_aware_inclusive_end_date() -> None:
    df = pd.DataFrame(
        {
            "txn_date": [
                pd.Timestamp("2025-01-31 15:00:00", tz="UTC"),
                pd.Timestamp("2025-02-01 00:00:00", tz="UTC"),
            ]
        }
    )

    date_filter = DateFilterConfig(date_start="2025-01-01", date_end="2025-01-31")
    result = apply_date_filter(df, "txn_date", date_filter=date_filter)

    assert result.shape[0] == 1
    assert result.iloc[0]["txn_date"] == pd.Timestamp("2025-01-31 15:00:00", tz="UTC")

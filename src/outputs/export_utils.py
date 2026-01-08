# Docstring for src/export_utils module
"""
export_utils.py

Utilities for exporting pandas DataFrames to Excel for ad-hoc analysis.

Design goals
------------
- Low friction: simple entrypoints for single-sheet and multi-sheet exports.
- Safe output: ensure parent directories exist before writing files.
- Consistent engine: always use the openpyxl engine for .xlsx output.
- Notebook-friendly: timestamped filenames for quick iteration.

Public API
----------
- write_df_excel(df, output_path=None, *, out_dir="reports/exports",
  filename_prefix="export", sheet_name="data", index=False) -> Path
- write_multi_sheet_excel(sheets, output_path, *, index=False) -> Path
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from ..config import REPORTS_DIR, get_engine_outputs_dir


EXCEL_SHEETNAME_LIMIT = 31


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _timestamped_filename(prefix: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}.xlsx"


def _truncate_sheet_name(name: str) -> str:
    return name[:EXCEL_SHEETNAME_LIMIT] if len(name) > EXCEL_SHEETNAME_LIMIT else name


def _dedupe_sheet_names(names: list[str]) -> list[str]:
    """Ensure sheet names are unique after truncation by appending numeric suffixes."""
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for raw_name in names:
        base = _truncate_sheet_name(raw_name)
        if base not in seen:
            seen[base] = 0
            deduped.append(base)
            continue
        seen[base] += 1
        suffix = f"_{seen[base]}"
        trimmed_base = base[: EXCEL_SHEETNAME_LIMIT - len(suffix)]
        deduped.append(f"{trimmed_base}{suffix}")
    return deduped


def write_df_excel(
    df: pd.DataFrame,
    output_path: Path | str | None = None,
    *,
    out_dir: Path | str = REPORTS_DIR / "outputs",
    engine: str | None = None,
    filename_prefix: str = "export",
    sheet_name: str = "data",
    index: bool = False,
) -> Path:
    """
    Write a DataFrame to a single-sheet Excel file and return the output path.

    If output_path is None, a timestamped file is created under out_dir (or the
    engine-specific outputs directory when engine is provided) with the prefix
    filename_prefix.
    """
    if output_path is None:
        out_dir_path = get_engine_outputs_dir(engine) if engine is not None else Path(out_dir)
        output_path = out_dir_path / _timestamped_filename(filename_prefix)
    path = Path(output_path)
    _ensure_parent_dir(path)
    df.to_excel(path, engine="openpyxl", sheet_name=sheet_name, index=index)
    return path


def write_multi_sheet_excel(
    sheets: dict[str, pd.DataFrame],
    output_path: Path | str,
    *,
    index: bool = False,
) -> Path:
    """
    Write multiple DataFrames to a single Excel workbook and return the path.

    Each dict key becomes a sheet name (truncated to Excel's 31-character limit).
    """
    path = Path(output_path)
    _ensure_parent_dir(path)
    sheet_names = _dedupe_sheet_names(list(sheets.keys()))
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, sheet_name in zip(sheets.keys(), sheet_names):
            sheets[name].to_excel(writer, sheet_name=sheet_name, index=index)
    return path

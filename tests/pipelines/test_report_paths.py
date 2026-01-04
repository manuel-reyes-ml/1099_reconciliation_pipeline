from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src import config
from src import export_utils
from src import build_correction_file as bcf


def _patch_report_dirs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    reports_dir = tmp_path / "reports"
    samples_dir = reports_dir / "samples"
    outputs_dir = reports_dir / "outputs"
    figures_dir = reports_dir / "figures"

    monkeypatch.setattr(config, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(config, "REPORTS_SAMPLES_DIR", samples_dir)
    monkeypatch.setattr(config, "REPORTS_OUTPUTS_DIR", outputs_dir)
    monkeypatch.setattr(config, "REPORTS_FIGURES_DIR", figures_dir)

    monkeypatch.setattr(bcf, "REPORTS_SAMPLES_DIR", samples_dir)
    monkeypatch.setattr(bcf, "REPORTS_OUTPUTS_DIR", outputs_dir)

    return samples_dir, outputs_dir


def test_write_correction_file_routes_to_engine_samples(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    samples_dir, _ = _patch_report_dirs(monkeypatch, tmp_path)
    monkeypatch.setattr(bcf, "USE_SAMPLE_DATA_DEFAULT", True)
    monkeypatch.setattr(config, "USE_SAMPLE_DATA_DEFAULT", True)

    df = pd.DataFrame({"sample": [1]})

    path = bcf.write_correction_file(df, output_path=None, engine="match_planid")

    expected_dir = samples_dir / "match_planid"
    assert path.parent == expected_dir
    assert path.exists() is True


def test_write_correction_file_routes_to_engine_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _, outputs_dir = _patch_report_dirs(monkeypatch, tmp_path)
    monkeypatch.setattr(bcf, "USE_SAMPLE_DATA_DEFAULT", False)
    monkeypatch.setattr(config, "USE_SAMPLE_DATA_DEFAULT", False)

    df = pd.DataFrame({"sample": [1]})

    path = bcf.write_correction_file(df, output_path=None, engine="match_planid")

    expected_dir = outputs_dir / "match_planid"
    assert path.parent == expected_dir
    assert path.exists() is True


def test_write_df_excel_engine_creates_outputs_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _, outputs_dir = _patch_report_dirs(monkeypatch, tmp_path)

    df = pd.DataFrame({"a": [1]})

    path = export_utils.write_df_excel(df, output_path=None, engine="age_taxcode")

    expected_dir = outputs_dir / "age_taxcode"
    assert path.parent == expected_dir
    assert expected_dir.exists() is True
    assert path.exists() is True


def test_invalid_engine_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown engine"):
        config.get_engine_outputs_dir("unknown_engine")

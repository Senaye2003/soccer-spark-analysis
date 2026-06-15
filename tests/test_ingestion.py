"""Tests for src/ingestion.py that don't require a live Spark cluster."""

import importlib.util
import pathlib

# Load src/ingestion.py as a module without needing it to be a package.
_SPEC_PATH = pathlib.Path(__file__).resolve().parents[1] / "src" / "ingestion.py"
_spec = importlib.util.spec_from_file_location("ingestion", _SPEC_PATH)
ingestion = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ingestion)


def test_default_args():
    args = ingestion.parse_args([])
    assert args.competition_id == 11
    assert args.season_id == 90
    assert args.fmt == "parquet"


def test_custom_args():
    args = ingestion.parse_args(
        ["--competition-id", "2", "--season-id", "44", "--format", "delta"]
    )
    assert args.competition_id == 2
    assert args.season_id == 44
    assert args.fmt == "delta"


def test_event_columns_present():
    # The working column set the Spark frame is built from.
    for col in ["id", "type", "team", "player", "possession_team"]:
        assert col in ingestion.EVENT_COLUMNS

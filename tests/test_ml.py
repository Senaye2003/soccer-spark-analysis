"""Unit tests for src/ml_pipeline.py feature engineering (no model training)."""

import importlib.util
import pathlib

import pytest
from pyspark.sql import SparkSession

_SPEC_PATH = pathlib.Path(__file__).resolve().parents[1] / "src" / "ml_pipeline.py"
_spec = importlib.util.spec_from_file_location("ml_pipeline", _SPEC_PATH)
ml = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ml)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def test_add_label_and_distance(spark):
    rows = [
        ("Shot", "Goal", 0.5, "[120.0, 40.0]"),   # on the goal line, centre
        ("Shot", "Saved", 0.2, "[110.0, 40.0]"),  # 10m straight out
        ("Pass", None, None, "[60.0, 40.0]"),     # not a shot -> filtered out
    ]
    cols = ["type", "shot_outcome", "shot_statsbomb_xg", "location"]
    df = spark.createDataFrame(rows, cols)

    out = ml.add_label_and_distance(df).collect()
    assert len(out) == 2  # only the two shots survive the filter

    by_outcome = {r["shot_outcome"]: r for r in out}
    assert by_outcome["Goal"]["label"] == 1.0
    assert by_outcome["Goal"]["distance_to_goal"] == pytest.approx(0.0, abs=0.01)
    assert by_outcome["Saved"]["label"] == 0.0
    assert by_outcome["Saved"]["distance_to_goal"] == pytest.approx(10.0, abs=0.01)

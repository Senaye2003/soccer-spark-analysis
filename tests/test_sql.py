"""Unit tests for the analytical queries in src/transformations.py.

These build a tiny in-memory Spark DataFrame (no cluster, no StatsBomb calls)
and assert on the computed metrics. Requires pyspark + Java 17.
"""

import importlib.util
import pathlib

import pytest
from pyspark.sql import SparkSession

_SPEC_PATH = pathlib.Path(__file__).resolve().parents[1] / "src" / "transformations.py"
_spec = importlib.util.spec_from_file_location("transformations", _SPEC_PATH)
tf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tf)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def events(spark):
    # type, team, player, shot_outcome, shot_statsbomb_xg, pass_outcome,
    # pass_length, minute
    rows = [
        ("Shot", "A", "p1", "Goal", 0.5, None, None, 10),
        ("Shot", "A", "p1", "Saved", 0.2, None, None, 20),
        ("Shot", "A", "p2", "Off T", 0.1, None, None, 30),
        ("Shot", "B", "p3", "Goal", 0.9, None, None, 40),
        ("Pass", "A", "p1", None, None, None, 10.0, 5),
        ("Pass", "A", "p2", None, None, "Incomplete", 20.0, 6),
        ("Pass", "B", "p3", None, None, None, 30.0, 7),
    ]
    cols = ["type", "team", "player", "shot_outcome", "shot_statsbomb_xg",
            "pass_outcome", "pass_length", "minute"]
    return spark.createDataFrame(rows, cols)


def _as_dict(df, key):
    return {r[key]: r.asDict() for r in df.collect()}


def test_shot_efficiency_by_team(events):
    res = _as_dict(tf.shot_efficiency_by_team(events), "team")
    assert res["A"]["shots"] == 3
    assert res["A"]["goals"] == 1
    assert res["A"]["conversion_pct"] == pytest.approx(33.3, abs=0.1)
    # A scored 1 goal on 0.8 total xG -> +0.2 overperformance.
    assert res["A"]["xg_diff"] == pytest.approx(0.2, abs=0.01)
    assert res["B"]["goals"] == 1


def test_passing_analysis(events):
    res = _as_dict(tf.passing_analysis(events), "team")
    # Team A: 2 passes, 1 completed (the other is "Incomplete").
    assert res["A"]["passes"] == 2
    assert res["A"]["completed"] == 1
    assert res["A"]["completion_pct"] == pytest.approx(50.0, abs=0.1)
    assert res["A"]["avg_pass_length"] == pytest.approx(15.0, abs=0.1)
    # Team B: single completed pass.
    assert res["B"]["completion_pct"] == pytest.approx(100.0, abs=0.1)


def test_team_comparison(events):
    res = _as_dict(tf.team_comparison(events), "team")
    assert res["A"]["passes"] == 2
    assert res["A"]["shots"] == 3
    assert res["A"]["goals"] == 1
    assert res["A"]["events"] == 5


def test_event_trends_by_interval(events):
    res = _as_dict(tf.event_trends_by_interval(events, minutes=15), "interval_start")
    # minutes 5,6,7,10 -> bucket 0 ; 20 -> 15 ; 30 -> 30 ; 40 -> 30
    assert res[0]["events"] == 4
    assert res[0]["shots"] == 1
    assert res[30]["goals"] == 1

"""
Analytical queries over the ingested La Liga 2020/21 event data.

Reads the Parquet written by src/ingestion.py and runs deeper analysis than the
original notebook's simple counts: shot efficiency vs xG, team comparisons,
passing completion, event trends over the match, and a join with match metadata.

Run (after ingestion has written ./output/events and ./output/matches):
    python src/transformations.py --events-path ./output/events \
        --matches-path ./output/matches

Each query is a small, independently testable function that takes and returns a
Spark DataFrame, so the logic can be unit-tested without a cluster of its own
(see tests/test_sql.py).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("transformations")


def get_spark(app_name: str = "soccer-spark-analysis") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def shot_efficiency_by_player(events: DataFrame, min_shots: int = 10) -> DataFrame:
    """Shots, goals, conversion %, total xG and goals-minus-xG per player.

    A positive xg_diff means the player scored more than their shot quality
    predicted (clinical finishing); negative means underperformance.
    """
    shots = events.filter(F.col("type") == "Shot")
    agg = shots.groupBy("player", "team").agg(
        F.count("*").alias("shots"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
        F.round(F.sum("shot_statsbomb_xg"), 2).alias("total_xg"),
    )
    agg = agg.withColumn(
        "conversion_pct", F.round(F.col("goals") / F.col("shots") * 100, 1)
    ).withColumn("xg_diff", F.round(F.col("goals") - F.col("total_xg"), 2))
    return agg.filter(F.col("shots") >= min_shots).orderBy(F.col("goals").desc())


def shot_efficiency_by_team(events: DataFrame) -> DataFrame:
    """Shots, goals, conversion % and total xG aggregated by team."""
    shots = events.filter(F.col("type") == "Shot")
    agg = shots.groupBy("team").agg(
        F.count("*").alias("shots"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
        F.round(F.sum("shot_statsbomb_xg"), 2).alias("total_xg"),
    )
    agg = agg.withColumn(
        "conversion_pct", F.round(F.col("goals") / F.col("shots") * 100, 1)
    ).withColumn("xg_diff", F.round(F.col("goals") - F.col("total_xg"), 2))
    return agg.orderBy(F.col("goals").desc())


def team_comparison(events: DataFrame) -> DataFrame:
    """One row per team with totals for the main event types."""
    return events.groupBy("team").agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("type") == "Pass", 1).otherwise(0)).alias("passes"),
        F.sum(F.when(F.col("type") == "Shot", 1).otherwise(0)).alias("shots"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
        F.sum(F.when(F.col("type") == "Dribble", 1).otherwise(0)).alias("dribbles"),
        F.sum(F.when(F.col("type") == "Pressure", 1).otherwise(0)).alias("pressures"),
    ).orderBy(F.col("events").desc())


def passing_analysis(events: DataFrame) -> DataFrame:
    """Pass volume, completion % and average pass length by team.

    In StatsBomb data a completed pass has a null `pass_outcome`; only
    incomplete/out/offside passes carry an outcome value.
    """
    passes = events.filter(F.col("type") == "Pass")
    agg = passes.groupBy("team").agg(
        F.count("*").alias("passes"),
        F.sum(F.when(F.col("pass_outcome").isNull(), 1).otherwise(0)).alias("completed"),
        F.round(F.avg("pass_length"), 1).alias("avg_pass_length"),
    )
    return agg.withColumn(
        "completion_pct", F.round(F.col("completed") / F.col("passes") * 100, 1)
    ).orderBy(F.col("passes").desc())


def event_trends_by_interval(events: DataFrame, minutes: int = 15) -> DataFrame:
    """Events, shots and goals bucketed into fixed-length match intervals."""
    bucketed = events.withColumn(
        "interval_start", (F.floor(F.col("minute") / minutes) * minutes).cast("int")
    )
    return bucketed.groupBy("interval_start").agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("type") == "Shot", 1).otherwise(0)).alias("shots"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
    ).orderBy("interval_start")


def goals_by_match(events: DataFrame, matches: DataFrame) -> DataFrame:
    """Join shot-event goals with match metadata (date, teams, final score)."""
    goals = (
        events.filter(F.col("shot_outcome") == "Goal")
        .groupBy("match_id")
        .agg(F.count("*").alias("goal_events"))
    )
    joined = matches.join(goals, on="match_id", how="left").fillna(0, ["goal_events"])
    return joined.select(
        "match_date", "home_team", "away_team", "home_score", "away_score",
        "goal_events",
    ).orderBy("match_date")


def run_all(events: DataFrame, matches: DataFrame | None, top_n: int = 10) -> None:
    logger.info("Shot efficiency by player (min 10 shots):")
    shot_efficiency_by_player(events).show(top_n, truncate=False)

    logger.info("Shot efficiency by team:")
    shot_efficiency_by_team(events).show(top_n, truncate=False)

    logger.info("Team comparison:")
    team_comparison(events).show(top_n, truncate=False)

    logger.info("Passing analysis by team:")
    passing_analysis(events).show(top_n, truncate=False)

    logger.info("Event trends by 15-minute interval:")
    event_trends_by_interval(events).show(truncate=False)

    if matches is not None:
        logger.info("Goals by match (joined with metadata):")
        goals_by_match(events, matches).show(top_n, truncate=False)
    else:
        logger.warning("No matches dimension found; skipping match-metadata join.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events-path", default="./output/events",
                   help="Path to the events dataset written by ingestion.")
    p.add_argument("--matches-path", default="./output/matches",
                   help="Path to the match-dimension dataset (optional).")
    p.add_argument("--format", dest="fmt", default="parquet",
                   choices=["parquet", "delta"], help="Input format.")
    p.add_argument("--top-n", type=int, default=10, help="Rows to show per query.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spark = get_spark()
    events = spark.read.format(args.fmt).load(args.events_path)
    logger.info("Loaded %d events from %s", events.count(), args.events_path)

    matches = None
    if os.path.exists(args.matches_path):
        matches = spark.read.format(args.fmt).load(args.matches_path)
        logger.info("Loaded %d matches from %s", matches.count(), args.matches_path)

    run_all(events, matches, top_n=args.top_n)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

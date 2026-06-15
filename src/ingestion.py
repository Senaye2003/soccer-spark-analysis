"""
Data ingestion + EDA for La Liga 2020/21 StatsBomb event data.

Refactored from 01_data_ingestion.ipynb so the pipeline runs as a standalone
script (deployment target) instead of only inside a Databricks notebook.

Run locally:
    python src/ingestion.py --competition-id 11 --season-id 90 \
        --output-path ./output/events --format parquet

Run as a Databricks Job (the cluster injects `spark`; we reuse it via
SparkSession.getOrCreate) and persist to a managed Delta table:
    python src/ingestion.py --output-table main.soccer.la_liga_events --format delta

The notebook relied on `display()` and an ambient `spark`; both only exist in a
notebook session. This script creates/reuses its own SparkSession and writes
processed data to a real sink (Delta table or Parquet), which fixes the
"persist between sessions on serverless" blocker from the Week 3 check-in.

It now also retains shot- and pass-level fields (xG, outcomes, pass length)
and a `match_id`, so the downstream analysis in src/transformations.py can run
shot-efficiency, passing, and match-metadata-join queries.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import warnings

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# statsbombpy warns once per call when no credentials are supplied (open data
# access). That is expected here, so silence the noise rather than print it
# hundreds of times across all matches.
warnings.filterwarnings("ignore", message="credentials were not supplied")

logger = logging.getLogger("data_ingestion")

# Columns kept from the raw StatsBomb event frame (111 columns otherwise),
# grouped by target type so we can build an explicit Spark schema.
STRING_COLS = [
    "id",
    "type",
    "team",
    "player",
    "possession_team",
    "play_pattern",
    "location",
    "shot_outcome",
    "shot_type",
    "pass_outcome",
    "pass_height",
]
LONG_COLS = ["match_id", "index", "minute", "second"]
DOUBLE_COLS = ["shot_statsbomb_xg", "pass_length", "duration"]
EVENT_COLUMNS = STRING_COLS + LONG_COLS + DOUBLE_COLS

# Subset of match metadata persisted as a dimension table for joins.
MATCH_COLUMNS = [
    "match_id",
    "match_date",
    "home_team",
    "away_team",
    "home_score",
    "away_score",
]


def get_spark(app_name: str = "soccer-spark-analysis") -> SparkSession:
    """Reuse the cluster's SparkSession on Databricks, or create one locally."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_matches(competition_id: int, season_id: int) -> pd.DataFrame:
    """Load the match list for a competition/season from StatsBomb open data."""
    from statsbombpy import sb

    matches = sb.matches(competition_id=competition_id, season_id=season_id)
    logger.info("Matches loaded: %d", len(matches))
    return matches


def load_events(match_ids: list[int]) -> pd.DataFrame:
    """Load and concatenate events for every match, tagging each with match_id."""
    from statsbombpy import sb

    frames = []
    for i, match_id in enumerate(match_ids, start=1):
        events = sb.events(match_id=match_id)
        events["match_id"] = match_id
        frames.append(events)
        if i % 10 == 0 or i == len(match_ids):
            logger.info("Loaded events for %d/%d matches", i, len(match_ids))

    events = pd.concat(frames, ignore_index=True)
    logger.info("Total events: %d", len(events))
    return events


def _event_schema() -> StructType:
    fields = [StructField(c, StringType(), True) for c in STRING_COLS]
    fields += [StructField(c, LongType(), True) for c in LONG_COLS]
    fields += [StructField(c, DoubleType(), True) for c in DOUBLE_COLS]
    return StructType(fields)


def _coerce(events_pd: pd.DataFrame, columns: list[str], string_cols: list[str],
            long_cols: list[str], double_cols: list[str]) -> pd.DataFrame:
    """Select the target columns (filling missing ones) and coerce dtypes so the
    pandas -> Spark conversion is deterministic."""
    subset = events_pd.reindex(columns=columns).copy()
    for c in long_cols:
        subset[c] = pd.to_numeric(subset[c], errors="coerce").fillna(0).astype("int64")
    for c in double_cols:
        subset[c] = pd.to_numeric(subset[c], errors="coerce").astype("float64")
    for c in string_cols:
        subset[c] = subset[c].astype(object).where(subset[c].notna(), None)
    return subset


def to_spark_df(spark: SparkSession, events_pd: pd.DataFrame) -> DataFrame:
    """Select the working columns, coerce types, and build a Spark DataFrame."""
    subset = _coerce(events_pd, EVENT_COLUMNS, STRING_COLS, LONG_COLS, DOUBLE_COLS)
    df = spark.createDataFrame(subset, schema=_event_schema())
    logger.info("Spark DataFrame created with %d rows", df.count())
    return df


def matches_to_spark_df(spark: SparkSession, matches_pd: pd.DataFrame) -> DataFrame:
    """Build a small match-dimension Spark DataFrame for downstream joins."""
    string_cols = ["match_date", "home_team", "away_team"]
    long_cols = ["match_id", "home_score", "away_score"]
    subset = _coerce(matches_pd, MATCH_COLUMNS, string_cols, long_cols, [])
    schema = StructType(
        [StructField("match_id", LongType(), True),
         StructField("match_date", StringType(), True),
         StructField("home_team", StringType(), True),
         StructField("away_team", StringType(), True),
         StructField("home_score", LongType(), True),
         StructField("away_score", LongType(), True)]
    )
    # Reorder to schema field order.
    subset = subset[["match_id", "match_date", "home_team", "away_team",
                     "home_score", "away_score"]]
    return spark.createDataFrame(subset, schema=schema)


def event_type_distribution(df: DataFrame) -> DataFrame:
    return df.groupBy("type").count().orderBy(F.col("count").desc())


def most_active_players(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("player").isNotNull())
        .groupBy("player")
        .count()
        .orderBy(F.col("count").desc())
    )


def possession_by_team(df: DataFrame) -> DataFrame:
    return df.groupBy("possession_team").count().orderBy(F.col("count").desc())


def run_eda(df: DataFrame, top_n: int = 10) -> None:
    """Print basic EDA, using show() instead of the notebook's display()."""
    logger.info("Event type distribution:")
    event_type_distribution(df).show(20, truncate=False)

    logger.info("Most active players (top %d):", top_n)
    most_active_players(df).show(top_n, truncate=False)

    logger.info("Possession by team (top %d):", top_n)
    possession_by_team(df).show(top_n, truncate=False)


def persist(df: DataFrame, output_path: str | None, output_table: str | None,
            fmt: str) -> None:
    """Write processed events to a durable sink so they survive across sessions."""
    if output_table:
        df.write.format(fmt).mode("overwrite").saveAsTable(output_table)
        logger.info("Wrote %s table: %s", fmt, output_table)
    elif output_path:
        df.write.format(fmt).mode("overwrite").save(output_path)
        logger.info("Wrote %s to path: %s", fmt, output_path)
    else:
        logger.warning("No --output-path or --output-table given; skipping persist.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--competition-id", type=int, default=11,
                   help="StatsBomb competition id (default 11 = La Liga).")
    p.add_argument("--season-id", type=int, default=90,
                   help="StatsBomb season id (default 90 = 2020/21).")
    p.add_argument("--output-path", default=None,
                   help="Path to write processed events (Parquet/Delta). A sibling "
                        "'matches' dataset is written next to it for joins.")
    p.add_argument("--output-table", default=None,
                   help="Managed table name to write to (e.g. catalog.schema.table).")
    p.add_argument("--format", dest="fmt", default="parquet",
                   choices=["parquet", "delta"], help="Output format.")
    p.add_argument("--top-n", type=int, default=10, help="Rows to show in EDA.")
    p.add_argument("--skip-eda", action="store_true", help="Skip EDA output.")
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
    matches = load_matches(args.competition_id, args.season_id)
    events_pd = load_events(matches["match_id"].tolist())
    df = to_spark_df(spark, events_pd)

    if not args.skip_eda:
        run_eda(df, top_n=args.top_n)

    persist(df, args.output_path, args.output_table, args.fmt)

    # Persist the match dimension next to the events for join-based analysis.
    if args.output_path:
        matches_path = os.path.join(os.path.dirname(args.output_path) or ".",
                                    "matches")
        matches_df = matches_to_spark_df(spark, matches)
        matches_df.write.format(args.fmt).mode("overwrite").save(matches_path)
        logger.info("Wrote %s matches dimension to: %s", args.fmt, matches_path)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

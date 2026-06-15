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
"persist between sessions" blocker from the Week 3 check-in.
"""

from __future__ import annotations

import argparse
import logging
import sys
import warnings

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# statsbombpy warns once per call when no credentials are supplied (open data
# access). That is expected here, so silence the noise rather than print it
# hundreds of times across all matches.
warnings.filterwarnings("ignore", message="credentials were not supplied")

logger = logging.getLogger("data_ingestion")

# Columns we keep from the raw StatsBomb event frame (111 columns otherwise).
EVENT_COLUMNS = [
    "id",
    "index",
    "type",
    "team",
    "player",
    "minute",
    "second",
    "location",
    "possession_team",
    "play_pattern",
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
    """Load and concatenate events for every match into one pandas DataFrame."""
    from statsbombpy import sb

    frames = []
    for i, match_id in enumerate(match_ids, start=1):
        frames.append(sb.events(match_id=match_id))
        if i % 10 == 0 or i == len(match_ids):
            logger.info("Loaded events for %d/%d matches", i, len(match_ids))

    events = pd.concat(frames, ignore_index=True)
    logger.info("Total events: %d", len(events))
    return events


def to_spark_df(spark: SparkSession, events_pd: pd.DataFrame) -> DataFrame:
    """Select the working columns and convert to a Spark DataFrame.

    Values are cast to string to match the original notebook behaviour and to
    avoid mixed-type inference issues from StatsBomb's nested fields.
    """
    subset = events_pd[EVENT_COLUMNS].astype(str)
    df = spark.createDataFrame(subset)
    logger.info("Spark DataFrame created with %d rows", df.count())
    return df


def event_type_distribution(df: DataFrame) -> DataFrame:
    return df.groupBy("type").count().orderBy(F.col("count").desc())


def most_active_players(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("player") != "nan")
        .groupBy("player")
        .count()
        .orderBy(F.col("count").desc())
    )


def possession_by_team(df: DataFrame) -> DataFrame:
    return df.groupBy("possession_team").count().orderBy(F.col("count").desc())


def run_eda(df: DataFrame, top_n: int = 10) -> None:
    """Print the same EDA the notebook produced, using show() instead of display()."""
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
        (df.write.format(fmt).mode("overwrite").saveAsTable(output_table))
        logger.info("Wrote %s table: %s", fmt, output_table)
    elif output_path:
        (df.write.format(fmt).mode("overwrite").save(output_path))
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
                   help="Path to write processed events (Parquet/Delta).")
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
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

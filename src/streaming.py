"""
Spark Structured Streaming job over the match-event data.

StatsBomb open data is static, so we simulate a real-time feed: the ingested
events are written out as a set of JSON files into a landing directory, and the
streaming query reads them one file per micro-batch (maxFilesPerTrigger=1),
maintaining a running per-team aggregation of events / shots / goals.

Run (after ingestion has written ./output/events):
    python src/streaming.py --events-path ./output/events

Uses the availableNow trigger so it processes all simulated batches and then
terminates, which makes it runnable as part of run.sh.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

logger = logging.getLogger("streaming")

STREAM_SCHEMA = StructType([
    StructField("match_id", LongType(), True),
    StructField("team", StringType(), True),
    StructField("type", StringType(), True),
    StructField("shot_outcome", StringType(), True),
    StructField("minute", LongType(), True),
])


def get_spark(app_name: str = "soccer-spark-analysis") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def prepare_stream_input(spark: SparkSession, events_path: str, landing_dir: str,
                         fmt: str = "parquet", num_files: int = 12) -> None:
    """Write the ingested events out as JSON files to simulate an arriving feed."""
    events = spark.read.format(fmt).load(events_path)
    (
        events.select("match_id", "team", "type", "shot_outcome", "minute")
        .repartition(num_files)
        .write.mode("overwrite")
        .json(landing_dir)
    )
    logger.info("Wrote %d simulated stream files to %s", num_files, landing_dir)


def build_aggregation(stream):
    """Running per-team count of events, shots and goals."""
    return stream.groupBy("team").agg(
        F.count("*").alias("events"),
        F.sum(F.when(F.col("type") == "Shot", 1).otherwise(0)).alias("shots"),
        F.sum(F.when(F.col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals"),
    )


def run_stream(spark: SparkSession, landing_dir: str, checkpoint_dir: str) -> None:
    # Start from a clean checkpoint so the demo replays the full feed each run.
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)

    stream = (
        spark.readStream.schema(STREAM_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(landing_dir)
    )
    agg = build_aggregation(stream)
    query = (
        agg.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    logger.info("Streaming query finished.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events-path", default="./output/events",
                   help="Path to the events dataset written by ingestion.")
    p.add_argument("--format", dest="fmt", default="parquet",
                   choices=["parquet", "delta"], help="Input format.")
    p.add_argument("--landing-dir", default="./output/stream_input",
                   help="Directory for the simulated streaming JSON files.")
    p.add_argument("--checkpoint-dir", default="./output/stream_checkpoint",
                   help="Streaming checkpoint location.")
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
    prepare_stream_input(spark, args.events_path, args.landing_dir, args.fmt)
    run_stream(spark, args.landing_dir, args.checkpoint_dir)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

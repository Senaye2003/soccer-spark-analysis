"""
MLlib model: predict whether a shot becomes a goal.

Trains a logistic-regression classifier on the ingested shot events using
StatsBomb expected-goals (xG), distance to goal, and the play pattern / shot
type as features, then reports evaluation metrics (AUC, accuracy, F1).

Run (after ingestion has written ./output/events):
    python src/ml_pipeline.py --events-path ./output/events

Feature engineering is split into a pure transformation (`add_label_and_distance`)
so it can be unit-tested without training a model (see tests/test_ml.py).
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("ml_pipeline")

# StatsBomb pitch is 120x80; the goal centre is at (120, 40).
GOAL_X, GOAL_Y = 120.0, 40.0
LOCATION_RE = r"\[([0-9.]+), ([0-9.]+)\]"


def get_spark(app_name: str = "soccer-spark-analysis") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def add_label_and_distance(events: DataFrame) -> DataFrame:
    """Filter to shots and derive the label + distance-to-goal feature.

    label = 1.0 if the shot was a goal, else 0.0.
    distance_to_goal is parsed from the StatsBomb `location` string "[x, y]".
    """
    shots = events.filter(F.col("type") == "Shot")
    shots = shots.withColumn(
        "label", F.when(F.col("shot_outcome") == "Goal", 1.0).otherwise(0.0)
    )
    shots = shots.withColumn(
        "shot_x", F.regexp_extract(F.col("location"), LOCATION_RE, 1).cast("double")
    ).withColumn(
        "shot_y", F.regexp_extract(F.col("location"), LOCATION_RE, 2).cast("double")
    )
    return shots.withColumn(
        "distance_to_goal",
        F.sqrt(
            F.pow(F.lit(GOAL_X) - F.col("shot_x"), 2)
            + F.pow(F.lit(GOAL_Y) - F.col("shot_y"), 2)
        ),
    )


def build_pipeline() -> Pipeline:
    """Assemble the feature + logistic-regression pipeline."""
    play_idx = StringIndexer(
        inputCol="play_pattern", outputCol="play_pattern_idx", handleInvalid="keep"
    )
    type_idx = StringIndexer(
        inputCol="shot_type", outputCol="shot_type_idx", handleInvalid="keep"
    )
    encoder = OneHotEncoder(
        inputCols=["play_pattern_idx", "shot_type_idx"],
        outputCols=["play_pattern_oh", "shot_type_oh"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=["shot_statsbomb_xg", "distance_to_goal",
                   "play_pattern_oh", "shot_type_oh"],
        outputCol="features",
        handleInvalid="skip",
    )
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
    return Pipeline(stages=[play_idx, type_idx, encoder, assembler, lr])


def train_and_evaluate(shots: DataFrame, seed: int = 42):
    """Split, train, and return (model, metrics dict)."""
    data = (
        shots.select("label", "shot_statsbomb_xg", "distance_to_goal",
                     "play_pattern", "shot_type")
        .na.fill({"shot_statsbomb_xg": 0.0})
        .filter(F.col("distance_to_goal").isNotNull())
    )
    train, test = data.randomSplit([0.8, 0.2], seed=seed)
    model = build_pipeline().fit(train)
    preds = model.transform(test)

    auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(preds)
    accuracy = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy",
    ).evaluate(preds)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1",
    ).evaluate(preds)

    metrics = {
        "train_rows": train.count(),
        "test_rows": test.count(),
        "auc": round(auc, 3),
        "accuracy": round(accuracy, 3),
        "f1": round(f1, 3),
    }
    return model, metrics


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events-path", default="./output/events",
                   help="Path to the events dataset written by ingestion.")
    p.add_argument("--format", dest="fmt", default="parquet",
                   choices=["parquet", "delta"], help="Input format.")
    p.add_argument("--model-output", default=None,
                   help="Optional path to save the fitted pipeline model.")
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
    shots = add_label_and_distance(events)
    logger.info("Shots: %d (goals: %d)",
                shots.count(), shots.filter(F.col("label") == 1.0).count())

    model, metrics = train_and_evaluate(shots)
    logger.info("Shot-to-goal model metrics: %s", metrics)

    if args.model_output:
        model.write().overwrite().save(args.model_output)
        logger.info("Saved model to %s", args.model_output)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

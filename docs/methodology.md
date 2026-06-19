# Methodology

The pipeline is four stages, each a standalone script in `src/`, chained
together by `run.sh`. It integrates all required Spark components: Structured
APIs (DataFrames), Spark SQL-style analysis, Structured Streaming, and MLlib.

```
ingestion.py  ->  transformations.py  ->  streaming.py  ->  ml_pipeline.py
 (load/clean)      (analysis)             (streaming)        (machine learning)
        \              |                      |                    /
         \------------ writes/reads Parquet under ./output ------/
```

## 1. Ingestion — Structured APIs (`src/ingestion.py`)

- Loads La Liga 2020/21 matches and their events via `statsbombpy`.
- Tags each event with `match_id` and concatenates all 35 matches.
- Selects an analysis-ready column subset and converts the pandas frame into a
  Spark DataFrame using an **explicit `StructType` schema** so numeric fields
  (xG, pass length, match clock) and categorical fields are typed correctly.
- Persists events and a match-dimension table to **Parquet** via
  `df.write` (`--output-path`). On Databricks the same code path writes a
  managed **Delta** table, replacing the original notebook's `display()` calls
  and fixing the serverless persistence problem.
- Runs basic EDA (event-type distribution, most active players, possession by
  team) with `show()`.

## 2. Analysis — Structured APIs + Spark SQL (`src/transformations.py`)

Reads the Parquet events and runs DataFrame/SQL-style aggregations and a join:

- **Shot efficiency** by player and team — shots, goals, conversion %, summed
  xG, and goals-minus-xG (finishing over/under-performance).
- **Team comparison** — passes, shots, goals, dribbles, pressures per team.
- **Passing analysis** — volume, completion % (a completed StatsBomb pass has a
  null `pass_outcome`), and average pass length.
- **Event trends** — events/shots/goals bucketed into 15-minute intervals.
- **Goals by match** — a **join** of goal events with the match dimension to
  show date, teams, and final score.

## 3. Streaming — Spark Structured Streaming (`src/streaming.py`)

Because StatsBomb data is static, a live feed is **simulated**:

- The ingested events are written out as a set of JSON files into a landing
  directory.
- A streaming query reads that directory with `maxFilesPerTrigger=1`, so each
  file is processed as its own micro-batch.
- The query maintains a running per-team aggregation of events / shots / goals
  and writes it to the console in `complete` output mode.
- The `availableNow` trigger processes all simulated batches and then stops,
  so the job runs unattended inside `run.sh`.

## 4. MLlib — classification (`src/ml_pipeline.py`)

Predicts whether a shot becomes a goal:

- **Label:** `1.0` if `shot_outcome == "Goal"`, else `0.0`.
- **Features:** `shot_statsbomb_xg`, distance to goal (parsed from the
  `location` string and computed against the goal centre at (120, 40)),
  `play_pattern`, and `shot_type`.
- **Pipeline:** `StringIndexer` + `OneHotEncoder` for the categoricals →
  `VectorAssembler` → `LogisticRegression`.
- **Evaluation:** an 80/20 train/test split; metrics are area under ROC (AUC),
  accuracy, and F1, reported with Spark's `BinaryClassificationEvaluator` and
  `MulticlassClassificationEvaluator`.

## Testing & reproducibility

- Unit tests in `tests/` cover argument parsing, the analytical aggregations
  (on small in-memory DataFrames), and the ML feature engineering — no cluster
  required.
- The full pipeline runs with a single command (`bash run.sh` / `make run`).
- Requires **Java 17** (PySpark 4 does not run on Java 22+); see
  [`reproduction_guide.md`](./reproduction_guide.md).

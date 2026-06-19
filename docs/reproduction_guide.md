# Reproduction Guide

Step-by-step instructions to reproduce the full pipeline and results.

## 1. Prerequisites

- **Python 3.9+**
- **Java 17 or 21** — PySpark 4 will **not** run on Java 22+ (it fails with
  `UnsupportedOperationException: getSubject is not supported`).

Install and select Java 17 on macOS:

```bash
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
java -version   # should report 17.x
```

Add the `export` line to `~/.zshrc` to make it permanent.

## 2. Get the code and install dependencies

```bash
git clone https://github.com/Senaye2003/soccer-spark-analysis.git
cd soccer-spark-analysis
pip3 install -r requirements.txt
```

Dependencies: `pyspark`, `pandas`, `matplotlib`, `pytest`, `statsbombpy`.
The data itself is downloaded at runtime from StatsBomb via `statsbombpy`; no
dataset files need to be committed or downloaded manually.

## 3. Run the full pipeline (one command)

```bash
bash run.sh      # or: make run
```

This runs all four stages in order and produces output under `./output/`:

1. `ingestion.py` → writes `output/events` and `output/matches` (Parquet)
2. `transformations.py` → prints the analytical query results
3. `streaming.py` → prints the streaming micro-batch aggregations
4. `ml_pipeline.py` → prints the shot-to-goal model metrics

## 4. Run stages individually (optional)

```bash
python3 src/ingestion.py --output-path ./output/events
python3 src/transformations.py --events-path ./output/events --matches-path ./output/matches
python3 src/streaming.py --events-path ./output/events
python3 src/ml_pipeline.py --events-path ./output/events
```

Useful flags: `--skip-eda` and `--top-n N` (ingestion), `--format delta`
(ingestion, for Databricks), `--model-output PATH` (ml_pipeline, to save the
model).

## 5. Run the tests

```bash
python3 -m pytest tests/ -v
```

All 7 tests should pass (argument parsing, analytical aggregations, and ML
feature engineering — none require a Spark cluster).

## 6. Expected results

See [`results.md`](./results.md). As a quick check: ingestion reports
**35 matches / 139,030 events**, and the MLlib model reports **AUC ≈ 0.80**.

## Troubleshooting

- `getSubject is not supported` / `JAVA_GATEWAY_EXITED` → wrong Java version;
  set `JAVA_HOME` to Java 17 (step 1).
- `command not found: python` → use `python3` (and `pip3`) on macOS.
- Stale streaming output on re-run → the job clears its checkpoint each run;
  if needed, delete `output/stream_checkpoint` manually.

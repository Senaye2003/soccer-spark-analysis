#!/bin/bash
set -e

# Use python3 if available, otherwise fall back to python.
PY=$(command -v python3 || command -v python)

# Full pipeline: ingestion (Structured APIs) -> analysis (SQL/DataFrame)
# -> streaming -> MLlib model.
"$PY" src/ingestion.py --output-path ./output/events
"$PY" src/transformations.py --events-path ./output/events --matches-path ./output/matches
"$PY" src/streaming.py --events-path ./output/events
"$PY" src/ml_pipeline.py --events-path ./output/events

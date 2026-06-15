#!/bin/bash
set -e

# Use python3 if available, otherwise fall back to python.
PY=$(command -v python3 || command -v python)

# Ingest events + match dimension to ./output, then run the analytical queries.
"$PY" src/ingestion.py --output-path ./output/events
"$PY" src/transformations.py --events-path ./output/events --matches-path ./output/matches
"$PY" src/streaming.py
"$PY" src/ml_pipeline.py

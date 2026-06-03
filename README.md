# ITCS6190 Course Project — Soccer Match Event Analysis

## Problem Statement

Professional soccer generates massive amounts of event-level data per match,
but analyzing it at scale across multiple competitions and seasons is
computationally expensive. This project uses Apache Spark to process and
analyze large-scale soccer match event data to uncover patterns in team
performance, player behavior, and match outcomes.

## Dataset

**StatsBomb Open Data** — https://github.com/statsbomb/open-data

Free, event-level match data covering 3,600+ matches across competitions
including the UEFA Champions League, La Liga, and the Women's World Cup.
Each match contains thousands of granular events (passes, shots, dribbles,
defensive pressures, etc.) stored in JSON format — millions of records total.

## Planned Components

1. **Data Ingestion & Preprocessing** — Load and flatten nested JSON using
   PySpark, build a clean schema for downstream analysis

2. **Shot Quality Analysis** — Aggregate shot events by player/team,
   compute efficiency relative to expected goals (xG)

3. **Passing Network Analysis** — Identify passing patterns and sequences
   that characterize high-performing teams

4. **Defensive Pressure Analysis** — Measure how pressing after turnovers
   correlates with scoring opportunities

5. **Player Role Clustering** — Use Spark MLlib to cluster players into
   role archetypes based on aggregated event statistics

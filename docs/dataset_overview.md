# Dataset Overview

## Source

[**StatsBomb Open Data**](https://github.com/statsbomb/open-data) — free,
event-level football data published by StatsBomb for public research use. The
data is accessed programmatically through the
[`statsbombpy`](https://github.com/statsbomb/statsbombpy) Python client rather
than committed to this repository.

This project uses the **La Liga 2020/21** season
(`competition_id=11`, `season_id=90`).

## Size

| Item | Value |
|------|-------|
| Matches | 35 |
| Total events | 139,030 |
| Shots | 839 |
| Passes | 40,337 |
| Distinct event types | 33 |

In StatsBomb's open data, La Liga coverage is centred on **FC Barcelona's
matches** (every match in this season's open set involves Barcelona). The raw
JSON is on the order of tens of MB across the 35 match files; only the small
processed Parquet outputs are produced locally and are git-ignored.

## Schema

Events are delivered as deeply nested JSON (111+ columns once flattened). The
ingestion step keeps an analysis-ready subset:

**Events** (`output/events`)

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique event id |
| `match_id` | long | Match the event belongs to |
| `index` | long | Event order within the match |
| `type` | string | Event type (Pass, Shot, Carry, Pressure, …) |
| `team` | string | Team performing the event |
| `player` | string | Player performing the event |
| `minute`, `second` | long | Match clock |
| `location` | string | `[x, y]` pitch coordinates |
| `possession_team` | string | Team in possession |
| `play_pattern` | string | How the possession started |
| `shot_outcome` | string | Result of a shot (Goal, Saved, Off T, …) |
| `shot_type` | string | Shot context (Open Play, Penalty, …) |
| `shot_statsbomb_xg` | double | Expected-goals value of the shot |
| `pass_outcome` | string | Null when the pass is completed |
| `pass_height` | string | Ground / Low / High pass |
| `pass_length` | double | Pass distance |
| `duration` | double | Event duration (seconds) |

**Matches dimension** (`output/matches`)

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | long | Primary key |
| `match_date` | string | Date of the match |
| `home_team`, `away_team` | string | Teams |
| `home_score`, `away_score` | long | Final score |

## Licensing

StatsBomb Open Data is provided free for public use for research and football
analytics. Per StatsBomb's terms, any published analysis must **attribute
StatsBomb as the data source**. This is an academic, non-commercial course
project and is not affiliated with or endorsed by StatsBomb.

## Preprocessing

1. Load the match list for La Liga 2020/21, then load events for every match.
2. Tag each event with its `match_id` and concatenate into one frame.
3. Select the analysis-ready columns above and coerce types (numeric xG / pass
   length / clock to numbers; categorical fields to strings) using an explicit
   Spark schema for deterministic conversion.
4. Write events and the match dimension to Parquet under `output/` so later
   stages (analysis, streaming, MLlib) re-read processed data instead of
   re-downloading.

See [`methodology.md`](./methodology.md) for the full pipeline.

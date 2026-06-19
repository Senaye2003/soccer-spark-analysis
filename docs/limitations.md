# Limitations

Known constraints and assumptions in this project:

## Data coverage

- **Barcelona-only.** StatsBomb's open La Liga 2020/21 set contains only
  Barcelona's matches. Every "team comparison" is therefore Barcelona versus a
  set of single-game opponents, not a full-league table. Opponent totals are
  based on one match each and are not representative of those teams' seasons.
- **Single season / competition.** Findings describe La Liga 2020/21 and should
  not be generalised to other leagues or seasons.

## Streaming is simulated

- StatsBomb open data is static, so there is no genuine real-time feed. The
  streaming job replays already-ingested events as JSON files to demonstrate
  Structured Streaming mechanics (micro-batches, running aggregation, triggers).
  It does not measure true latency or ingest from an external live source.

## Modelling caveats

- **xG leakage.** The shot-to-goal model uses StatsBomb's `shot_statsbomb_xg`
  as a feature. xG is itself a model of goal probability, so it is a very strong
  (near-target) predictor; the high AUC partly reflects this rather than novel
  feature discovery.
- **Class imbalance.** Only ~13% of shots are goals, so accuracy is inflated by
  the majority class — AUC and F1 are the more honest metrics.
- **Small positive class.** With 111 goals total, the test set contains a small
  number of positive examples, so metrics carry meaningful variance across
  different random splits.
- **No tuning.** A single logistic-regression model is trained with default-ish
  settings; no hyperparameter search or model comparison was performed.

## Engineering caveats

- **Local mode.** The pipeline is validated in Spark local mode on a single
  machine, not on a distributed cluster.
- **Type coercion.** Some nested StatsBomb fields are simplified (e.g. location
  parsed from a string, several columns dropped) to keep the schema analysis-
  ready; richer 360/freeze-frame data is not used.
- **Java version sensitivity.** PySpark 4 requires Java 17/21 and fails on
  Java 22+, which is an environment constraint for reproduction.

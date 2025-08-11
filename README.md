# Football Analytics Pipeline

A modular Python pipeline that ingests **StatsBomb open-data**, processes it via the Medallion pattern, and outputs **analysis-ready** Parquet tables plus **ML-ready** features.

---

## What you get
- **Bronze**: exact mirror of StatsBomb open-data (traceable, immutable).
- **Silver**: normalized Parquet tables: `events`, `matches`, `lineups`, `three_sixty`.
- **Gold**: feature tables (e.g., shot angle, distance, xG labels) for ML.

---

## Quickstart

```bash
# 1) Create a venv (Mac/Linux)
python3 -m venv .venv
source .venv/bin/activate

# 2) Install in editable mode with useful extras
pip install -e .[dev,ml,viz]

# 3) Print the active config (sanity check)
football-pipeline --config-only

# 4) Ingest Bronze (mirrors StatsBomb layout)
football-pipeline --bronze --source open_data/j1_league

# 5) Build Silver normalized tables
football-pipeline --silver

# 6) (Optional) Build Gold feature tables for ML
football-pipeline --gold
```

---

## Configuration

Project config lives at `src/football_pipeline/config/pipeline_config.yaml`.

Key bits:
```yaml
processing:
  bronze: true
  silver: false
  gold: false
  source: "open_data"   # "open_data", "j1_league", or null for all
logging:
  console_level: "INFO"
  file_level: "DEBUG"
directories:
  data: "data"
  logs: "logs"
```

CLI flags override YAML defaults:
```bash
# run only bronze regardless of YAML
football-pipeline --bronze

# run everything, all sources
football-pipeline --all-layers --source all
```

---

## Outputs

```
data/
  landing/                      # raw (if you use them)
  bronze/
    open_data/
      data/{events,lineups,matches,three-sixty}/...
  silver/
    open_data/
      {events,lineups,matches,three_sixty}/  # normalized Parquet
  gold/
    open_data/
      shots/                    # ML features (example)
logs/
  pipeline.log                  # plus per-run logs
```

> Bronze mirrors StatsBomb’s original `open-data/data/...` layout for traceability.  
> Silver simplifies to one folder per table with stable schemas/dtypes.  
> Gold focuses on ML-ready features (e.g., xG).

---

## Example: load Silver in a notebook

**Pandas**
```python
import pandas as pd
events = pd.read_parquet("data/silver/open_data/events")
matches = pd.read_parquet("data/silver/open_data/matches")
```

**Polars**
```python
import polars as pl
events = pl.scan_parquet("data/silver/open_data/events/*.parquet").collect()
```

---

## 🧭 CLI reference

```bash
football-pipeline --help

# common runs
football-pipeline                  # uses YAML defaults
football-pipeline --bronze         # bronze only
football-pipeline --silver         # silver only
football-pipeline --gold           # gold only
football-pipeline --all-layers     # bronze + silver + gold
football-pipeline --source j1_league
football-pipeline --source all
```

---

## Repo layout (high level)

```
src/football_pipeline/
  cli.py                 # entrypoint (exposes `football-pipeline`)
  pipeline.py            # run_bronze_layer/run_silver_layer/run_gold_layer
  config/
    pipeline_config.yaml
  bronze/ ...            # bronze transforms and schemas
  silver/ ...            # silver transforms and schemas
  gold/ ...              # feature engineering
  utils/ ...             # dirs, logging, constants
```

---

## 📝 License
MIT — see `LICENSE`.

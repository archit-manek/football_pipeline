# Football Analytics Pipeline

A Python pipeline for football data analysis. Ingests StatsBomb open-data and J1 League data through a Bronze-Silver-Gold medallion architecture, outputting clean Parquet files ready for analysis and ML.

## What you get
- **Bronze**: Raw data converted to Parquet format for fast querying
- **Silver**: Cleaned and normalized tables ready for analysis  
- **Gold**: ML-ready feature tables (placeholder for future development)

## Quickstart

```bash
# 1) Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2) Install the package
pip install -e .

# 3) Run bronze layer (default: open_data only)
python main.py

# 4) Or use the CLI for more control
python -m football_pipeline.cli --bronze --source all

# 5) Run bronze + silver layers
python -m football_pipeline.cli --bronze --silver --source open_data
```

## Configuration

The pipeline uses simple defaults:
- **Bronze**: Enabled by default
- **Silver**: Disabled by default  
- **Gold**: Disabled by default
- **Source**: open_data by default

No complex configuration files needed - just use CLI flags to override defaults.

## Data Structure

```
data/
  landing/                      # Raw source data
    open_data/data/            # StatsBomb open data
    j1_league/                 # J1 League data
  bronze/                      # Parquet conversion of raw data
    open_data/data/
      competitions.parquet
      matches/*.parquet
      lineups/*.parquet  
      events/*.parquet
      three-sixty/*.parquet
    j1_league/
      matches/sb_matches.parquet
      events/sb_events.parquet
      physical/hudl_physical.parquet
      mappings/*.parquet
  silver/                      # Cleaned, normalized data
    open_data/data/
      competitions.parquet     # Ready for analysis
  gold/                        # ML-ready features (future)
logs/                          # Pipeline execution logs
```

## Loading Data in Notebooks

**Best practice - using path helpers:**
```python
from football_pipeline.utils.constants import get_data_path
import polars as pl

# Read competitions from bronze (clean, no hardcoded paths)
competitions_path = get_data_path("bronze", "open_data", "competitions")
competitions = pl.read_parquet(competitions_path)

# Read all matches from bronze
matches_dir = get_data_path("bronze", "open_data", "matches")
matches = pl.scan_parquet(matches_dir / "*.parquet").collect()

# Read J1 League data
j1_matches_path = get_data_path("bronze", "j1_league", "matches")
j1_matches = pl.read_parquet(j1_matches_path)
```

**Using pandas:**
```python
import pandas as pd
from football_pipeline.utils.constants import get_data_path

competitions = pd.read_parquet(get_data_path("bronze", "open_data", "competitions"))
```

**For silver layer:**
```python
# Read processed data from silver layer
silver_competitions = pl.read_parquet(get_data_path("silver", "open_data", "competitions"))
```

## CLI Usage

```bash
# Run bronze layer for open_data (default)
python -m football_pipeline.cli --bronze

# Run bronze for both sources
python -m football_pipeline.cli --bronze --source all

# Run bronze + silver
python -m football_pipeline.cli --bronze --silver --source open_data

# Run all layers for all sources
python -m football_pipeline.cli --all-layers --source all

# Get help
python -m football_pipeline.cli --help
```

## Project Structure

```
src/football_pipeline/
  cli.py                 # Command-line interface
  pipeline.py            # Core pipeline functions
  bronze/                # Raw data ingestion
    open_data/ingest.py
    j1_league/ingest.py
  silver/                # Data cleaning and normalization
    open_data/competitions.py
  utils/                 # Utilities
    constants.py         # Project paths and constants
    logging.py           # Simple logging setup
    dataframe.py         # Data processing utilities
```

## License
MIT

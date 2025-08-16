"""
Simple constants and path utilities for the football pipeline.
"""

from pathlib import Path

def find_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    current = Path(__file__).parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Could not find project root")

# Project structure
PROJECT_ROOT = find_project_root()
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# Supported values
SUPPORTED_LAYERS = ["landing", "bronze", "silver", "gold"]
SUPPORTED_SOURCES = ["open_data", "j1_league"]
SUPPORTED_DATA_TYPES = {
    "open_data": ["competitions", "matches", "lineups", "events", "three-sixty"],
    "j1_league": ["matches", "events", "physical", "mappings"]
}

# =============================================================================
# PREDEFINED PATHS FOR EASY NOTEBOOK USAGE
# =============================================================================

# LANDING LAYER PATHS
# Open Data
LANDING_OPEN_DATA_DIR = DATA_DIR / "landing" / "open_data" / "data"
LANDING_OPEN_DATA_COMPETITIONS = LANDING_OPEN_DATA_DIR / "competitions.json"
LANDING_OPEN_DATA_MATCHES_DIR = LANDING_OPEN_DATA_DIR / "matches"
LANDING_OPEN_DATA_LINEUPS_DIR = LANDING_OPEN_DATA_DIR / "lineups"
LANDING_OPEN_DATA_EVENTS_DIR = LANDING_OPEN_DATA_DIR / "events"
LANDING_OPEN_DATA_360_DIR = LANDING_OPEN_DATA_DIR / "three-sixty"

# J1 League
LANDING_J1_DIR = DATA_DIR / "landing" / "j1_league"
LANDING_J1_MATCHES_DIR = LANDING_J1_DIR / "matches"
LANDING_J1_EVENTS_DIR = LANDING_J1_DIR / "events"
LANDING_J1_PHYSICAL_DIR = LANDING_J1_DIR / "physical"
LANDING_J1_MAPPINGS_DIR = LANDING_J1_DIR / "mappings"

# BRONZE LAYER PATHS
# Open Data (follows landing structure - directories for multi-file datasets)
BRONZE_OPEN_DATA_DIR = DATA_DIR / "bronze" / "open_data" / "data"
BRONZE_OPEN_DATA_COMPETITIONS = BRONZE_OPEN_DATA_DIR / "competitions"
BRONZE_OPEN_DATA_MATCHES_DIR = BRONZE_OPEN_DATA_DIR / "matches"
BRONZE_OPEN_DATA_LINEUPS_DIR = BRONZE_OPEN_DATA_DIR / "lineups"
BRONZE_OPEN_DATA_EVENTS_DIR = BRONZE_OPEN_DATA_DIR / "events"
BRONZE_OPEN_DATA_360_DIR = BRONZE_OPEN_DATA_DIR / "three-sixty"

# J1 League
BRONZE_J1_DIR = DATA_DIR / "bronze" / "j1_league"
BRONZE_J1_MATCHES = BRONZE_J1_DIR / "matches"
BRONZE_J1_EVENTS = BRONZE_J1_DIR / "events"
BRONZE_J1_PHYSICAL = BRONZE_J1_DIR / "physical"
BRONZE_J1_MAPPINGS = BRONZE_J1_DIR / "mappings"

# SILVER LAYER PATHS
# Open Data
SILVER_OPEN_DATA_DIR = DATA_DIR / "silver" / "open_data"
SILVER_OPEN_DATA_COMPETITIONS = SILVER_OPEN_DATA_DIR / "competitions"
SILVER_OPEN_DATA_MATCHES_DIR = SILVER_OPEN_DATA_DIR / "matches"
SILVER_OPEN_DATA_LINEUPS_DIR = SILVER_OPEN_DATA_DIR / "lineups"
SILVER_OPEN_DATA_EVENTS_DIR = SILVER_OPEN_DATA_DIR / "events"
SILVER_OPEN_DATA_360_DIR = SILVER_OPEN_DATA_DIR / "three-sixty"

# J1 League
SILVER_J1_DIR = DATA_DIR / "silver" / "j1_league"
SILVER_J1_MATCHES = SILVER_J1_DIR / "matches"
SILVER_J1_EVENTS = SILVER_J1_DIR / "events"
SILVER_J1_PHYSICAL = SILVER_J1_DIR / "physical"
SILVER_J1_MAPPINGS = SILVER_J1_DIR / "mappings"

# GOLD LAYER PATHS
# Open Data
GOLD_OPEN_DATA_DIR = DATA_DIR / "gold" / "open_data" / "data"
GOLD_OPEN_DATA_COMPETITIONS = GOLD_OPEN_DATA_DIR / "competitions"
GOLD_OPEN_DATA_MATCHES_DIR = GOLD_OPEN_DATA_DIR / "matches"
GOLD_OPEN_DATA_LINEUPS_DIR = GOLD_OPEN_DATA_DIR / "lineups"
GOLD_OPEN_DATA_EVENTS_DIR = GOLD_OPEN_DATA_DIR / "events"
GOLD_OPEN_DATA_360_DIR = GOLD_OPEN_DATA_DIR / "three-sixty"

# J1 League
GOLD_J1_DIR = DATA_DIR / "gold" / "j1_league"
GOLD_J1_MATCHES = GOLD_J1_DIR / "matches"
GOLD_J1_EVENTS = GOLD_J1_DIR / "events"
GOLD_J1_PHYSICAL = GOLD_J1_DIR / "physical"
GOLD_J1_MAPPINGS = GOLD_J1_DIR / "mappings"

# LOG PATHS
# Open Data Logs
LOGS_OPEN_DATA_DIR = LOGS_DIR / "open_data"
LOGS_OPEN_DATA_BRONZE = LOGS_OPEN_DATA_DIR / "bronze.log"
LOGS_OPEN_DATA_SILVER = LOGS_OPEN_DATA_DIR / "silver.log"
LOGS_OPEN_DATA_GOLD = LOGS_OPEN_DATA_DIR / "gold.log"

# J1 League Logs
LOGS_J1_DIR = LOGS_DIR / "j1_league"
LOGS_J1_BRONZE = LOGS_J1_DIR / "bronze.log"
LOGS_J1_SILVER = LOGS_J1_DIR / "silver.log"
LOGS_J1_GOLD = LOGS_J1_DIR / "gold.log"

# Pipeline Logs
LOGS_PIPELINE_MAIN = LOGS_DIR / "pipeline.log"
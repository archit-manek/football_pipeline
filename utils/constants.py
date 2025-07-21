# filepath: utils/constants.py
from pathlib import Path

# Get the project root dynamically
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Define base directories
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Supported data sources
SUPPORTED_SOURCES = ["open_data", "j1_league"]

def get_open_data_dirs():
    """
    Returns directory paths for open_data source (StatsBomb structure).
    """
    source_name = "open_data"

    output_dirs = {
        # Landing data directories
        "landing": LANDING_DIR / source_name,
        "landing_matches": LANDING_DIR / source_name / "matches",
        "landing_competitions": LANDING_DIR / source_name / "competitions",
        "landing_lineups": LANDING_DIR / source_name / "lineups",
        "landing_events": LANDING_DIR / source_name / "events",
        "landing_360_events": LANDING_DIR / source_name / "three-sixty",
        
        # Bronze layer directories
        "bronze": BRONZE_DIR / source_name,
        "bronze_matches": BRONZE_DIR / source_name / "matches",
        "bronze_competitions": BRONZE_DIR / source_name / "competitions",
        "bronze_lineups": BRONZE_DIR / source_name / "lineups",
        "bronze_events": BRONZE_DIR / source_name / "events",
        "bronze_360_events": BRONZE_DIR / source_name / "360_events",
        
        # Silver layer directories
        "silver": SILVER_DIR / source_name,
        "silver_matches": SILVER_DIR / source_name / "matches",
        "silver_competitions": SILVER_DIR / source_name / "competitions",
        "silver_lineups": SILVER_DIR / source_name / "lineups",
        "silver_events": SILVER_DIR / source_name / "events",
        "silver_360_events": SILVER_DIR / source_name / "360_events",
        
        # Gold layer directories
        "gold": GOLD_DIR / source_name,
        
        # Log directories
        "logs": LOGS_DIR / source_name,
        "logs_bronze": LOGS_DIR / source_name / "bronze",
        "logs_silver": LOGS_DIR / source_name / "silver",
        "logs_gold": LOGS_DIR / source_name / "gold"
    }
    return output_dirs

def get_j1_league_dirs():
    """
    Returns directory paths for j1_league source (different structure).
    """
    source_name = "j1_league"
    return {
        # Landing data directories (J1 League specific structure)
        "landing": LANDING_DIR / source_name,
        "landing_sb_matches": LANDING_DIR / source_name / "sb-matches",
        "landing_sb_events": LANDING_DIR / source_name / "sb-events",
        "landing_hudl_physical": LANDING_DIR / source_name / "hudl-physical",
        "landing_mappings": LANDING_DIR / source_name / "mappings",
        
        # Bronze layer directories
        "bronze": BRONZE_DIR / source_name,
        "bronze_matches": BRONZE_DIR / source_name / "matches",
        "bronze_events": BRONZE_DIR / source_name / "events",
        "bronze_physical": BRONZE_DIR / source_name / "physical",
        "bronze_mappings": BRONZE_DIR / source_name / "mappings",
        
        # Silver layer directories (standardized structure)
        "silver": SILVER_DIR / source_name,
        "silver_matches": SILVER_DIR / source_name / "matches",
        "silver_competitions": SILVER_DIR / source_name / "competitions",
        "silver_lineups": SILVER_DIR / source_name / "lineups",
        "silver_events": SILVER_DIR / source_name / "events",
        "silver_360_events": SILVER_DIR / source_name / "360_events",
        
        # Gold layer directories
        "gold": GOLD_DIR / source_name,
        
        # Log directories
        "logs": LOGS_DIR / source_name,
        "logs_bronze": LOGS_DIR / source_name / "bronze",
        "logs_silver": LOGS_DIR / source_name / "silver",
        "logs_gold": LOGS_DIR / source_name / "gold"
    }

def ensure_directories_exist(source_name: str | None = None):
    """
    Ensures all necessary directories for a given source exist.
    If source_id is None, creates general directories.
    """
    if source_name == "open_data":
        dirs = get_open_data_dirs() 
    elif source_name == "j1_league":
        dirs = get_j1_league_dirs()
    else:
        raise ValueError(f"Invalid source_name: {source_name}. Supported: {SUPPORTED_SOURCES}")

    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)

def as_relative_path(path, root=PROJECT_ROOT):
    """
    Returns a path relative to the project root.
    If not possible, returns the original path as string.
    """
    path = Path(path)
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)

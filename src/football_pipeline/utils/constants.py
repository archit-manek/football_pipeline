"""Simple constants for the football pipeline."""

from pathlib import Path

def find_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    current = Path(__file__).parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Could not find project root")

PROJECT_ROOT = find_project_root()
SUPPORTED_SOURCES = ["open_data", "j1_league"]

# Simple directory structure
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# Centralized source subpaths
OPEN_DATA_SOURCE_PATH = "open_data/data"
J1_LEAGUE_SOURCE_PATH = "j1_league"

def get_data_path(layer: str, source: str, data_type: str | None = None) -> Path:
    """
    Get standardized data paths for any layer/source/data_type combination.
    
    Args:
        layer: "landing", "bronze", "silver", "gold"
        source: "open_data", "j1_league" 
        data_type: Optional data type (e.g., "competitions", "matches")
    
    Returns:
        Path to the data location
    
    Examples:
        get_data_path("bronze", "open_data", "competitions")
        -> PROJECT_ROOT/data/bronze/open_data/data/competitions.parquet
        
        get_data_path("bronze", "j1_league", "matches") 
        -> PROJECT_ROOT/data/bronze/j1_league/matches/sb_matches.parquet
    """
    if source == "open_data":
        source_path = OPEN_DATA_SOURCE_PATH
    elif source == "j1_league":
        source_path = J1_LEAGUE_SOURCE_PATH
    else:
        raise ValueError(f"Unknown source: {source}. Supported: {SUPPORTED_SOURCES}")
    
    base_path = DATA_DIR / layer / source_path
    
    if data_type is None:
        return base_path
    
    # Handle different data type patterns
    if source == "open_data":
        if data_type in ["competitions"]:
            return base_path / f"{data_type}.parquet"
        else:
            # matches, lineups, events, three-sixty are in subdirectories
            return base_path / data_type
    
    elif source == "j1_league":
        if data_type == "matches":
            return base_path / "matches" / "sb_matches.parquet"
        elif data_type == "events":
            return base_path / "events" / "sb_events.parquet"
        elif data_type == "physical":
            return base_path / "physical" / "hudl_physical.parquet"
        elif data_type == "mappings":
            return base_path / "mappings"
        else:
            return base_path / data_type
    
    # Fallback for any other cases
    return base_path / data_type
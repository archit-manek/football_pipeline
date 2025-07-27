from pathlib import Path
import os
from enum import Enum
from typing import Literal, Dict
from dotenv import load_dotenv
from platformdirs import user_cache_dir

APP_NAME = "football_pipeline"
load_dotenv()

DATA_DIR  = Path(os.getenv("DATA_DIR")  or "data").resolve()
CACHE_DIR = Path(os.getenv("CACHE_DIR") or user_cache_dir(APP_NAME)).resolve()

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR   = DATA_DIR / "gold"
LANDING_DIR = DATA_DIR / "landing"
LOGS_DIR   = Path(os.getenv("LOGS_DIR") or "logs").resolve()

# Source types
class SourceType(str, Enum):
    OPEN_DATA = "open_data"
    J1_LEAGUE = "j1_league"

# Type alias for source names
SourceName = Literal["open_data", "j1_league"]

def ensure_dirs():
    for p in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, LANDING_DIR, CACHE_DIR, LOGS_DIR):
        p.mkdir(parents=True, exist_ok=True)

def ensure_directories_exist(source_name: SourceName | None = None):
    """
    Ensures all necessary directories for a given source exist.
    
    Args:
        source_name: The source name to create directories for, or None for base directories
    """
    ensure_dirs()
    
    if source_name:
        # Validate source name
        try:
            SourceType(source_name)
        except ValueError:
            raise ValueError(f"Invalid source_name: {source_name}. Supported: {[s.value for s in SourceType]}")
        
        # Get all directories for this source
        source_dirs = get_source_dirs(source_name)
        
        # Create all directories
        for dir_path in source_dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)

def _build_source_dirs(source_name: SourceName) -> Dict[str, Path]:
    """
    Generic directory builder that creates paths for any source.
    
    Args:
        source_name: The source name (open_data, j1_league, etc.)
        
    Returns:
        Dictionary mapping path keys to Path objects
    """
    source_type = SourceType(source_name)
    
    # Standard directory structure for all sources
    dirs = {
        # Base directories
        "landing": LANDING_DIR / source_name,
        "bronze": BRONZE_DIR / source_name,
        "silver": SILVER_DIR / source_name,
        "gold": GOLD_DIR / source_name,
        "logs": LOGS_DIR / source_name,
        
        # Log subdirectories
        "logs_bronze": LOGS_DIR / source_name / "bronze",
        "logs_silver": LOGS_DIR / source_name / "silver",
        "logs_gold": LOGS_DIR / source_name / "gold",
    }
    
    # Standard data type directories (same for all sources)
    standard_types = ["matches", "competitions", "lineups", "events"]
    
    for data_type in standard_types:
        # Landing directories
        dirs[f"landing_{data_type}"] = LANDING_DIR / source_name / data_type
        # Bronze directories
        dirs[f"bronze_{data_type}"] = BRONZE_DIR / source_name / data_type
        # Silver directories
        dirs[f"silver_{data_type}"] = SILVER_DIR / source_name / data_type
    
    # Handle 360 events - use "three-sixty" consistently
    dirs["landing_360_events"] = LANDING_DIR / source_name / "three-sixty"
    dirs["bronze_360_events"] = BRONZE_DIR / source_name / "three-sixty"
    dirs["silver_360_events"] = SILVER_DIR / source_name / "three-sixty"
    
    # Source-specific directories
    if source_type == SourceType.J1_LEAGUE:
        # J1 League specific landing directories
        dirs.update({
            "landing_sb_matches": LANDING_DIR / source_name / "sb-matches",
            "landing_sb_events": LANDING_DIR / source_name / "sb-events",
            "landing_hudl_physical": LANDING_DIR / source_name / "hudl-physical",
            "landing_mappings": LANDING_DIR / source_name / "mappings",
            
            # J1 League specific bronze directories
            "bronze_physical": BRONZE_DIR / source_name / "physical",
            "bronze_mappings": BRONZE_DIR / source_name / "mappings",
        })
    
    return dirs

def get_source_dirs(source_name: SourceName) -> Dict[str, Path]:
    """
    Get directory paths for a specific source with caching.
    
    Args:
        source_name: The source name (open_data, j1_league, etc.)
        
    Returns:
        Dictionary mapping path keys to Path objects
    """
    return _build_source_dirs(source_name)

def get_open_data_dirs() -> Dict[str, Path]:
    """
    Returns directory paths for open_data source (StatsBomb structure).
    """
    return get_source_dirs("open_data")

def get_j1_league_dirs() -> Dict[str, Path]:
    """
    Returns directory paths for j1_league source (different structure).
    """
    return get_source_dirs("j1_league")
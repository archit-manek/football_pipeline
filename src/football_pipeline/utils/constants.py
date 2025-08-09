from pathlib import Path

def find_project_root() -> Path:
    """Find project root by looking for pyproject.toml or other markers."""
    current = Path(__file__).parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Could not find project root")

# Get the project root dynamically
PROJECT_ROOT = find_project_root()

def get_configured_directories():
    """Get directory paths from configuration with fallback to defaults."""
    try:
        from football_pipeline.config import get_directories_config
        dirs_config = get_directories_config()
        
        return {
            'data': PROJECT_ROOT / dirs_config.get('data', 'data'),
            'logs': PROJECT_ROOT / dirs_config.get('logs', 'logs'),
            'landing': PROJECT_ROOT / dirs_config.get('landing', 'data/landing'),
            'bronze': PROJECT_ROOT / dirs_config.get('bronze', 'data/bronze'),
            'silver': PROJECT_ROOT / dirs_config.get('silver', 'data/silver'),
            'gold': PROJECT_ROOT / dirs_config.get('gold', 'data/gold'),
        }
    except ImportError:
        # Fallback if config not available (during initial import)
        return {
            'data': PROJECT_ROOT / "data",
            'logs': PROJECT_ROOT / "logs", 
            'landing': PROJECT_ROOT / "data" / "landing",
            'bronze': PROJECT_ROOT / "data" / "bronze",
            'silver': PROJECT_ROOT / "data" / "silver",
            'gold': PROJECT_ROOT / "data" / "gold",
        }

# Legacy constants for backward compatibility
# These are kept for any code that might still reference them
# New code should use get_configured_directories() instead
_DIRS = get_configured_directories()
DATA_DIR = _DIRS['data']
LOGS_DIR = _DIRS['logs']
LANDING_DIR = _DIRS['landing']
BRONZE_DIR = _DIRS['bronze']
SILVER_DIR = _DIRS['silver']
GOLD_DIR = _DIRS['gold']

# Supported data sources
SUPPORTED_SOURCES = ["open_data", "j1_league"]

def get_open_data_dirs():
    """
    Returns directory paths for open_data source (StatsBomb structure).
    Uses configured directory paths.
    """
    # Get current configured directories
    dirs = get_configured_directories()
    source_name = "open_data/data"

    output_dirs = {
        # Landing data directories
        "landing": dirs['landing'] / source_name,
        "landing_matches": dirs['landing'] / source_name / "matches",
        "landing_competitions": dirs['landing'] / source_name,
        "landing_lineups": dirs['landing'] / source_name / "lineups",
        "landing_events": dirs['landing'] / source_name / "events",
        "landing_three_sixty_events": dirs['landing'] / source_name / "three-sixty",
        
        # Bronze layer directories
        "bronze": dirs['bronze'] / source_name,
        "bronze_matches": dirs['bronze'] / source_name / "matches",
        "bronze_competitions": dirs['bronze'] / source_name,
        "bronze_lineups": dirs['bronze'] / source_name / "lineups",
        "bronze_events": dirs['bronze'] / source_name / "events",
        "bronze_three_sixty_events": dirs['bronze'] / source_name / "three-sixty",
        
        # Silver layer directories
        "silver": dirs['silver'] / source_name,
        "silver_matches": dirs['silver'] / source_name / "matches",
        "silver_competitions": dirs['silver'] / source_name,
        "silver_lineups": dirs['silver'] / source_name / "lineups",
        "silver_events": dirs['silver'] / source_name / "events",
        "silver_three_sixty_events": dirs['silver'] / source_name / "three-sixty",
        
        # Gold layer directories
        "gold": dirs['gold'] / source_name,
        
        # Log directories
        "logs": dirs['logs'] / source_name,
        "logs_bronze": dirs['logs'] / source_name / "bronze",
        "logs_silver": dirs['logs'] / source_name / "silver",
        "logs_gold": dirs['logs'] / source_name / "gold"
    }
    return output_dirs

def get_j1_league_dirs():
    """
    Returns directory paths for j1_league source (different structure).
    Uses configured directory paths.
    """
    # Get current configured directories
    dirs = get_configured_directories()
    source_name = "j1_league"
    return {
        # Landing data directories (J1 League specific structure)
        "landing": dirs['landing'] / source_name,
        "landing_sb_matches": dirs['landing'] / source_name / "sb-matches",
        "landing_sb_events": dirs['landing'] / source_name / "sb-events",
        "landing_hudl_physical": dirs['landing'] / source_name / "hudl-physical",
        "landing_mappings": dirs['landing'] / source_name / "mappings",
        
        # Bronze layer directories
        "bronze": dirs['bronze'] / source_name,
        "bronze_matches": dirs['bronze'] / source_name / "matches",
        "bronze_events": dirs['bronze'] / source_name / "events",
        "bronze_physical": dirs['bronze'] / source_name / "physical",
        "bronze_mappings": dirs['bronze'] / source_name / "mappings",
        
        # Silver layer directories (standardized structure)
        "silver": dirs['silver'] / source_name,
        "silver_matches": dirs['silver'] / source_name / "matches",
        "silver_competitions": dirs['silver'] / source_name / "competitions",
        "silver_lineups": dirs['silver'] / source_name / "lineups",
        "silver_events": dirs['silver'] / source_name / "events",
        "silver_360_events": dirs['silver'] / source_name / "three-sixty",
        
        # Gold layer directories
        "gold": dirs['gold'] / source_name,
        
        # Log directories
        "logs": dirs['logs'] / source_name,
        "logs_bronze": dirs['logs'] / source_name / "bronze",
        "logs_silver": dirs['logs'] / source_name / "silver",
        "logs_gold": dirs['logs'] / source_name / "gold"
    }

def ensure_directories_exist(source_name: str | None = None, include_logs: bool = False):
    """
    Ensures all necessary directories for a given source exist.
    If source_name is None, creates directories for all sources.
    
    Args:
        source_name: The source name ("open_data", "j1_league", or None for all)
        include_logs: Whether to create log directories (default: False)
    """
    if source_name == "open_data":
        dirs = get_open_data_dirs() 
    elif source_name == "j1_league":
        dirs = get_j1_league_dirs()
    elif source_name is None:
        # Create directories for all sources
        all_dirs = {}
        all_dirs.update(get_open_data_dirs())
        all_dirs.update(get_j1_league_dirs())
        dirs = all_dirs
    else:
        raise ValueError(f"Invalid source_name: {source_name}. Supported: {SUPPORTED_SOURCES}")

    for dir_path in dirs.values():
        # Skip log directories unless include_logs=True
        if not include_logs and "logs" in str(dir_path):
            continue
        dir_path.mkdir(parents=True, exist_ok=True)



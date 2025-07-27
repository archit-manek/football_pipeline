import logging
from src.utils.constants import get_open_data_dirs, ensure_directories_exist
from src.utils.dataframe import ingest_json_batch_to_parquet, ingest_json_to_parquet

OPEN_DATA_DIRS = get_open_data_dirs()

# Ensure the log directory exists first
OPEN_DATA_DIRS["logs_bronze"].mkdir(parents=True, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OPEN_DATA_DIRS["logs_bronze"] / "bronze_open_data.log", mode="w"),
        logging.StreamHandler()
    ]
)

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def ingest_competitions_local():
    """
    Ingest competitions from the raw data directory into the bronze layer.
    """
    ingest_json_to_parquet(
        OPEN_DATA_DIRS["landing_competitions"] / "competitions.json",
        OPEN_DATA_DIRS["bronze_competitions"] / "competitions.parquet",
        logger=None,
        description="competitions"
    )

def ingest_matches_local():
    """
    Ingest matches from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_matches"],
        output_dir=OPEN_DATA_DIRS["bronze_matches"],
        logger=logger,
        description="matches",
        file_pattern="*/*.json",  # matches are in subdirectories
        output_prefix="matches",
        log_frequency=5
    )

def ingest_lineups_local():
    """
    Ingest lineups from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_lineups"],
        output_dir=OPEN_DATA_DIRS["bronze_lineups"],
        logger=logger,
        description="lineups",
        output_prefix="lineups",
        log_frequency=10
    )

def ingest_events_local():
    """
    Ingest events from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_events"],
        output_dir=OPEN_DATA_DIRS["bronze_events"],
        logger=logger,
        description="events",
        output_prefix="events",
        log_frequency=50
    )

def ingest_360_events_local():
    """
    Ingest 360 events from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_360_events"],
        output_dir=OPEN_DATA_DIRS["bronze_360_events"],
        logger=logger,
        description="360 events",
        output_prefix="events_360",
        log_frequency=50
    )

def open_data_ingest():
    """
    Ingest all open_data bronze layer data from the raw data directory.
    """
    logger.info("Starting open_data bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("open_data")
    
    ingest_competitions_local()
    ingest_matches_local()
    ingest_lineups_local()
    ingest_events_local()
    ingest_360_events_local()
    
    logger.info("Open_data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
import logging
from src.utils.settings import get_open_data_dirs, ensure_directories_exist
from src.utils.dataframe import ingest_json_batch_to_parquet, ingest_json_to_parquet
from src.utils.logging import setup_logger

# Get directory paths
OPEN_DATA_DIRS = get_open_data_dirs()

def ingest_competitions_local(logger):
    """
    Ingest competitions from the raw data directory into the bronze layer.
    """
    ingest_json_to_parquet(
        OPEN_DATA_DIRS["landing_competitions"] / "competitions.json",
        OPEN_DATA_DIRS["bronze_competitions"] / "competitions.parquet",
        logger=logger,
        description="competitions"
    )

def ingest_matches_local(logger):
    """
    Ingest matches from the raw data directory into the bronze layer.
    Maintains the season_id/match_id.json structure in bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_matches"],
        output_dir=OPEN_DATA_DIRS["bronze_matches"],
        logger=logger,
        description="matches",
        file_pattern="*/*.json",  # matches are in subdirectories
        maintain_structure=True,  # Maintain season_id/match_id structure
        log_frequency=5
    )

def ingest_lineups_local(logger):
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

def ingest_events_local(logger):
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

def ingest_360_events_local(logger):
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
    # Ensure all necessary directories exist first
    ensure_directories_exist("open_data")
    
    # Setup logging after directories are created
    log_path = OPEN_DATA_DIRS["logs_bronze"] / "bronze_open_data.log"
    logger = setup_logger(log_path, "open_data_bronze_layer")
    
    logger.info("Starting open_data bronze layer ingestion...")
    
    ingest_competitions_local(logger)
    ingest_matches_local(logger)
    ingest_lineups_local(logger)
    ingest_events_local(logger)
    ingest_360_events_local(logger)
    
    logger.info("Open_data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
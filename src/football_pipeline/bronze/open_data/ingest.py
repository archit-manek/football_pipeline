from football_pipeline.utils.constants import get_open_data_dirs, ensure_directories_exist
from football_pipeline.utils.dataframe import ingest_json_batch_to_parquet, ingest_json_to_parquet
from football_pipeline.utils.logging import setup_logger

OPEN_DATA_DIRS = get_open_data_dirs()

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

def ingest_matches_local(logger):
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

def ingest_three_sixty_events_local(logger):
    """
    Ingest three-sixty events from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=OPEN_DATA_DIRS["landing_three_sixty_events"],
        output_dir=OPEN_DATA_DIRS["bronze_three_sixty_events"],
        logger=logger,
        description="three-sixty events",
        output_prefix="events_three_sixty",
        log_frequency=50
    )

def open_data_ingest(logger=None):
    """
    Ingest all open_data bronze layer data from the raw data directory.
    
    Args:
        logger: Optional logger to use. If None, creates a new one.
    """
    if logger is None:
        log_path = OPEN_DATA_DIRS["logs_bronze"] / "bronze_open_data.log"
        logger = setup_logger(log_path, "open_data_bronze")
    
    logger.info("Starting open_data bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("open_data")
    
    ingest_competitions_local()
    ingest_matches_local(logger)
    ingest_lineups_local(logger)
    ingest_events_local(logger)
    ingest_three_sixty_events_local(logger)
    
    logger.info("Open_data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
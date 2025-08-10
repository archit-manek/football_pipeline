from football_pipeline.utils.constants import DATA_DIR
from football_pipeline.utils.dataframe import ingest_json_batch_to_parquet, ingest_json_to_parquet
from football_pipeline.utils.logging import setup_logger

# Simple helper to get open_data paths
def _get_paths():
    """Get open_data paths with subdirectories."""
    source_path = "open_data/data"
    return {
        # Landing
        "landing_competitions": DATA_DIR / "landing" / source_path,
        "landing_matches": DATA_DIR / "landing" / source_path / "matches", 
        "landing_lineups": DATA_DIR / "landing" / source_path / "lineups",
        "landing_events": DATA_DIR / "landing" / source_path / "events",
        "landing_three_sixty_events": DATA_DIR / "landing" / source_path / "three-sixty",
        # Bronze
        "bronze_competitions": DATA_DIR / "bronze" / source_path,
        "bronze_matches": DATA_DIR / "bronze" / source_path / "matches",
        "bronze_lineups": DATA_DIR / "bronze" / source_path / "lineups", 
        "bronze_events": DATA_DIR / "bronze" / source_path / "events",
        "bronze_three_sixty_events": DATA_DIR / "bronze" / source_path / "three-sixty"
    }

def ingest_competitions_local():
    """
    Ingest competitions from the raw data directory into the bronze layer.
    """
    paths = _get_paths()
    ingest_json_to_parquet(
        paths["landing_competitions"] / "competitions.json",
        paths["bronze_competitions"] / "competitions.parquet",
        logger=None,
        description="competitions"
    )

def ingest_matches_local(logger):
    """
    Ingest matches from the raw data directory into the bronze layer.
    """
    ingest_json_batch_to_parquet(
        input_dir=_get_paths()["landing_matches"],
        output_dir=_get_paths()["bronze_matches"],
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
        input_dir=_get_paths()["landing_lineups"],
        output_dir=_get_paths()["bronze_lineups"],
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
        input_dir=_get_paths()["landing_events"],
        output_dir=_get_paths()["bronze_events"],
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
        input_dir=_get_paths()["landing_three_sixty_events"],
        output_dir=_get_paths()["bronze_three_sixty_events"],
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
        log_path = _get_paths()["logs_bronze"] / "bronze_open_data.log"
        logger = setup_logger(log_path, "open_data_bronze")
    
    logger.info("Starting open_data bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    # Directories are created by the pipeline
    
    ingest_competitions_local()
    ingest_matches_local(logger)
    ingest_lineups_local(logger)
    ingest_events_local(logger)
    ingest_three_sixty_events_local(logger)
    
    logger.info("Open_data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
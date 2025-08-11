import polars as pl
from football_pipeline.utils.constants import DATA_DIR, LOGS_DIR
from football_pipeline.utils.dataframe import ingest_json_to_parquet, ingest_csv_batch_to_parquet
from football_pipeline.utils.logging import setup_logger

# Simple helper to get j1_league paths
def _get_paths():
    base = {
        "landing": DATA_DIR / "landing" / "j1_league",
        "bronze": DATA_DIR / "bronze" / "j1_league",
    }
    return {
        # Landing
        "landing_sb_events": base["landing"] / "sb-events",
        "landing_sb_matches": base["landing"] / "sb-matches",
        "landing_hudl_physical": base["landing"] / "hudl-physical",
        "landing_mappings": base["landing"] / "mappings",
        # Bronze
        "bronze_events": base["bronze"] / "events",
        "bronze_matches": base["bronze"] / "matches",
        "bronze_physical": base["bronze"] / "physical",
        "bronze_mappings": base["bronze"] / "mappings",
    }

def ingest_j1_league_events(logger):
    """
    Ingest J1 League events from the landing directory into the bronze layer.
    """
    p = _get_paths()
    ingest_json_to_parquet(
        p["landing_sb_events"] / "sb_events.json",
        p["bronze_events"] / "sb_events.parquet",
        logger,
        description="events",
    )

def ingest_j1_league_matches(logger):
    """
    Ingest J1 League matches from the landing directory into the bronze layer.
    """
    p = _get_paths()
    ingest_json_to_parquet(
        p["landing_sb_matches"] / "sb_matches.json",
        p["bronze_matches"] / "sb_matches.parquet",
        logger,
        description="matches",
    )

def ingest_j1_league_physical(logger):
    """
    Ingest J1 League physical data from the landing directory into the bronze layer.
    """
    p = _get_paths()
    ingest_json_to_parquet(
        p["landing_hudl_physical"] / "hudl_physical.json",
        p["bronze_physical"] / "hudl_physical.parquet",
        logger,
        description="physical"
    )

def ingest_j1_league_mappings(logger):
    """
    Ingest all CSV mapping files in the J1 League mappings directory.
    """
    logger.info("Starting J1 League mappings ingestion...")
    p = _get_paths()
    mappings_dir = p["landing_mappings"]
    output_dir = p["bronze_mappings"]
    ingest_csv_batch_to_parquet(
        input_dir=mappings_dir,
        output_dir=output_dir,
        logger=logger,
        description="mapping",
        log_frequency=1,
    )

def j1_league_ingest(logger=None):
    """
    Main function to ingest all J1 League bronze layer data.
    
    Args:
        logger: Optional logger to use. If None, creates a new one.
    """
    if logger is None:
        # Setup logger only when this function is called
        log_path = LOGS_DIR / "j1_league" / "bronze" / "j1_league_bronze.log"
        logger = setup_logger(log_path, "j1_league_bronze")
    
    logger.info("Starting J1 League bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    # Directories are created by the pipeline
    
    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)
    
    try:
        # Ingest all data types
        ingest_j1_league_matches(logger)
        ingest_j1_league_events(logger)
        ingest_j1_league_physical(logger)
        ingest_j1_league_mappings(logger)
        
        logger.info("J1 League bronze layer ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during J1 League bronze layer ingestion: {e}")
        raise

if __name__ == "__main__":
    j1_league_ingest() 
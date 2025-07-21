import polars as pl
from utils.constants import get_j1_league_dirs, ensure_directories_exist
from utils.dataframe import ingest_json_to_parquet, ingest_csv_batch_to_parquet
from utils.logging import setup_logger

# Get J1 League directories
J1_LEAGUE_DIRS = get_j1_league_dirs()

# Set up logging
log_path = J1_LEAGUE_DIRS["logs_bronze"] / "j1_league_bronze.log"
logger = setup_logger(log_path, "j1_league_bronze")

def ingest_j1_league_events():
    """
    Ingest J1 League events from the landing directory into the bronze layer.
    """
    ingest_json_to_parquet(
        J1_LEAGUE_DIRS["landing_sb_events"] / "sb_events.json",
        J1_LEAGUE_DIRS["bronze_events"] / "sb_events.parquet",
        logger,
        description="events",
    )

def ingest_j1_league_matches():
    """
    Ingest J1 League matches from the landing directory into the bronze layer.
    """
    ingest_json_to_parquet(
        J1_LEAGUE_DIRS["landing_sb_matches"] / "sb_matches.json",
        J1_LEAGUE_DIRS["bronze_matches"] / "sb_matches.parquet",
        logger,
        description="matches",
    )

def ingest_j1_league_physical():
    """
    Ingest J1 League physical data from the landing directory into the bronze layer.
    """
    ingest_json_to_parquet(
        J1_LEAGUE_DIRS["landing_hudl_physical"] / "hudl_physical.json",
        J1_LEAGUE_DIRS["bronze_physical"] / "hudl_physical.parquet",
        logger,
        description="physical"
    )

def ingest_j1_league_mappings():
    """
    Ingest all CSV mapping files in the J1 League mappings directory.
    """
    logger.info("Starting J1 League mappings ingestion...")
    mappings_dir = J1_LEAGUE_DIRS["landing_mappings"]
    output_dir = J1_LEAGUE_DIRS["bronze_mappings"]
    ingest_csv_batch_to_parquet(
        input_dir=mappings_dir,
        output_dir=output_dir,
        logger=logger,
        description="mapping",
        log_frequency=1,
    )

def j1_league_ingest():
    """
    Main function to ingest all J1 League bronze layer data.
    """
    logger.info("Starting J1 League bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("j1_league")
    
    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)
    
    try:
        # Ingest all data types
        ingest_j1_league_matches()
        ingest_j1_league_events()
        ingest_j1_league_physical()
        ingest_j1_league_mappings()
        
        logger.info("J1 League bronze layer ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during J1 League bronze layer ingestion: {e}")
        raise

if __name__ == "__main__":
    j1_league_ingest() 
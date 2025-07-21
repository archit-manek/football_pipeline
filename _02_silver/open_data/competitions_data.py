import polars as pl
from pathlib import Path

from utils.constants import get_open_data_dirs
from utils.dataframe import *
from schemas.open_data.competitions_schema import COMPETITIONS_SCHEMA
from utils.logging import setup_logger

def process_competitions_data():
    """
    Process competitions data from bronze to silver layer for a specific source.

    """
    # Get source-specific directories
    dirs = get_open_data_dirs()
    
    # Set up logging
    log_path = dirs["logs_silver"] / "competitions.log"
    logger = setup_logger(log_path, f"bronze_open_data_competitions")
    
    logger.info(f"Starting silver competitions processing pipeline for open_data...")
    
    bronze_comp_path = dirs["bronze_competitions"] / "competitions.parquet"
    silver_comp_path = dirs["silver_competitions"] / "competitions.parquet"

    # Ensure the silver directory exists
    dirs["silver_competitions"].mkdir(parents=True, exist_ok=True)

    # Check if source is newer than output
    if silver_comp_path.exists() and not is_source_newer(bronze_comp_path, silver_comp_path):
        logger.info("Silver competitions file is up to date, skipping processing")
        return

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    try:
        # Read bronze competitions data
        df = pl.read_parquet(bronze_comp_path)
        competitions_count = len(df)
        logger.info(f"Processing {competitions_count} competitions")

        # Data is already flattened by pandas json_normalize in bronze layer
        # Convert dot notation to underscore notation for silver layer standardization
        df = normalize_column_names(df)
        
        # Write to silver layer
        df.write_parquet(silver_comp_path, compression="snappy")
        logger.info(f"Successfully processed {competitions_count} competitions to silver layer")

    except Exception as e:
        logger.error(f"Failed to process competitions data: {e}")
        raise

    logger.info(f"=== SILVER COMPETITIONS PROCESSING SUMMARY OPEN_DATA ===")
    logger.info(f"Competitions processed: {competitions_count}")

if __name__ == "__main__":
    process_competitions_data()
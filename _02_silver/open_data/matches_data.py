import polars as pl
from pathlib import Path

from utils.constants import get_open_data_dirs
from utils.dataframe import *
from schemas.open_data.matches_schema import MATCHES_SCHEMA
from utils.logging import setup_logger

def process_matches_data():
    """
    Process matches data from bronze to silver layer for a specific source.
    
    Args:
    """
    # Get source-specific directories
    dirs = get_open_data_dirs()
    
    # Set up logging
    log_path = dirs["logs_silver"] / "matches.log"
    logger = setup_logger(log_path, f"bronze_open_data_matches")
    
    logger.info(f"Starting silver matches processing pipeline for open_data...")
    
    bronze_matches_dir = dirs["bronze_matches"]
    silver_matches_dir = dirs["silver_matches"]

    # Get all bronze files
    bronze_files = list(bronze_matches_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze match files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_matches = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        match_file = parquet_file.stem
        silver_path = silver_matches_dir / parquet_file.name
        
        # Check if source is newer than output
        if silver_path.exists() and not is_source_newer(parquet_file, silver_path):
            skipped_count += 1
            continue
        
        try:
            df = pl.read_parquet(parquet_file)
            matches_count = len(df)
            total_matches += matches_count

            # Data is already flattened by pandas json_normalize in bronze layer
            # Convert dot notation to underscore notation for silver layer standardization
            df = normalize_column_names(df)
            
            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 20 files (fewer match files than events)
            if processed_count % 20 == 0:
                logger.info(f"Processed {processed_count} match files...")
            
        except Exception as e:
            logger.warning(f"Failed to process matches file {match_file}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info(f"=== SILVER MATCHES PROCESSING SUMMARY OPEN_DATA ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total matches processed: {total_matches:,}")

if __name__ == "__main__":
    process_matches_data()
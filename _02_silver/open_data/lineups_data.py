from pathlib import Path
import polars as pl
from schemas.open_data.lineups_schema import LINEUPS_SCHEMA
from utils.constants import get_open_data_dirs
from utils.dataframe import *
from utils.logging import setup_logger

def process_lineups_data():
    """
    Process lineups data from bronze to silver layer for a specific source.
    
    Args:

    """
    # Get source-specific directories
    dirs = get_open_data_dirs()
    
    # Set up logging TODO: debug
    log_path = dirs["logs_silver"] / "lineups.log"
    logger = setup_logger(log_path, f"bronze_open_data_lineups")
    
    logger.info(f"Starting silver lineups processing pipeline for open_data...")
    
    bronze_lineups_dir = dirs["bronze_lineups"]
    silver_lineups_dir = dirs["silver_lineups"]

    # Ensure silver directory exists
    silver_lineups_dir.mkdir(parents=True, exist_ok=True)

    # Get all bronze files
    bronze_files = list(bronze_lineups_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze lineup files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_lineups = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        lineup_file = parquet_file.stem
        silver_path = silver_lineups_dir / parquet_file.name
        
        # Check if source is newer than output
        if silver_path.exists() and not is_source_newer(parquet_file, silver_path):
            skipped_count += 1
            continue
        
        try:
            df = pl.read_parquet(parquet_file)
            lineups_count = len(df)
            total_lineups += lineups_count

            # Data is already flattened by pandas json_normalize in bronze layer
            # Convert dot notation to underscore notation for silver layer standardization
            df = normalize_column_names(df)
            
            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 100 files to reduce I/O overhead
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} lineup files...")
            
        except Exception as e:
            logger.warning(f"Failed to process lineups file {lineup_file}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info(f"=== SILVER LINEUPS PROCESSING SUMMARY ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total lineups processed: {total_lineups:,}")

if __name__ == "__main__":
    process_lineups_data()



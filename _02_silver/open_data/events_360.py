import polars as pl
from utils.constants import get_open_data_dirs
from utils.dataframe import *
from utils.logging import setup_logger
from schemas.open_data.schema_360 import SCHEMA_360

def process_360_events_data():
    """
    Process 360 events data from bronze to silver layer for a specific source.
    
    Args:
    """
    # Get source-specific directories
    dirs = get_open_data_dirs()
    
    # Set up logging
    log_path = dirs["logs_silver"] / "360_events.log"
    logger = setup_logger(log_path, f"bronze_open_data_360_events")
    
    logger.info(f"Starting silver 360 events processing pipeline for open_data...")

    bronze_360_events_dir = dirs["bronze_360_events"]
    silver_360_events_dir = dirs["silver_360_events"]

    # Ensure the silver directory exists
    silver_360_events_dir.mkdir(parents=True, exist_ok=True)

    # Get all bronze files
    bronze_files = list(bronze_360_events_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze 360 event files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_events = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        event_file = parquet_file.stem
        silver_path = silver_360_events_dir / parquet_file.name
        
        # Check if source is newer than output
        if silver_path.exists() and not is_source_newer(parquet_file, silver_path):
            skipped_count += 1
            continue
        
        try:
            df = pl.read_parquet(parquet_file)
            events_count = len(df)
            total_events += events_count

            # Flatten columns
            df = flatten_columns(df)

            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 50 files to reduce I/O overhead
            if processed_count % 50 == 0:
                logger.info(f"Processed {processed_count} 360 event files...")
            
        except Exception as e:
            logger.warning(f"Failed to process 360 events file {event_file}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info(f"=== SILVER 360 EVENTS PROCESSING SUMMARY OPEN_DATA ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total 360 events processed: {total_events}")

if __name__ == "__main__":
    process_360_events_data()
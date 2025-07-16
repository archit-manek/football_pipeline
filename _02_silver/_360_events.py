import importlib
from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_360_EVENTS, SILVER_DIR_360_EVENTS, SILVER_LOGS_360_EVENTS_PATH
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, get_int_columns_from_schema, fix_int_columns_with_nans, is_source_newer
from utils.logging import setup_logger

# Import the 360 schema using importlib since the filename starts with a number
schema_module = importlib.import_module('schemas.360_schema')
_360_schema = schema_module._360_schema

# logs/silver/360_events.log
log_path = Path(SILVER_LOGS_360_EVENTS_PATH)
logger = setup_logger(log_path, "360_events")



def process_360_events_data():
    logger.info("Starting silver 360 events processing pipeline...")

    bronze_360_events_dir = Path(BRONZE_DIR_360_EVENTS)
    silver_360_events_dir = Path(SILVER_DIR_360_EVENTS)

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
            
            # Add missing columns
            expected_cols = set(_360_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)

            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(_360_schema)
            df = fix_int_columns_with_nans(df, int_cols)

            # Cast columns to schema types
            df = cast_columns_to_schema_types(df, _360_schema)

            # Validate schema
            log_schema_differences(df, _360_schema, logger, parquet_file)

            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 50 files to reduce I/O overhead
            if processed_count % 50 == 0:
                logger.info(f"Processed {processed_count} 360 event files...")
            
        except Exception as e:
            logger.warning(f"Failed to process 360 events file {event_file}: {e}")
            error_count += 1
            continue

    # Summary without performance metrics
    logger.info("=== SILVER 360 EVENTS PROCESSING SUMMARY ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total 360 events processed: {total_events:,}")
    if processed_count > 0:
        logger.info(f"Average events per file: {total_events // processed_count:,}")
    logger.info("=============================================")
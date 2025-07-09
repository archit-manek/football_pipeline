import logging
import importlib
from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_360_EVENTS, SILVER_DIR_360_EVENTS, SILVER_LOGS_360_EVENTS_PATH
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, get_int_columns_from_schema, fix_int_columns_with_nans
from utils.logging import setup_logger

# Import the 360 schema using importlib since the filename starts with a number
schema_module = importlib.import_module('schemas.360_schema')
_360_schema = schema_module._360_schema

# logs/silver/360_events.log
log_path = Path(SILVER_LOGS_360_EVENTS_PATH)
logger = setup_logger(log_path, "360_events")

def process_360_events_data():
    logger.info("Processing 360 events data...")

    bronze_360_events_dir = Path(BRONZE_DIR_360_EVENTS)
    silver_360_events_dir = Path(SILVER_DIR_360_EVENTS)

    # Ensure the silver directory exists
    Path(SILVER_DIR_360_EVENTS).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_360_EVENTS).glob("*.parquet"):
        try:
            file.unlink()
            logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_360_events_dir.glob("*.parquet"):
        event_file = parquet_file.stem
        logger.info(f"Processing 360 events from {parquet_file}")
        
        try:
            df = pl.read_parquet(parquet_file)
            logger.info(f"Loaded {len(df)} 360 event records from {event_file}")

            # Transformations can be added here
            df = flatten_columns(df)
            logger.info(f"Flattened columns for {event_file}")
            
            # Add missing columns
            expected_cols = set(_360_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            logger.info(f"Added missing columns for {event_file}")

            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(_360_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            logger.info(f"Fixed {len(int_cols)} integer columns to preserve Int64 type for {event_file}")

            # Cast columns to schema types
            df = cast_columns_to_schema_types(df, _360_schema)
            logger.info(f"Cast columns to schema types for {event_file}")

            # Validate schema
            log_schema_differences(df, _360_schema, logger, parquet_file)
            logger.info(f"Schema validation skipped for {event_file} (validation disabled)")

            df.write_parquet(silver_360_events_dir / parquet_file.name)
            logger.info(f"Saved processed 360 events for {event_file} to {silver_360_events_dir / parquet_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to process 360 events file {parquet_file}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            continue

    logger.info("360 events data processed successfully.")
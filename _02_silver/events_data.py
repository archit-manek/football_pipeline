import polars as pl
import numpy as np
import pandera as pa
from pathlib import Path
from utils.constants import *
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, validate_polars_with_pandera, get_int_columns_from_schema, fix_int_columns_with_nans, is_source_newer
from schemas.events_schema import events_schema
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_EVENTS_PATH)
logger = setup_logger(log_path, "events")



###
# Process match event data from bronze to silver layer
###
def process_events_data():
    logger.info("Starting silver events processing pipeline...")
    
    bronze_events_dir = Path(BRONZE_DIR_EVENTS)
    silver_events_dir = Path(SILVER_DIR_EVENTS)

    # Ensure silver directory exists
    silver_events_dir.mkdir(parents=True, exist_ok=True)

    # Get all bronze files
    bronze_files = list(bronze_events_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze event files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_events = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        match_id = parquet_file.stem.replace("events_", "")
        silver_path = silver_events_dir / f"events_{match_id}.parquet"
        
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
            expected_cols = set(events_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            
            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(events_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            
            # Cast columns to schema types
            df = cast_columns_to_schema_types(df, events_schema)
            
            # Process timestamps
            df = df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%H:%M:%S%.f", strict=False)
            )
            
            # Enrich locations
            df = enrich_locations(df)
            
            # Add possession stats
            df = add_possession_stats(df)
            
            # Validate schema
            log_schema_differences(df, events_schema, logger, parquet_file)
            
            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 100 files to reduce I/O overhead
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} event files...")
                
        except Exception as e:
            logger.warning(f"Failed to process match {match_id}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info("=== SILVER EVENTS PROCESSING SUMMARY ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total events processed: {total_events:,}")
    if processed_count > 0:
        logger.info(f"Average events per file: {total_events // processed_count:,}")
    logger.info("==========================================")

### Location Normalization Functions ###

def enrich_locations(df):
    df = df.with_columns([
        pl.col("location").cast(pl.Array(pl.Float64, 2)).alias("location"),
        pl.col("pass_end_location").cast(pl.Array(pl.Float64, 2)).alias("pass_end_location"),
    ])
    df = df.with_columns([
        (pl.col("location").arr.get(0) / 120).alias("x"),
        (pl.col("location").arr.get(1) / 80).alias("y"),
        pl.when(pl.col("type_name") == "Pass")
          .then(pl.col("pass_end_location").arr.get(0) / 120)
          .otherwise(None)
          .alias("end_x"),
        pl.when(pl.col("type_name") == "Pass")
          .then(pl.col("pass_end_location").arr.get(1) / 80)
          .otherwise(None)
          .alias("end_y"),
    ])
    return df

def add_possession_stats(df):
    """Add possession-level statistics to events"""
    df = df.with_columns([
        pl.col("possession").count().over("possession").alias("possession_event_count"),
        pl.col("possession").filter(pl.col("type_name") == "Pass").count().over("possession").alias("possession_pass_count"),
        pl.col("player_id").n_unique().over("possession").alias("possession_player_count"),
        pl.col("duration").sum().over("possession").alias("possession_duration"),
    ])
    return df
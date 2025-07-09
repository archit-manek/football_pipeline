import polars as pl
import logging
import numpy as np
import pandera as pa
from pathlib import Path
from utils.constants import *
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, validate_polars_with_pandera, get_int_columns_from_schema, fix_int_columns_with_nans
from schemas.events_schema import events_schema
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_EVENTS_PATH)
logger = setup_logger(log_path, "events")

###
# Process match event data from bronze to silver layer
###
def process_events_data():
    logger.info("Starting match data processing pipeline.")
    bronze_events_dir = Path(BRONZE_DIR_EVENTS)
    silver_events_dir = Path(SILVER_DIR_EVENTS)

    # Clean the silver directory before processing
    if silver_events_dir.exists():
        for file in silver_events_dir.glob("*.parquet"):
            try:
                file.unlink()
                logger.info(f"Deleted old file: {file}")
            except Exception as e:
                logger.warning(f"Could not delete file {file}: {e}")
    else:
        silver_events_dir.mkdir(parents=True, exist_ok=True)

    for parquet_file in bronze_events_dir.glob("*.parquet"):
        match_id = parquet_file.stem.replace("events_", "")
        logger.info(f"Processing match {match_id} from {parquet_file}")
        try:
            df = pl.read_parquet(parquet_file)
            logger.info(f"Loaded {len(df)} events for match {match_id}")
            
            # Flatten columns
            df = flatten_columns(df)
            logger.info(f"Flattened columns for match {match_id}")
            
            # Add missing columns
            expected_cols = set(events_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            logger.info(f"Added missing columns for match {match_id}")
            
            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(events_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            logger.info(f"Fixed {len(int_cols)} integer columns to preserve Int64 type for match {match_id}")
            
            # Cast columns to schema types
            df = cast_columns_to_schema_types(df, events_schema)
            logger.info(f"Cast columns to schema types for match {match_id}")
            
            # Process timestamps
            df = df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%H:%M:%S%.f", strict=False)
            )
            logger.info(f"Processed timestamps for match {match_id}")
            
            # Enrich locations
            df = enrich_locations(df)
            logger.info(f"Enriched locations for match {match_id}")
            
            # Add possession stats
            df = add_possession_stats(df)
            logger.info(f"Added possession stats for match {match_id}")
            
            # Validate schema
            log_schema_differences(df, events_schema, logger, parquet_file)
            logger.info(f"Schema validation skipped for match {match_id} (validation disabled)")
            
            df.write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logger.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logger.warning(f"Failed to process match {match_id}: {e}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")
    logger.info("Completed match data processing pipeline.")

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

### Possession Stats Functions ###

def add_possession_stats(df):
    """Add possession stats

    Args:
        df (pd.DataFrame): DataFrame with pass data

    Returns:
        pd.DataFrame: DataFrame with possession stats including:
        - possession_event_count: Number of events in each possession
        - possession_pass_count: Number of passes in each possession
        - possession_player_count: Number of unique players in each possession
        - possession_duration: Duration of each possession
        - possession_xg: Total xG in the possession
    """
    
    # Calculate event count per possession
    event_count = df.group_by("possession").agg(
        pl.count().alias("possession_event_count")
    )

    # Number of passes in each possession
    pass_count = (
        df.filter(pl.col("type_name") == "Pass")
        .group_by("possession")
        .agg(pl.count().alias("possession_pass_count"))
    )

    # Number of unique players in each possession
    player_count = df.group_by("possession").agg(
        pl.col("player_id").n_unique().alias("possession_player_count")
    )
    
    # Possession duration (in seconds)
    duration = df.group_by("possession").agg(
        (pl.col("timestamp").max() - pl.col("timestamp").min()).dt.total_seconds().alias("possession_duration")
    )

    # Total xG per possession
    total_xg = (
        df.filter(pl.col("type_name") == "Shot")
        .group_by("possession")
        .agg(pl.col("shot_statsbomb_xg").sum().alias("total_xG"))
    )

    # Merge it back into the main DataFrame as a new column
    # Check if columns already exist to avoid duplicates
    if "possession_event_count" not in df.columns:
        df = df.join(event_count, on="possession", how="left")
    if "possession_pass_count" not in df.columns:
        df = df.join(pass_count, on="possession", how="left")
    if "possession_player_count" not in df.columns:
        df = df.join(player_count, on="possession", how="left")
    if "possession_duration" not in df.columns:
        df = df.join(duration, on="possession", how="left")
    if "total_xG" not in df.columns:
        df = df.join(total_xg, on="possession", how="left")

    return df
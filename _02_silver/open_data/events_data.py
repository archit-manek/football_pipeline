import polars as pl
import numpy as np
import pandera as pa
from pathlib import Path
from utils.constants import get_open_data_dirs
from utils.dataframe import is_source_newer, normalize_column_names
from schemas.open_data.events_schema import EVENTS_SCHEMA
from utils.logging import setup_logger

def process_events_data():
    """
    Process match event data from bronze to silver layer for a specific source.
    """
    # Get source-specific directories
    dirs = get_open_data_dirs()
    
    # Set up logging
    log_path = dirs["logs_silver"] / "events.log"
    logger = setup_logger(log_path, f"bronze_open_data_events")
    
    logger.info(f"Starting silver events processing pipeline for open_data...")
    
    bronze_events_dir = dirs["bronze_events"]
    silver_events_dir = dirs["silver_events"]

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
            
            # Process timestamps
            df = (
                df
                .with_columns(
                    pl.col("timestamp")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Time, "%H:%M:%S%.3f", strict=False)
                )
            )
            
            # Add possession stats
            df = add_possession_stats(df)

            # Data is already flattened by pandas json_normalize in bronze layer
            # Convert dot notation to underscore notation for silver layer standardization
            df = normalize_column_names(df)
            
            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 100 files to reduce I/O overhead
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} event files...")
            
        except Exception as e:
            logger.warning(f"Failed to process events file {match_id}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info(f"=== SILVER EVENTS PROCESSING SUMMARY OPEN_DATA ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total events processed: {total_events}")

def add_possession_stats(df):
    """
    Add possession statistics to the dataframe.
    """
    # Add possession stats logic here

    df = df.with_columns([
        pl.col("possession").count().over("possession").alias("possession_event_count"),
        pl.col("possession").filter(pl.col("type_name") == "Pass").count().over("possession").alias("possession_pass_count"),
        pl.col("player_id").n_unique().over("possession").alias("possession_player_count")
    ])
    return df

if __name__ == "__main__":
    process_events_data()
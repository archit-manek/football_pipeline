import logging
from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_360_EVENTS, SILVER_DIR_360_EVENTS, SILVER_LOGS_360_EVENTS_PATH
from utils.dataframe import flatten_columns
from utils.logging import setup_logger

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
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_360_events_dir / parquet_file.name)
        logger.info(f"Processed matches from {parquet_file} to {silver_360_events_dir / parquet_file.name}")

    logger.info(f"Reading matches from {bronze_360_events_dir}")

    logger.info("Matches data processed successfully.")
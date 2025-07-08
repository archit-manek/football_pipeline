import logging
from pathlib import Path
import polars as pl

from utils.constants import BRONZE_DIR_MATCHES, SILVER_DIR_MATCHES, SILVER_LOGS_MATCHES_PATH
from utils.dataframe import flatten_columns
from utils.logging import setup_logger


log_path = Path(SILVER_LOGS_MATCHES_PATH)
logger = setup_logger(log_path, "matches")

def process_matches_data():
    logger.info("Processing matches data...")

    bronze_matches_dir = Path(BRONZE_DIR_MATCHES)
    silver_matches_dir = Path(SILVER_DIR_MATCHES)

    # Ensure the silver directory exists
    Path(SILVER_DIR_MATCHES).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_MATCHES).glob("*.parquet"):
        try:
            file.unlink()
            logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_matches_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_matches_dir / parquet_file.name)
        logger.info(f"Processed matches from {parquet_file} to {silver_matches_dir / parquet_file.name}")

    logger.info(f"Reading matches from {bronze_matches_dir}")

    logger.info("Matches data processed successfully.")
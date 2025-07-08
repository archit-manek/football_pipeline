import logging
from pathlib import Path
import polars as pl

from utils.constants import BRONZE_DIR_MATCHES, SILVER_DIR_MATCHES, SILVER_LOGS_MATCHES_PATH
from utils.dataframe import flatten_columns
from utils.logging import setup_logging


log_path = Path(SILVER_LOGS_MATCHES_PATH)
setup_logging(log_path)

def process_matches_data():
    logging.info("Processing matches data...")

    bronze_matches_dir = Path(BRONZE_DIR_MATCHES)
    silver_matches_dir = Path(SILVER_DIR_MATCHES)

    # Ensure the silver directory exists
    Path(SILVER_DIR_MATCHES).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_MATCHES).glob("*.parquet"):
        try:
            file.unlink()
            logging.info(f"Deleted old file: {file}")
        except Exception as e:
            logging.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_matches_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_matches_dir / parquet_file.name)
        logging.info(f"Processed matches from {parquet_file} to {silver_matches_dir / parquet_file.name}")

    logging.info(f"Reading matches from {bronze_matches_dir}")

    logging.info("Matches data processed successfully.")
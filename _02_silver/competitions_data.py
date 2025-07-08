import logging
import polars as pl
from pathlib import Path

from utils.constants import BRONZE_DIR_COMPETITIONS, SILVER_DIR_COMPETITIONS, SILVER_LOGS_COMPETITIONS_PATH
from utils.logging import setup_logger


log_path = Path(SILVER_LOGS_COMPETITIONS_PATH)
logger = setup_logger(log_path, "competitions")

###
# Process competitions data from bronze to silver layer
###
def process_competitions_data():
    """
    Process competitions data to create silver layer.
    """
    bronze_comp_path = Path(BRONZE_DIR_COMPETITIONS) / "competitions.parquet"
    silver_comp_path = Path(SILVER_DIR_COMPETITIONS) / "competitions.parquet"

    logger.info("Starting competitions data processing pipeline.")

    # Ensure the silver directory exists
    Path(SILVER_DIR_COMPETITIONS).mkdir(parents=True, exist_ok=True)

    # Delete the old file if it exists
    if silver_comp_path.exists():
        try:
            silver_comp_path.unlink()
            logger.info(f"Deleted old file: {silver_comp_path}")
        except Exception as e:
            logger.warning(f"Could not delete file {silver_comp_path}: {e}")

    logger.info(f"Reading {bronze_comp_path}")
    df = pl.read_parquet(bronze_comp_path)

    # Ensure correct dtypes for DateTime columns
    df = df.with_columns([
        pl.col("match_updated").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
        pl.col("match_available").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
    ])

    # Transformations go here

    df.write_parquet(silver_comp_path)
    logger.info(f"Saved cleaned competitions to {silver_comp_path}")

    logger.info("Competitions data processing completed.")
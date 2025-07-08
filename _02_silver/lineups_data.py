from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_LINEUPS, SILVER_DIR_LINEUPS, SILVER_LOGS_LINEUPS_PATH
from utils.dataframe import flatten_columns
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_LINEUPS_PATH)
logger = setup_logger(log_path, "lineups")

###
# Process lineups data from the bronze layer to the silver layer.
###
def process_lineups_data():
    logger.info("Starting to process lineups data...")

    bronze_lineups_dir = Path(BRONZE_DIR_LINEUPS)
    silver_lineups_dir = Path(SILVER_DIR_LINEUPS)

    # Ensure the silver directory exists
    Path(SILVER_DIR_LINEUPS).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_LINEUPS).glob("*.parquet"):
        try:
            file.unlink()
            logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_lineups_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_lineups_dir / parquet_file.name)
        logger.info(f"Processed lineups from {parquet_file} to {silver_lineups_dir / parquet_file.name}")

    logger.info("Lineups data processing completed successfully.")



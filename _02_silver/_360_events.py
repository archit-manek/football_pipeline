import logging
from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_360_EVENTS, SILVER_DIR_360_EVENTS
from utils.dataframe import flatten_columns


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/silver/360_events.log", mode="w"),
        logging.StreamHandler()
    ]
)

def process_360_events_data():
    logging.info("Processing 360 events data...")

    bronze_360_events_dir = Path(BRONZE_DIR_360_EVENTS)
    silver_360_events_dir = Path(SILVER_DIR_360_EVENTS)

    # Ensure the silver directory exists
    Path(SILVER_DIR_360_EVENTS).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_360_EVENTS).glob("*.parquet"):
        try:
            file.unlink()
            logging.info(f"Deleted old file: {file}")
        except Exception as e:
            logging.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_360_events_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_360_events_dir / parquet_file.name)
        logging.info(f"Processed matches from {parquet_file} to {silver_360_events_dir / parquet_file.name}")

    logging.info(f"Reading matches from {bronze_360_events_dir}")

    logging.info("Matches data processed successfully.")
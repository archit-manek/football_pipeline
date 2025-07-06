import logging
from pathlib import Path
import polars as pl
from utils.constants import BRONZE_DIR_LINEUPS, SILVER_DIR_LINEUPS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/silver/lineups_data.log", mode="w"),
        logging.StreamHandler()
    ]
)

###
# Process lineups data from the bronze layer to the silver layer.
###
def process_lineups_data():
    logging.info("Starting to process lineups data...")

    bronze_events_dir = Path(BRONZE_DIR_LINEUPS)
    silver_events_dir = Path(SILVER_DIR_LINEUPS)

    # Ensure the silver directory exists
    Path(SILVER_DIR_LINEUPS).mkdir(parents=True, exist_ok=True)

    # Delete the old file if it exists
    if silver_events_dir.exists():
        try:
            silver_events_dir.unlink()
            logging.info(f"Deleted old file: {silver_events_dir}")
        except Exception as e:
            logging.warning(f"Could not delete file {silver_events_dir}: {e}")

    logging.info(f"Reading lineups from {bronze_events_dir}")

    for parquet_file in bronze_events_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)

        # Transformations can be added here
        df = flatten_columns(df)

        df.write_parquet(silver_events_dir / parquet_file.name)
        logging.info(f"Processed lineups from {parquet_file} to {silver_events_dir / parquet_file.name}")

    # Placeholder for actual processing logic
    # This could involve reading from a bronze layer, transforming the data,
    # and writing to a silver layer.

        logging.info("Lineups data processing completed successfully.")
    
def flatten_columns(df):
    """
    Flatten nested columns in the DataFrame.
    """

    # Flatten 'country' struct column
    df = df.with_columns([
        pl.col("country").struct.field("id").alias("country_id"),
        pl.col("country").struct.field("name").alias("country_name"),
    ]).drop("country")

    return df


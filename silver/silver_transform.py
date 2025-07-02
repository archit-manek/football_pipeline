import pandas as pd
import polars as pl
import logging
import numpy as np
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/silver/pipeline.log", mode="w"),
        logging.StreamHandler()
    ]
)

def normalize_location(location, x_max=120, y_max=80):
    logging.debug(f"Normalizing location: {location}")
    if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
        x, y = location
        result = [x / x_max, y / y_max]
        logging.debug(f"Normalized location: {result}")
        return result
    logging.warning(f"Invalid location format: {location}")
    return [None, None]

def normalize_end_location(location, x_max=120, y_max=80):
    logging.debug(f"Normalizing end location: {location}")
    if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
        result = [location[0] / x_max, location[1] / y_max]
        logging.debug(f"Normalized end location: {result}")
        return result
    logging.debug(f"Invalid end location format: {location}")
    return [None, None]

def enrich_pass_data():
    logging.info("Starting pass data enrichment process.")
    bronze_events_dir = Path("data/bronze/events")
    silver_events_dir = Path("data/silver/events")
    silver_events_dir.mkdir(parents=True, exist_ok=True)

    for parquet_file in bronze_events_dir.glob("*.parquet"):
        match_id = parquet_file.stem.replace("events_", "")
        logging.info(f"Processing match {match_id} from {parquet_file}")
        try:
            df = pd.read_parquet(parquet_file)
            df["x"], df["y"] = zip(*df["location"].map(normalize_location))
            df["end_x"], df["end_y"] = None, None
            is_pass = (df["type.name"] == "Pass") & df["pass.end_location"].notnull()
            normalized_ends = (
                df.loc[is_pass, "pass.end_location"]
                .map(normalize_end_location)
                .apply(pd.Series)
            )
            normalized_ends.columns = ["end_x", "end_y"]
            df.loc[is_pass, ["end_x", "end_y"]] = normalized_ends.values
            if "pass.recipient.name" in df.columns:
                df["recipient"] = df["pass.recipient.name"]
            pl.from_pandas(df).write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logging.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logging.warning(f"Failed to process match {match_id}: {e}")
    logging.info("Completed pass data enrichment process.")
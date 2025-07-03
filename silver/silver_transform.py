import pandas as pd
import polars as pl
import logging
import numpy as np
from pathlib import Path
from utils.config import *
from itertools import islice

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
    logging.debug(f"Invalid location format: {location}")
    return [None, None]

def normalize_end_location(location, x_max=120, y_max=80):
    logging.debug(f"Normalizing end location: {location}")
    if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
        result = [location[0] / x_max, location[1] / y_max]
        logging.debug(f"Normalized end location: {result}")
        return result
    logging.debug(f"Invalid end location format: {location}")
    return [None, None]

def add_pass_length_angle(df):
    # Only for passes with valid end locations
    mask = (
        (df["type.name"] == "Pass") &
        df["end_x"].notnull() & df["end_y"].notnull() &
        df["x"].notnull() & df["y"].notnull()
    )
    # Ensure numeric types and handle missing values
    for col in ["end_x", "end_y", "x", "y"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    dx = df.loc[mask, "end_x"] - df.loc[mask, "x"]
    dy = df.loc[mask, "end_y"] - df.loc[mask, "y"]
    df.loc[mask, "pass_length_calc"] = np.sqrt(dx**2 + dy**2)
    df.loc[mask, "pass_angle_calc"] = np.arctan2(dy, dx)
    return df

def categorize_pass_direction(angle_rad, threshold=np.pi/4):
    # threshold = 45 degrees in radians
    if -threshold <= angle_rad <= threshold:
        return "forward"
    elif np.abs(angle_rad) > (np.pi - threshold):
        return "backward"
    else:
        return "sideways"

def enrich_locations(df):
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
    return df

def enrich_pass_features(df):
    df = add_pass_length_angle(df)
    if "pass_angle_calc" in df.columns:
        df["pass_direction_calc"] = df["pass_angle_calc"].apply(
            lambda x: categorize_pass_direction(x) if pd.notnull(x) else None
        )
    return df

def flatten_columns(df):
    df.columns = [col.replace('.', '_') for col in df.columns]
    return df

def add_possession_stats(df):
    # Number of events in each possession
    event_count = df.groupby('possession').size().rename('possession_event_count')
    # Number of passes in each possession
    pass_count = df[df['type_name'] == 'Pass'].groupby('possession').size().rename('possession_pass_count')
    # Number of unique players in each possession
    player_count = df.groupby('possession')['player_id'].nunique().rename('possession_player_count')
    # Possession duration (if you have a 'timestamp' column)
    if 'timestamp' in df.columns:
        duration = df.groupby('possession')['timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).rename('possession_duration')
    else:
        duration = None
    # Did the possession end with a shot?
    ended_with_shot = df.groupby('possession')['type_name'].apply(lambda x: (x == 'Shot').any()).rename('possession_ended_with_shot')
    # Total xG in the possession (if you have an xG column)
    if 'shot_statsbomb_xg' in df.columns:
        possession_xg = df.groupby('possession')['shot_statsbomb_xg'].sum().rename('possession_xg')
    else:
        possession_xg = None

    # Merge all stats back to the main DataFrame
    df = df.merge(event_count, left_on='possession', right_index=True, how='left')
    df = df.merge(pass_count, left_on='possession', right_index=True, how='left')
    df = df.merge(player_count, left_on='possession', right_index=True, how='left')
    if duration is not None:
        df = df.merge(duration, left_on='possession', right_index=True, how='left')
    df = df.merge(ended_with_shot, left_on='possession', right_index=True, how='left')
    if possession_xg is not None:
        df = df.merge(possession_xg, left_on='possession', right_index=True, how='left')

    return df

def enrich_pass_data():
    logging.info("Starting pass data enrichment process.")
    bronze_events_dir = Path(BRONZE_DIR_EVENTS)
    silver_events_dir = Path(SILVER_DIR_EVENTS)

    # Clean the silver directory before processing
    if silver_events_dir.exists():
        for file in silver_events_dir.glob("*.parquet"):
            try:
                file.unlink()
                logging.info(f"Deleted old file: {file}")
            except Exception as e:
                logging.warning(f"Could not delete file {file}: {e}")
    else:
        silver_events_dir.mkdir(parents=True, exist_ok=True)

    # Limit to first 50 files for testing
    for parquet_file in islice(bronze_events_dir.glob("*.parquet"), 50):
        match_id = parquet_file.stem.replace("events_", "")
        logging.info(f"Processing match {match_id} from {parquet_file}")
        try:
            df = pd.read_parquet(parquet_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S.%f')
            df = enrich_locations(df)
            df = enrich_pass_features(df)
            df = flatten_columns(df)
            df = add_possession_stats(df)
            pl.from_pandas(df).write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logging.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logging.warning(f"Failed to process match {match_id}: {e}")
    logging.info("Completed pass data enrichment process.")
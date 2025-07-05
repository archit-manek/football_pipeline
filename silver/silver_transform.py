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

### Location Normalization Functions ###

def normalize_location(location, x_max=120, y_max=80):
    logging.debug(f"Normalizing location: {location}")
    try:
        if location is None:
            logging.debug(f"Location is None: {location}")
            return [None, None]
        
        # Handle numpy arrays properly
        if isinstance(location, np.ndarray):
            if location.size == 0 or np.any(pd.isna(location)):
                logging.debug(f"Location array is empty or contains NaN: {location}")
                return [None, None]
        
        if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
            x, y = location
            result = [x / x_max, y / y_max]
            logging.debug(f"Normalized location: {result}")
            return result
        else:
            logging.debug(f"Invalid location format: {location}, type: {type(location)}")
            return [None, None]
    except Exception as e:
        logging.debug(f"Error normalizing location {location}: {e}")
        return [None, None]

def normalize_end_location(location, x_max=120, y_max=80):
    logging.debug(f"Normalizing end location: {location}")
    try:
        if location is None:
            logging.debug(f"End location is None: {location}")
            return [None, None]
        
        # Handle numpy arrays properly
        if isinstance(location, np.ndarray):
            if location.size == 0 or np.any(pd.isna(location)):
                logging.debug(f"End location array is empty or contains NaN: {location}")
                return [None, None]
        
        if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
            x, y = location
            result = [x / x_max, y / y_max]
            logging.debug(f"Normalized end location: {result}")
            return result
        else:
            logging.debug(f"Invalid end location format: {location}, type: {type(location)}")
            return [None, None]
    except Exception as e:
        logging.debug(f"Error normalizing end location {location}: {e}")
        return [None, None]

def enrich_locations(df):
    """Enrich locations

    Args:
        df (pd.DataFrame): DataFrame with pass data

    Returns:
        pd.DataFrame: DataFrame with pass features. Added so far:
        - x: Normalized x coordinate
        - y: Normalized y coordinate
    """
    # Initialize coordinate columns
    df["x"] = None
    df["y"] = None
    df["end_x"] = None
    df["end_y"] = None
    
    # Process start locations
    for idx in df.index:
        location = df.at[idx, "location"]
        if location is not None and not (isinstance(location, (list, tuple, np.ndarray)) and len(location) == 0):
            normalized = normalize_location(location)
            if normalized != [None, None]:
                df.at[idx, "x"] = normalized[0]
                df.at[idx, "y"] = normalized[1]
    
    # Process end locations for passes
    is_pass = (df["type_name"] == "Pass") & df["pass_end_location"].notnull()
    for idx in df[is_pass].index:
        end_location = df.at[idx, "pass_end_location"]
        if end_location is not None and not (isinstance(end_location, (list, tuple, np.ndarray)) and len(end_location) == 0):
            normalized = normalize_end_location(end_location)
            if normalized != [None, None]:
                df.at[idx, "end_x"] = normalized[0]
                df.at[idx, "end_y"] = normalized[1]
    
    return df

### Column Flattening Functions ###

def flatten_columns(df):
    df.columns = [col.replace('.', '_') for col in df.columns]
    return df

### Possession Stats Functions ###

def add_possession_stats(df):
    """Add possession stats

    Args:
        df (pd.DataFrame): DataFrame with pass data

    Returns:
        pd.DataFrame: DataFrame with possession stats including:
        - possession_event_count: Number of events in each possession
        - possession_pass_count: Number of passes in each possession
        - possession_player_count: Number of unique players in each possession
        - possession_duration: Duration of each possession
        - possession_xg: Total xG in the possession
    """
    
    # Calculate event count per possession
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S.%f')
    event_count = df.groupby('possession').size().rename('possession_event_count')

    # Number of passes in each possession
    possession_pass_count = df[df['type_name'] == 'Pass'].groupby('possession').size().rename('possession_pass_count')

    # Number of unique players in each possession
    possession_player_count = df.groupby('possession')['player_id'].nunique().rename('possession_player_count')

    # Calculate possession duration
    possession_duration = df.groupby('possession')['timestamp'].max() - df.groupby('possession')['timestamp'].min()
    possession_duration = possession_duration.dt.total_seconds().rename('possession_duration')

    # Total xG in the possession
    total_xg = df[df['type_name'] == "Shot"].groupby('possession')['shot_statsbomb_xg'].sum().rename('total_xG')

    # Merge it back into the main DataFrame as a new column
    df = df.merge(event_count, left_on='possession', right_index=True, how='left')
    df = df.merge(possession_pass_count, left_on='possession', right_index=True, how='left')
    df = df.merge(possession_player_count, left_on='possession', right_index=True, how='left')
    df = df.merge(possession_duration, left_on='possession', right_index=True, how='left')
    df = df.merge(total_xg, left_on='possession', right_index=True, how='left')

    return df

def process_match_data():
    logging.info("Starting match data processing pipeline.")
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
    for parquet_file in bronze_events_dir.glob("*.parquet"):
        match_id = parquet_file.stem.replace("events_", "")
        logging.info(f"Processing match {match_id} from {parquet_file}")
        try:
            df = pd.read_parquet(parquet_file)
            logging.info(f"Loaded {len(df)} events for match {match_id}")
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S.%f')
            logging.info(f"Processed timestamps for match {match_id}")
            
            df = flatten_columns(df)
            logging.info(f"Flattened columns for match {match_id}")
            
            df = enrich_locations(df)
            logging.info(f"Enriched locations for match {match_id}")
            
            df = add_possession_stats(df)
            logging.info(f"Added possession stats for match {match_id}")
            
            pl.from_pandas(df).write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logging.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logging.warning(f"Failed to process match {match_id}: {e}")
            import traceback
            logging.warning(f"Traceback: {traceback.format_exc()}")
    logging.info("Completed match data processing pipeline.")
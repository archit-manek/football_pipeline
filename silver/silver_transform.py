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

def add_pass_length_angle(df):
    """Add length and angle of passes

    Args:
        df (pd.DataFrame): DataFrame with pass data

    Returns:
        pd.DataFrame: DataFrame with pass length and angle
    """
    # Only for passes with valid end locations
    mask = (
        (df["type_name"] == "Pass") &
        df["end_x"].notnull() & df["end_y"].notnull() &
        df["x"].notnull() & df["y"].notnull()
    )
    # Ensure numeric types and handle missing values
    for col in ["end_x", "end_y", "x", "y"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    dx = df.loc[mask, "end_x"] - df.loc[mask, "x"]
    dy = df.loc[mask, "end_y"] - df.loc[mask, "y"]
    df.loc[mask, "pass_length_calc"] = np.sqrt(dx**2 + dy**2)
    df.loc[mask, "pass_angle_calc"] = np.arctan2(dx, dy) - np.pi/2  # North is 0 degrees
    return df

def categorize_pass_direction(angle_rad, threshold=np.pi/4):
    """Categorize pass direction

    Args:
        angle_rad (float): Angle of the pass in radians (North is 0 degrees)
        threshold (float, optional): Threshold for categorization. Defaults to np.pi/4.

    Returns:
        str: Direction of the pass (forward, backward, sideways):
        - forward: Pass is forward (angle between -45 and 45 degrees, toward opponent's goal)
        - backward: Pass is backward (angle between 135 and -135 degrees, toward own goal)
        - sideways: Pass is sideways (angle between 45-135 degrees or -45 to -135 degrees, left/right)
    """
    # Convert to degrees for easier understanding
    angle_deg = np.degrees(angle_rad)
    
    # Normalize to -180 to 180 range
    while angle_deg > 180:
        angle_deg -= 360
    while angle_deg < -180:
        angle_deg += 360
    
    # Categorize based on angle ranges
    if -45 <= angle_deg <= 45:
        return "forward"  # Toward opponent's goal (north)
    elif angle_deg >= 135 or angle_deg <= -135:
        return "backward"  # Toward own goal (south)
    else:
        return "sideways"  # Left/right movement

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

def enrich_pass_features(df):
    """Enrich pass features

    Args:
        df (pd.DataFrame): DataFrame with pass data

    Returns:
        pd.DataFrame: DataFrame with pass features. Added so far:
        - pass_direction_calc: Direction of the pass (forward, backward, sideways)
    """
    # Pass length and angle calculated from pass start and end locations
    df = add_pass_length_angle(df)
    if "pass_angle_calc" in df.columns:
        # Add pass direction calculated from pass angle
        df["pass_direction_calc"] = df["pass_angle_calc"].apply(
            lambda x: categorize_pass_direction(x) if pd.notnull(x) else None
        )
    return df

def flatten_columns(df):
    df.columns = [col.replace('.', '_') for col in df.columns]
    return df

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
        - possession_ended_with_shot: Whether the possession ended with a shot
        - possession_xg: Total xG in the possession
    """
    # Check if required columns exist
    required_columns = ['possession', 'type_name', 'player_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.warning(f"Missing columns in add_possession_stats: {missing_columns}")
        logging.warning(f"Available columns: {list(df.columns)}")
        return df

    # Helper to drop column if it exists
    def drop_if_exists(df, col):
        if col in df.columns:
            df = df.drop(columns=[col])
        return df

    # Number of events in each possession
    event_count = df.groupby('possession').size().rename('possession_event_count')
    df = drop_if_exists(df, 'possession_event_count')

    # Number of passes in each possession
    pass_count = df[df['type_name'] == 'Pass'].groupby('possession').size().rename('possession_pass_count')
    df = drop_if_exists(df, 'possession_pass_count')

    # Number of unique players in each possession
    player_count = df.groupby('possession')['player_id'].nunique().rename('possession_player_count')
    df = drop_if_exists(df, 'possession_player_count')

    # Possession duration
    if 'timestamp' in df.columns:
        duration = df.groupby('possession')['timestamp'].agg(lambda x: (x.max() - x.min()).total_seconds()).rename('possession_duration')
        df = drop_if_exists(df, 'possession_duration')
    else:
        duration = None

    # Did the possession end with a shot?
    ended_with_shot = df.groupby('possession')['type_name'].apply(lambda x: (x == 'Shot').any()).rename('possession_ended_with_shot')
    df = drop_if_exists(df, 'possession_ended_with_shot')

    # Total xG in the possession (if you have an xG column)
    if 'shot_statsbomb_xg' in df.columns:
        possession_xg = df.groupby('possession')['shot_statsbomb_xg'].sum().rename('possession_xg')
        df = drop_if_exists(df, 'possession_xg')
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
    for parquet_file in islice(bronze_events_dir.glob("*.parquet"), 50):
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
            
            df = enrich_pass_features(df)
            logging.info(f"Enriched pass features for match {match_id}")
            
            df = add_possession_stats(df)
            logging.info(f"Added possession stats for match {match_id}")
            
            pl.from_pandas(df).write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logging.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logging.warning(f"Failed to process match {match_id}: {e}")
            import traceback
            logging.warning(f"Traceback: {traceback.format_exc()}")
    logging.info("Completed match data processing pipeline.")
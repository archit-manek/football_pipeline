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
        logging.FileHandler("logs/silver/event_data.log", mode="w"),
        logging.StreamHandler()
    ]
)

### Location Normalization Functions ###

def enrich_locations(df):
    df = df.with_columns([
        pl.col("location").cast(pl.Array(pl.Float64, 2)).alias("location"),
        pl.col("pass_end_location").cast(pl.Array(pl.Float64, 2)).alias("pass_end_location"),
    ])
    df = df.with_columns([
        (pl.col("location").arr.get(0) / 120).alias("x"),
        (pl.col("location").arr.get(1) / 80).alias("y"),
        pl.when(pl.col("type_name") == "Pass")
          .then(pl.col("pass_end_location").arr.get(0) / 120)
          .otherwise(None)
          .alias("end_x"),
        pl.when(pl.col("type_name") == "Pass")
          .then(pl.col("pass_end_location").arr.get(1) / 80)
          .otherwise(None)
          .alias("end_y"),
    ])
    return df

### Column Flattening Functions ###

def flatten_columns(df):
    return df.rename({col: col.replace('.', '_') for col in df.columns})

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
    event_count = df.group_by("possession").agg(
        pl.count().alias("possession_event_count")
    )

    # Number of passes in each possession
    pass_count = (
        df.filter(pl.col("type_name") == "Pass")
        .group_by("possession")
        .agg(pl.count().alias("possession_pass_count"))
    )

    # Number of unique players in each possession
    player_count = df.group_by("possession").agg(
        pl.col("player_id").n_unique().alias("possession_player_count")
    )
    
    # Possession duration (in seconds)
    duration = df.group_by("possession").agg(
        (pl.col("timestamp").max() - pl.col("timestamp").min()).dt.total_seconds().alias("possession_duration")
    )

    # Total xG per possession
    total_xg = (
        df.filter(pl.col("type_name") == "Shot")
        .group_by("possession")
        .agg(pl.col("shot_statsbomb_xg").sum().alias("total_xG"))
    )

    # Merge it back into the main DataFrame as a new column
    df = df.join(event_count, on="possession", how="left")
    df = df.join(pass_count, on="possession", how="left")
    df = df.join(player_count, on="possession", how="left")
    df = df.join(duration, on="possession", how="left")
    df = df.join(total_xg, on="possession", how="left")

    return df

def process_event_data():
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

    for parquet_file in bronze_events_dir.glob("*.parquet"):
        match_id = parquet_file.stem.replace("events_", "")
        logging.info(f"Processing match {match_id} from {parquet_file}")
        try:
            df = pl.read_parquet(parquet_file)
            logging.info(f"Loaded {len(df)} events for match {match_id}")
            
            df = df.with_columns(
                pl.col("timestamp").str.strptime(pl.Datetime, "%H:%M:%S.%f", strict=False)
            )
            logging.info(f"Processed timestamps for match {match_id}")
            
            df = flatten_columns(df)
            logging.info(f"Flattened columns for match {match_id}")
            
            df = enrich_locations(df)
            logging.info(f"Enriched locations for match {match_id}")
            
            df = add_possession_stats(df)
            logging.info(f"Added possession stats for match {match_id}")
            
            df.write_parquet(silver_events_dir / f"events_{match_id}.parquet")
            logging.info(f"Saved enriched events for match {match_id} to {silver_events_dir / f'events_{match_id}.parquet'}")
        except Exception as e:
            logging.warning(f"Failed to process match {match_id}: {e}")
            import traceback
            logging.warning(f"Traceback: {traceback.format_exc()}")
    logging.info("Completed match data processing pipeline.")
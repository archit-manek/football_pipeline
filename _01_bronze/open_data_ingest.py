import logging
from pathlib import Path
import polars as pl
from utils.constants import get_open_data_dirs, ensure_directories_exist
from utils.dataframe import is_source_newer, get_int_columns_from_schema
import json

from schemas.matches_schema import MATCHES_SCHEMA
from schemas.competitions_schema import COMPETITIONS_SCHEMA
from schemas.events_schema import EVENTS_SCHEMA
from schemas.lineups_schema import LINEUPS_SCHEMA
from schemas.schema_360 import SCHEMA_360

OPEN_DATA_DIRS = get_open_data_dirs()

# Ensure the log directory exists first
OPEN_DATA_DIRS["logs_bronze"].mkdir(parents=True, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OPEN_DATA_DIRS["logs_bronze"] / "bronze_open_data.log", mode="w"),
        logging.StreamHandler()
    ]
)

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def ingest_competitions_local():
    """
    Ingest competitions from the raw data directory into the bronze layer.
    """
    competitions_path = OPEN_DATA_DIRS["landing_competitions"] / "competitions.json"
    output_path = OPEN_DATA_DIRS["bronze_competitions"] / "competitions.parquet"
    
    if output_path.exists() and not is_source_newer(competitions_path, output_path):
        logger.info(f"{output_path} already exists and source is not newer, skipping.")
        return
    logger.info(f"Ingesting competitions from {competitions_path} to {output_path}")
    if competitions_path.exists():
        try:
            with open(competitions_path, "r") as f:
                competitions = json.load(f)
            if not competitions:
                logger.info(f"No competitions found in {competitions_path}")
                return
            df = pl.DataFrame(competitions, schema=COMPETITIONS_SCHEMA)
            df.write_parquet(output_path)
            logger.info(f"Saved competitions to {output_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Could not decode competitions JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing competitions data: {e}")
    else:
        logger.warning(f"No competitions.json found in {OPEN_DATA_DIRS['landing_competitions']}")

def ingest_matches_local():
    """
    Ingest matches from the raw data directory into the bronze layer.
    """
    matches_dir = OPEN_DATA_DIRS["landing_matches"]
    json_files = list(matches_dir.glob("*/*.json"))
    logger.info(f"Found {len(json_files)} match files to process")
    
    processed_count = 0
    skipped_count = 0
    
    for json_file in json_files:
        comp_id = json_file.parent.name
        season_id = json_file.stem
        matches_path = OPEN_DATA_DIRS["bronze_matches"] / f"matches_{comp_id}_{season_id}.parquet"
        if matches_path.exists() and not is_source_newer(json_file, matches_path):
            logger.debug(f"{matches_path} already exists and source is not newer, skipping.")
            skipped_count += 1
            continue
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if not data:
                logger.info(f"No matches found for competition {comp_id}, season {season_id}")
                continue        
            df = pl.DataFrame(data, schema=MATCHES_SCHEMA)
            df.write_parquet(matches_path)
            logger.info(f"Saved matches to {matches_path}")
            processed_count += 1
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for competition {comp_id}, season {season_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing matches for competition {comp_id}, season {season_id}: {e}")
    
    logger.info(f"Matches processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_lineups_local():
    """
    Ingest lineups from the raw data directory into the bronze layer.
    """
    lineups_dir = OPEN_DATA_DIRS["landing_lineups"]
    bronze_lineups_dir = OPEN_DATA_DIRS["bronze_lineups"]
    bronze_lineups_dir.mkdir(parents=True, exist_ok=True)
    
    json_files = list(lineups_dir.glob("*.json"))
    logger.info(f"Found {len(json_files)} lineup files to process")
    
    processed_count = 0
    skipped_count = 0
    
    for json_file in json_files:
        match_id = json_file.stem
        parquet_path = bronze_lineups_dir / f"lineups_{match_id}.parquet"
        if parquet_path.exists() and not is_source_newer(json_file, parquet_path):
            logger.debug(f"{parquet_path} already exists and source is not newer, skipping.")
            skipped_count += 1
            continue
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if not data:
                logger.info(f"No lineups found for match {match_id}")
                continue
            df = pl.DataFrame(data, schema=LINEUPS_SCHEMA)
            df.write_parquet(parquet_path)
            logger.info(f"Saved lineups for match {match_id} to {parquet_path}")    
            processed_count += 1
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing lineups for match {match_id}: {e}")
    
    logger.info(f"Lineups processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_events_local():
    """
    Ingest events from the raw data directory into the bronze layer.
    """
    events_dir = OPEN_DATA_DIRS["landing_events"]
    bronze_events_dir = OPEN_DATA_DIRS["bronze_events"]
    bronze_events_dir.mkdir(parents=True, exist_ok=True)
    
    json_files = list(events_dir.glob("*.json"))
    logger.info(f"Found {len(json_files)} event files to process")
    
    processed_count = 0
    skipped_count = 0
    
    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)  # Don't show table previews
    pl.Config.set_tbl_cols(0)  # Don't show column previews
    
    for json_file in json_files:
        match_id = json_file.stem
        events_path = bronze_events_dir / f"events_{match_id}.parquet"
        if events_path.exists() and not is_source_newer(json_file, events_path):
            skipped_count += 1
            continue
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if not data:
                logger.info(f"No events found for match {match_id}")
                continue
            df = pl.DataFrame(data, schema=EVENTS_SCHEMA)
            df.write_parquet(events_path, compression="snappy")
            processed_count += 1
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} events")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing events for match {match_id}: {e}")
    
    logger.info(f"Events processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_360_events_local():
    """
    Ingest 360 events from the raw data directory into the bronze layer.
    """
    raw_360_dir = OPEN_DATA_DIRS["landing_360_events"]
    bronze_360_dir = OPEN_DATA_DIRS["bronze_360_events"]
    bronze_360_dir.mkdir(parents=True, exist_ok=True)
    
    json_files = list(raw_360_dir.glob("*.json"))
    logger.info(f"Found {len(json_files)} 360 event files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    for json_file in json_files:
        match_id = json_file.stem
        parquet_path = bronze_360_dir / f"events_360_{match_id}.parquet"
        
        if parquet_path.exists() and not is_source_newer(json_file, parquet_path):
            skipped_count += 1
            continue
            
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if not data:
                logger.info(f"No 360 data for match {match_id}")
                continue
            df = pl.DataFrame(data, schema=SCHEMA_360)
            df.write_parquet(parquet_path, compression="snappy")
            processed_count += 1
            if processed_count % 50 == 0:
                logger.info(f"Processed {processed_count} 360 events")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id} in file {json_file}: {e}")
            error_count += 1
        except Exception as e:
            logger.warning(f"Error processing 360 data for match {match_id} in file {json_file}: {e}")
            error_count += 1
    
    logger.info(f"360 events processing complete: {processed_count} processed, {skipped_count} skipped, {error_count} errors")

def open_data_ingest():
    """
    Ingest all open-data bronze layer data from the raw data directory.
    """
    logger.info("Starting open-data bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("open-data")
    
    # ingest_competitions_local()
    # ingest_matches_local()
    # ingest_lineups_local()
    # ingest_events_local()
    ingest_360_events_local()
    
    logger.info("Open-data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
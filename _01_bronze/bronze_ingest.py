import logging
from pathlib import Path
import polars as pl
from utils.constants import *
import json
import time

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bronze/pipeline.log", mode="w"),
        logging.StreamHandler()
    ]
)

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Ensure the log directory exists
Path("logs/bronze").mkdir(parents=True, exist_ok=True)

def is_source_newer(source_path: Path, output_path: Path) -> bool:
    """Check if source file is newer than output file."""
    if not output_path.exists():
        return True
    return source_path.stat().st_mtime > output_path.stat().st_mtime

def ingest_competitions_local():
    competitions_path = Path("data/raw/competitions.json")
    output_path = BRONZE_DIR_COMPETITIONS / "competitions.parquet"
    
    if output_path.exists() and not is_source_newer(competitions_path, output_path):
        logger.debug(f"{output_path} already exists and source is not newer, skipping.")
        return
        
    if competitions_path.exists():
        try:
            with open(competitions_path, "r") as f:
                competitions = json.load(f)
            df = pl.DataFrame(competitions)
            df.write_parquet(output_path)
            logger.info(f"Saved competitions to {output_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Could not decode competitions JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing competitions data: {e}")
    else:
        logger.warning("No competitions.json found in data/raw/.")

def ingest_matches_local():
    matches_dir = Path("data/raw/matches")
    json_files = list(matches_dir.glob("*/*.json"))
    logger.info(f"Found {len(json_files)} match files to process")
    
    processed_count = 0
    skipped_count = 0
    
    for json_file in json_files:
        comp_id = json_file.parent.name
        season_id = json_file.stem
        matches_path = BRONZE_DIR_MATCHES / f"matches_{comp_id}_{season_id}.parquet"
        if matches_path.exists() and not is_source_newer(json_file, matches_path):
            logger.debug(f"{matches_path} already exists and source is not newer, skipping.")
            skipped_count += 1
            continue
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if data:
                df = pl.DataFrame(data)
                df.write_parquet(matches_path)
                logger.info(f"Saved matches to {matches_path}")
                processed_count += 1
            else:
                logger.info(f"No matches found for competition {comp_id}, season {season_id}")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for competition {comp_id}, season {season_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing matches for competition {comp_id}, season {season_id}: {e}")
    
    logger.info(f"Matches processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_lineups_local():
    lineups_dir = Path("data/raw/lineups")
    bronze_lineups_dir = BRONZE_DIR_LINEUPS
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
            all_teams = []
            for team in data:
                # Flatten lineup dicts for each team
                lineup = team["lineup"]
                for player in lineup:
                    player["match_id"] = match_id
                    player["team_id"] = team["team_id"]
                all_teams.extend(lineup)
            if all_teams:
                df = pl.DataFrame(all_teams)
                df.write_parquet(parquet_path)
                logger.info(f"Saved lineups for match {match_id} to {parquet_path}")
                processed_count += 1
            else:
                logger.info(f"No lineups found for match {match_id}")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing lineups for match {match_id}: {e}")
    
    logger.info(f"Lineups processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_events_local():
    events_dir = Path("data/raw/events")
    bronze_events_dir = BRONZE_DIR_EVENTS
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
            if data:
                df = pl.DataFrame(data)
                df.write_parquet(events_path, compression="snappy")
                processed_count += 1
                # Log only every 100 files to reduce I/O overhead
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count} event files...")
            else:
                logger.info(f"No events found for match {match_id}")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id}: {e}")
        except Exception as e:
            logger.warning(f"Error processing events for match {match_id}: {e}")
    
    logger.info(f"Events processing complete: {processed_count} processed, {skipped_count} skipped")

def ingest_360_events_local():
    raw_360_dir = Path("data/raw/three-sixty")
    bronze_360_dir = BRONZE_DIR_360_EVENTS / "events_360"
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
            if data:
                df = pl.DataFrame(data)
                df.write_parquet(parquet_path, compression="snappy")
                processed_count += 1
                # Log only every 50 files to reduce I/O overhead
                if processed_count % 50 == 0:
                    logger.info(f"Processed {processed_count} 360 event files...")
            else:
                logger.info(f"No 360 data for match {match_id}")
                error_count += 1
        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for match {match_id} in file {json_file}: {e}")
            error_count += 1
        except Exception as e:
            logger.warning(f"Error processing 360 data for match {match_id} in file {json_file}: {e}")
            error_count += 1
    
    logger.info(f"360 events processing complete: {processed_count} processed, {skipped_count} skipped, {error_count} errors")

def bronze_ingest():
    start_time = time.time()
    logger.info("Starting bronze layer ingestion...")
    
    # Time each step
    step_start = time.time()
    ingest_competitions_local()
    competitions_time = time.time() - step_start
    logger.info(f"Competitions processing completed in {competitions_time:.2f} seconds")
    
    step_start = time.time()
    ingest_matches_local()
    matches_time = time.time() - step_start
    logger.info(f"Matches processing completed in {matches_time:.2f} seconds")
    
    step_start = time.time()
    ingest_lineups_local()
    lineups_time = time.time() - step_start
    logger.info(f"Lineups processing completed in {lineups_time:.2f} seconds")
    
    step_start = time.time()
    ingest_events_local()
    events_time = time.time() - step_start
    logger.info(f"Events processing completed in {events_time:.2f} seconds")
    
    step_start = time.time()
    ingest_360_events_local()
    events_360_time = time.time() - step_start
    logger.info(f"360 events processing completed in {events_360_time:.2f} seconds")
    
    total_time = time.time() - start_time
    logger.info(f"Bronze layer ingestion completed in {total_time:.2f} seconds total!")
    
    # Log detailed timing breakdown
    logger.info("=== PERFORMANCE BREAKDOWN ===")
    logger.info(f"Competitions: {competitions_time:.2f}s")
    logger.info(f"Matches: {matches_time:.2f}s")
    logger.info(f"Lineups: {lineups_time:.2f}s")
    logger.info(f"Events: {events_time:.2f}s")
    logger.info(f"360 Events: {events_360_time:.2f}s")
    logger.info(f"Total: {total_time:.2f}s")
    logger.info("=============================")
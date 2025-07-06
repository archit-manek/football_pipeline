import logging
from pathlib import Path
import polars as pl
from utils.config import *
import json

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bronze/pipeline.log", mode="w"),
        logging.StreamHandler()
    ]
)

def ingest_competitions_local():
    competitions_path = Path("data/raw/competitions.json")
    if competitions_path.exists():
        with open(competitions_path, "r") as f:
            competitions = json.load(f)
        df = pl.DataFrame(competitions)
        df.write_parquet(BRONZE_DIR_COMPETITIONS / "competitions.parquet")
        logging.info(f"Saved competitions to {BRONZE_DIR_COMPETITIONS / 'competitions.parquet'}")
    else:
        logging.warning("No competitions.json found in data/raw/.")

def ingest_matches_local():
    matches_dir = Path("data/raw/matches")
    for json_file in matches_dir.glob("*/*.json"):
        comp_id = json_file.parent.name
        season_id = json_file.stem
        matches_path = BRONZE_DIR_MATCHES / f"matches_{comp_id}_{season_id}.parquet"
        if matches_path.exists():
            logging.debug(f"{matches_path} already exists, skipping.")
            continue
        with open(json_file, "r") as f:
            data = json.load(f)
        if data:
            df = pl.DataFrame(data)
            df.write_parquet(matches_path)
            logging.info(f"Saved matches to {matches_path}")
        else:
            logging.info(f"No matches found for competition {comp_id}, season {season_id}")

def ingest_lineups_local():
    lineups_dir = Path("data/raw/lineups")
    bronze_lineups_dir = BRONZE_DIR_LINEUPS
    bronze_lineups_dir.mkdir(parents=True, exist_ok=True)
    for json_file in lineups_dir.glob("*.json"):
        match_id = json_file.stem
        parquet_path = bronze_lineups_dir / f"lineups_{match_id}.parquet"
        if parquet_path.exists():
            logging.debug(f"{parquet_path} already exists, skipping.")
            continue
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
            logging.info(f"Saved lineups for match {match_id} to {parquet_path}")
        else:
            logging.info(f"No lineups found for match {match_id}")

def ingest_events_local():
    events_dir = Path("data/raw/events")
    bronze_events_dir = BRONZE_DIR_EVENTS
    bronze_events_dir.mkdir(parents=True, exist_ok=True)
    for json_file in events_dir.glob("*.json"):
        match_id = json_file.stem
        events_path = bronze_events_dir / f"events_{match_id}.parquet"
        if events_path.exists():
            logging.debug(f"File {events_path} already exists, skipping.")
            continue
        with open(json_file, "r") as f:
            data = json.load(f)
        if data:
            df = pl.DataFrame(data)
            df.write_parquet(events_path)
            logging.info(f"Saved events for match {match_id} to {events_path}")
        else:
            logging.info(f"No events found for match {match_id}")

def ingest_360_events_local():
    raw_360_dir = Path("data/raw/three-sixty")
    bronze_360_dir = BRONZE_DIR_360_EVENTS / "events_360"
    bronze_360_dir.mkdir(parents=True, exist_ok=True)
    for json_file in raw_360_dir.glob("*.json"):
        match_id = json_file.stem
        parquet_path = bronze_360_dir / f"events_360_{match_id}.parquet"
        if parquet_path.exists():
            logging.info(f"{parquet_path} already exists, overwriting.")
        else:
            logging.info(f"Creating new parquet for {match_id} at {parquet_path}")
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
            if data:
                df = pl.DataFrame(data)
                df.write_parquet(parquet_path)
                logging.info(f"Saved 360 events for match {match_id} to {parquet_path}")
            else:
                logging.info(f"No 360 data for match {match_id}")
        except json.JSONDecodeError as e:
            logging.warning(f"Could not decode JSON for match {match_id} in file {json_file}: {e}")
        except Exception as e:
            logging.warning(f"Error processing 360 data for match {match_id} in file {json_file}: {e}")

def bronze_ingest():
    ingest_competitions_local()
    ingest_matches_local()
    ingest_lineups_local()
    ingest_events_local()
    ingest_360_events_local()
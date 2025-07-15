# filepath: utils/config.py
from pathlib import Path

# Absolute path to the project root
ABS_PATH = Path("/Users/architmanek/Desktop/DataEngineering/football_pipeline")

# Define directories (RAW)
RAW_DIR = Path("data/raw")
RAW_DIR_MATCHES = RAW_DIR / "matches"
RAW_DIR_COMPETITIONS = RAW_DIR / "competitions"
RAW_DIR_LINEUPS = RAW_DIR / "lineups"
RAW_DIR_EVENTS = RAW_DIR / "events"
RAW_DIR_360_EVENTS = RAW_DIR / "three-sixty"

# Ensure directories exist (RAW)
RAW_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR_MATCHES.mkdir(parents=True, exist_ok=True)
RAW_DIR_COMPETITIONS.mkdir(parents=True, exist_ok=True)
RAW_DIR_LINEUPS.mkdir(parents=True, exist_ok=True)
RAW_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
RAW_DIR_360_EVENTS.mkdir(parents=True, exist_ok=True)

# Define directories (BRONZE)
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR_MATCHES = BRONZE_DIR / "matches"
BRONZE_DIR_COMPETITIONS = BRONZE_DIR / "competitions"
BRONZE_DIR_LINEUPS = BRONZE_DIR / "lineups"
BRONZE_DIR_EVENTS = BRONZE_DIR / "events"
BRONZE_DIR_360_EVENTS = BRONZE_DIR / "360_events" / "events_360"

# Ensure directories exist (BRONZE)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_MATCHES.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_COMPETITIONS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_LINEUPS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_360_EVENTS.mkdir(parents=True, exist_ok=True)

# Silver logs paths
SILVER_LOGS_PATH = Path("logs/silver")
SILVER_LOGS_PATH.mkdir(parents=True, exist_ok=True)
SILVER_LOGS_MATCHES_PATH = SILVER_LOGS_PATH / "matches.log"
SILVER_LOGS_COMPETITIONS_PATH = SILVER_LOGS_PATH / "competitions.log"
SILVER_LOGS_LINEUPS_PATH = SILVER_LOGS_PATH / "lineups.log"
SILVER_LOGS_EVENTS_PATH = SILVER_LOGS_PATH / "events.log"
SILVER_LOGS_360_EVENTS_PATH = SILVER_LOGS_PATH / "360_events.log" #logs/silver/360_events.log

# Define directories (SILVER)
SILVER_DIR = Path("data/silver")
SILVER_DIR_EVENTS = SILVER_DIR / "events"
SILVER_DIR_COMPETITIONS = SILVER_DIR / "competitions"
SILVER_DIR_LINEUPS = SILVER_DIR / "lineups"
SILVER_DIR_MATCHES = SILVER_DIR / "matches"
SILVER_DIR_360_EVENTS = SILVER_DIR / "360_events" / "events_360"

# Ensure directories exist (SILVER)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
SILVER_DIR_COMPETITIONS.mkdir(parents=True, exist_ok=True)
SILVER_DIR_LINEUPS.mkdir(parents=True, exist_ok=True)
SILVER_DIR_MATCHES.mkdir(parents=True, exist_ok=True)
SILVER_DIR_360_EVENTS.mkdir(parents=True, exist_ok=True)

# Define directories (GOLD)
GOLD_DIR = Path("data/gold")

# Ensure directories exist (GOLD)
GOLD_DIR.mkdir(parents=True, exist_ok=True)
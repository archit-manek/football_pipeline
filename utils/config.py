# filepath: utils/config.py
from pathlib import Path

# Define directories for the bronze layer
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR_MATCHES = BRONZE_DIR / "matches"
BRONZE_DIR_COMPETITIONS = BRONZE_DIR / "competitions"
BRONZE_DIR_LINEUPS = BRONZE_DIR / "lineups"
BRONZE_DIR_EVENTS = BRONZE_DIR / "events"
BRONZE_DIR_360_EVENTS = BRONZE_DIR / "360_events"

# Ensure directories exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_MATCHES.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_COMPETITIONS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_LINEUPS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_360_EVENTS.mkdir(parents=True, exist_ok=True)
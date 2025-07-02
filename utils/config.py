# filepath: utils/config.py
from pathlib import Path

# Define directories (BRONZE)
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR_MATCHES = BRONZE_DIR / "matches"
BRONZE_DIR_COMPETITIONS = BRONZE_DIR / "competitions"
BRONZE_DIR_LINEUPS = BRONZE_DIR / "lineups"
BRONZE_DIR_EVENTS = BRONZE_DIR / "events"
BRONZE_DIR_360_EVENTS = BRONZE_DIR / "360_events"

# Define directories (SILVER)
SILVER_DIR = Path("data/silver")
SILVER_DIR_EVENTS = SILVER_DIR / "events"

# Ensure directories exist (BRONZE)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_MATCHES.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_COMPETITIONS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_LINEUPS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
BRONZE_DIR_360_EVENTS.mkdir(parents=True, exist_ok=True)

# Ensure directories exist (SILVER)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR_EVENTS.mkdir(parents=True, exist_ok=True)
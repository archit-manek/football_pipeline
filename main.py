import os
from _01_bronze.bronze_ingest import bronze_ingest
from _02_silver.events_data import process_events_data
from _02_silver.competitions_data import process_competitions_data
from _02_silver.lineups_data import process_lineups_data
from _02_silver.matches_data import process_matches_data
from _02_silver._360_events import process_360_events_data
from _03_gold.xg_model import build_xg_model

# Set up logging
os.makedirs("logs/bronze", exist_ok=True)
os.makedirs("logs/silver", exist_ok=True)

if __name__ == "__main__":
    # BRONZE STAGE
    # bronze_ingest()

    # SILVER STAGE
    process_events_data()
    process_competitions_data()
    process_lineups_data()
    process_matches_data()
    process_360_events_data()

    # GOLD STAGE
    # aggregate_match_data()
    
    # Build xG model
    
    # build_xg_model()  # Uncomment to build full model
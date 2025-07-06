import os
from bronze.bronze_ingest import bronze_ingest
from silver.event_data import process_event_data
from silver.competitions_data import process_competitions_data

# Set up logging
os.makedirs("logs/bronze", exist_ok=True)
os.makedirs("logs/silver", exist_ok=True)

if __name__ == "__main__":
    # bronze_ingest()
    # process_event_data()
    process_competitions_data()
    # aggregate_match_data()
import os
from bronze.bronze_ingest import bronze_ingest
from silver.silver_transform import process_match_data
from statsbombpy import sb

# Set up logging
os.makedirs("logs/bronze", exist_ok=True)
os.makedirs("logs/silver", exist_ok=True)

if __name__ == "__main__":
    # bronze_ingest()
    process_match_data()
    # gold_transform()
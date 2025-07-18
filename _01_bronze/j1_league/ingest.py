import json
import polars as pl

from utils.constants import get_j1_league_dirs, ensure_directories_exist
from utils.dataframe import is_source_newer, create_dataframe_safely, apply_schema_flexibly
from utils.logging import setup_logger

# Import J1 League schemas
from schemas.j1_league.matches_schema import J1_LEAGUE_MATCHES_SCHEMA
from schemas.j1_league.events_schema import J1_LEAGUE_EVENTS_SCHEMA
from schemas.j1_league.physical_schema import J1_LEAGUE_PHYSICAL_SCHEMA
from schemas.j1_league.mappings_schema import *

# Get J1 League directories
J1_LEAGUE_DIRS = get_j1_league_dirs()

# Ensure the log directory exists first
J1_LEAGUE_DIRS["logs_bronze"].mkdir(parents=True, exist_ok=True)

# Set up logging
log_path = J1_LEAGUE_DIRS["logs_bronze"] / "j1_league_bronze.log"
logger = setup_logger(log_path, "j1_league_bronze")

def create_events_dataframe_safely(data, target_schema: dict) -> pl.DataFrame:
    """
    Create a DataFrame from events JSON data safely, handling mixed types.
    This is a specialized version for j1_league events data.
    """
    try:
        # Force all columns to be strings initially to avoid mixed type issues
        df = pl.DataFrame(data, infer_schema_length=0)
        logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        
        # Apply schema flexibly
        df = apply_schema_flexibly(df, target_schema)
        
        return df
    except Exception as e:
        logger.error(f"Failed to create events DataFrame: {e}")
        raise

def ingest_j1_league_matches():
    """Ingest J1 League matches data from StatsBomb format."""
    logger.info("Starting J1 League matches ingestion...")
    
    matches_file = J1_LEAGUE_DIRS["landing_sb_matches"] / "sb_matches.json"
    output_path = J1_LEAGUE_DIRS["bronze_matches"] / "sb_matches.parquet"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists() and not is_source_newer(matches_file, output_path):
        logger.info(f"Matches file {output_path} is up to date, skipping.")
        return
    
    if not matches_file.exists():
        logger.warning(f"Matches file {matches_file} not found, skipping.")
        return
    
    try:
        logger.info(f"Processing matches from {matches_file}")
        
        with open(matches_file, "r") as f:
            matches_data = json.load(f)
        
        if not matches_data:
            logger.warning("No matches data found in file")
            return
        
        # Create DataFrame with safe schema application
        df = create_dataframe_safely(matches_data, J1_LEAGUE_MATCHES_SCHEMA)
        
        # Write to parquet
        df.write_parquet(output_path, compression="snappy")
        
        logger.info(f"Successfully processed {len(df)} matches to {output_path}")
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in matches file: {e}")
    except Exception as e:
        logger.error(f"Error processing matches data: {e}")

def ingest_j1_league_events():
    """Ingest J1 League events data from StatsBomb format."""
    logger.info("Starting J1 League events ingestion...")
    
    events_file = J1_LEAGUE_DIRS["landing_sb_events"] / "sb_events.json"
    output_path = J1_LEAGUE_DIRS["bronze_events"] / "sb_events.parquet"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists() and not is_source_newer(events_file, output_path):
        logger.info(f"Events file {output_path} is up to date, skipping.")
        return
    
    if not events_file.exists():
        logger.warning(f"Events file {events_file} not found, skipping.")
        return
    
    # NOTE: Events data processing is currently skipped due to complex schema issues
    # The events data has highly variable schema across different events with mixed types
    # This requires specialized handling that will be implemented in a future iteration
    logger.warning("Events data processing is currently skipped due to complex schema issues")
    logger.warning("Events data has highly variable schema with mixed types that requires specialized handling")
    return
    
def ingest_j1_league_physical():
    """Ingest J1 League physical data from HUDL format."""
    logger.info("Starting J1 League physical data ingestion...")
    
    physical_file = J1_LEAGUE_DIRS["landing_hudl_physical"] / "hudl_physical.json"
    output_path = J1_LEAGUE_DIRS["bronze_physical"] / "hudl_physical.parquet"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_path.exists() and not is_source_newer(physical_file, output_path):
        logger.info(f"Physical file {output_path} is up to date, skipping.")
        return
    
    if not physical_file.exists():
        logger.warning(f"Physical file {physical_file} not found, skipping.")
        return
    
    try:
        logger.info(f"Processing physical data from {physical_file}")
        
        with open(physical_file, "r") as f:
            physical_data = json.load(f)
        
        if not physical_data:
            logger.warning("No physical data found in file")
            return
        
        # Create DataFrame with safe schema application
        df = create_dataframe_safely(physical_data, J1_LEAGUE_PHYSICAL_SCHEMA)
        
        # Write to parquet
        df.write_parquet(output_path, compression="snappy")
        
        logger.info(f"Successfully processed {len(df)} physical records to {output_path}")
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in physical file: {e}")
    except Exception as e:
        logger.error(f"Error processing physical data: {e}")

def ingest_j1_league_mappings():
    """Ingest J1 League mapping data from CSV files."""
    logger.info("Starting J1 League mappings ingestion...")
    
    mappings_dir = J1_LEAGUE_DIRS["landing_mappings"]
    output_dir = J1_LEAGUE_DIRS["bronze_mappings"]
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define mapping files and their schemas
    mapping_files = {
        "matches_mapping.csv": J1_LEAGUE_MATCHES_MAPPING_SCHEMA,
        "teams_mapping.csv": J1_LEAGUE_TEAMS_MAPPING_SCHEMA,
        "players_mapping.csv": J1_LEAGUE_PLAYERS_MAPPING_SCHEMA
    }
    
    for filename, schema in mapping_files.items():
        csv_file = mappings_dir / filename
        output_file = output_dir / filename.replace('.csv', '.parquet')
        
        if output_file.exists() and not is_source_newer(csv_file, output_file):
            logger.info(f"Mapping file {output_file} is up to date, skipping.")
            continue
        
        if not csv_file.exists():
            logger.warning(f"Mapping file {csv_file} not found, skipping.")
            continue
        
        try:
            logger.info(f"Processing mapping file {csv_file}")
            
            # Read CSV file
            df = pl.read_csv(csv_file)
            
            # Apply schema flexibly
            df = apply_schema_flexibly(df, schema)
            
            # Write to parquet
            df.write_parquet(output_file, compression="snappy")
            
            logger.info(f"Successfully processed {len(df)} mapping records to {output_file}")
            
        except Exception as e:
            logger.error(f"Error processing mapping file {csv_file}: {e}")

def j1_league_ingest():
    """
    Main function to ingest all J1 League bronze layer data.
    """
    logger.info("Starting J1 League bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("j1_league")
    
    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)
    
    try:
        # Ingest all data types
        ingest_j1_league_matches()
        ingest_j1_league_events()
        ingest_j1_league_physical()
        ingest_j1_league_mappings()
        
        logger.info("J1 League bronze layer ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during J1 League bronze layer ingestion: {e}")
        raise

if __name__ == "__main__":
    j1_league_ingest() 
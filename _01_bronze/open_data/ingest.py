import logging
from pathlib import Path
import polars as pl
import pandas as pd
from utils.constants import get_open_data_dirs, ensure_directories_exist
from utils.dataframe import is_source_newer
import json
from typing import Dict, Any, Callable, Optional

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

def ingest_data_generic(
    data_type: str,
    landing_dir: Path,
    bronze_dir: Path,
    file_pattern: str = "*.json",
    output_prefix: str = "",
    log_frequency: int = 50,
    custom_loader: Optional[Callable] = None
):
    """
    Simple bronze layer ingestion: JSON -> Parquet with minimal processing.
    
    Args:
        data_type: Name of data type for logging
        landing_dir: Directory containing source JSON files
        bronze_dir: Directory to write parquet files
        file_pattern: Glob pattern for finding files
        output_prefix: Prefix for output filenames
        log_frequency: How often to log progress
        custom_loader: Optional custom function to load and parse JSON
    """
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    json_files = list(landing_dir.glob(file_pattern))
    logger.info(f"Found {len(json_files)} {data_type} files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    for json_file in json_files:
        try:
            # Generate output filename
            if output_prefix:
                if data_type == "matches":
                    # Special case for matches: comp_id_season_id
                    comp_id = json_file.parent.name
                    season_id = json_file.stem
                    output_filename = f"{output_prefix}_{comp_id}_{season_id}.parquet"
                else:
                    # Standard case: prefix_stem
                    output_filename = f"{output_prefix}_{json_file.stem}.parquet"
            else:
                output_filename = f"{json_file.stem}.parquet"
            
            output_path = bronze_dir / output_filename
            
            # Check if source is newer
            if output_path.exists() and not is_source_newer(json_file, output_path):
                skipped_count += 1
                continue
            
            # Load JSON data
            if custom_loader:
                data = custom_loader(json_file)
            else:
                with open(json_file, "r") as f:
                    data = json.load(f)
            
            if not data:
                logger.info(f"No {data_type} found in {json_file.name}")
                continue
            
            # Bronze layer: Simple JSON flattening and Parquet conversion
            try:
                # Use pandas for JSON normalization (handles nested structures well)
                df_pd = pd.json_normalize(data)
                # Convert to Polars for efficient Parquet writing
                df = pl.from_pandas(df_pd)
                # Basic column name standardization (dots to underscores)
                df = df.rename({col: col.replace('.', '_') for col in df.columns})
            except Exception as e:
                # Fallback: Direct Polars processing
                logger.warning(f"Pandas normalization failed for {json_file.name}: {e} | Using Polars fallback")
                df = pl.DataFrame(data, infer_schema_length=0)  # Let Polars infer basic types
                df = df.rename({col: col.replace('.', '_') for col in df.columns})
            
            # Write to Parquet (Bronze layer stores data as-is, minimal processing)
            df.write_parquet(output_path, compression="snappy")
            
            processed_count += 1
            if processed_count % log_frequency == 0:
                logger.info(f"Processed {processed_count} bronze {data_type}")

        except json.JSONDecodeError as e:
            logger.warning(f"Could not decode JSON for {data_type} in {json_file}: {e}")
            error_count += 1
            raise
        except Exception as e:
            logger.warning(f"Error processing {data_type} in {json_file}: {e}")
            error_count += 1
            raise
    # Summary
    summary_msg = f"{data_type.title()} processing complete: {processed_count} processed, {skipped_count} skipped"
    if error_count > 0:
        summary_msg += f", {error_count} errors"
    logger.info(summary_msg)
    
    return processed_count, skipped_count, error_count

def ingest_competitions_local():
    """Ingest competitions from the raw data directory into the bronze layer."""
    competitions_path = OPEN_DATA_DIRS["landing_competitions"] / "competitions.json"
    output_path = OPEN_DATA_DIRS["bronze_competitions"] / "competitions.parquet"
    
    if output_path.exists() and not is_source_newer(competitions_path, output_path):
        logger.info(f"{output_path} already exists and source is not newer, skipping.")
        return
    
    logger.info(f"Ingesting competitions from {competitions_path} to {output_path}")
    if not competitions_path.exists():
        logger.warning(f"No competitions.json found in {OPEN_DATA_DIRS['landing_competitions']}")
        return
    
    try:
        with open(competitions_path, "r") as f:
            competitions = json.load(f)
        if not competitions:
            logger.info(f"No competitions found in {competitions_path}")
            return
        
        # Bronze layer: Simple processing, no complex schema validation
        df_pd = pd.json_normalize(competitions)
        df = pl.from_pandas(df_pd)
        # Basic column standardization
        df = df.rename({col: col.replace('.', '_') for col in df.columns})
        
        df.write_parquet(output_path, compression="snappy")
        logger.info(f"Saved {len(df)} competitions to {output_path}")
        
    except json.JSONDecodeError as e:
        logger.error(f"Could not decode competitions JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing competitions data: {e}")
        raise

def ingest_matches_local():
    """Ingest matches from the raw data directory into the bronze layer."""
    ingest_data_generic(
        data_type="matches",
        landing_dir=OPEN_DATA_DIRS["landing_matches"],
        bronze_dir=OPEN_DATA_DIRS["bronze_matches"],
        file_pattern="*/*.json",  # matches are in subdirectories
        output_prefix="matches",
        log_frequency=5
    )

def ingest_lineups_local():
    """Ingest lineups from the raw data directory into the bronze layer."""
    ingest_data_generic(
        data_type="lineups",
        landing_dir=OPEN_DATA_DIRS["landing_lineups"],
        bronze_dir=OPEN_DATA_DIRS["bronze_lineups"],
        output_prefix="lineups",
        log_frequency=10
    )

def ingest_events_local():
    """Ingest events from the raw data directory into the bronze layer."""
    ingest_data_generic(
        data_type="events",
        landing_dir=OPEN_DATA_DIRS["landing_events"],
        bronze_dir=OPEN_DATA_DIRS["bronze_events"],
        output_prefix="events",
        log_frequency=50
    )

def ingest_360_events_local():
    """Ingest 360 events from the raw data directory into the bronze layer."""
    ingest_data_generic(
        data_type="360 events",
        landing_dir=OPEN_DATA_DIRS["landing_360_events"],
        bronze_dir=OPEN_DATA_DIRS["bronze_360_events"],
        output_prefix="events_360",
        log_frequency=50
    )

def open_data_ingest():
    """
    Ingest all open_data bronze layer data from the raw data directory.
    """
    logger.info("Starting open_data bronze layer ingestion...")
    
    # Ensure all necessary directories exist
    ensure_directories_exist("open_data")
    
    ingest_competitions_local()
    ingest_matches_local()
    ingest_lineups_local()
    ingest_events_local()
    ingest_360_events_local()
    
    logger.info("Open_data bronze layer ingestion complete!")

if __name__ == "__main__":
    open_data_ingest()
import logging
import polars as pl
from pathlib import Path

from utils.constants import BRONZE_DIR_COMPETITIONS, SILVER_DIR_COMPETITIONS, SILVER_LOGS_COMPETITIONS_PATH
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, get_int_columns_from_schema, fix_int_columns_with_nans, is_source_newer
from schemas.competitions_schema import competitions_schema
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_COMPETITIONS_PATH)
logger = setup_logger(log_path, "competitions")



###
# Process competitions data from bronze to silver layer
###
def process_competitions_data():
    """
    Process competitions data to create silver layer.
    """
    logger.info("Starting silver competitions processing pipeline...")
    
    bronze_comp_path = Path(BRONZE_DIR_COMPETITIONS) / "competitions.parquet"
    silver_comp_path = Path(SILVER_DIR_COMPETITIONS) / "competitions.parquet"

    # Ensure the silver directory exists
    Path(SILVER_DIR_COMPETITIONS).mkdir(parents=True, exist_ok=True)

    # Check if source is newer than output
    if silver_comp_path.exists() and not is_source_newer(bronze_comp_path, silver_comp_path):
        logger.info("Silver competitions file is up to date, skipping processing")
        return

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    try:
        df = pl.read_parquet(bronze_comp_path)
        competitions_count = len(df)

        # Flatten columns (may not be needed for competitions but kept for consistency)
        df = flatten_columns(df)
        
        # Add missing columns
        expected_cols = set(competitions_schema.columns.keys())
        df = add_missing_columns(df, expected_cols)
        
        # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
        int_cols = get_int_columns_from_schema(competitions_schema)
        df = fix_int_columns_with_nans(df, int_cols)
        
        # Cast columns to schema types (excluding datetime columns we'll handle separately)
        df = cast_columns_to_schema_types(df, competitions_schema)
        
        # Ensure correct dtypes for DateTime columns
        df = df.with_columns([
            pl.col("match_updated").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
            pl.col("match_available").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
        ])

        # Validate schema
        log_schema_differences(df, competitions_schema, logger, bronze_comp_path)

        df.write_parquet(silver_comp_path, compression="snappy")

        logger.info("=== SILVER COMPETITIONS PROCESSING SUMMARY ===")
        logger.info(f"Files processed: 1")
        logger.info(f"Files skipped: 0")
        logger.info(f"Files with errors: 0")
        logger.info(f"Total competitions processed: {competitions_count:,}")
        logger.info("===============================================")
        
    except Exception as e:
        logger.warning(f"Failed to process competitions data: {e}")
        logger.info("=== SILVER COMPETITIONS PROCESSING SUMMARY ===")
        logger.info(f"Files processed: 0")
        logger.info(f"Files skipped: 0")
        logger.info(f"Files with errors: 1")
        logger.info("===============================================")
        raise
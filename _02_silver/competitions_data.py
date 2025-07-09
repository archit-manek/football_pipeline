import logging
import polars as pl
from pathlib import Path

from utils.constants import BRONZE_DIR_COMPETITIONS, SILVER_DIR_COMPETITIONS, SILVER_LOGS_COMPETITIONS_PATH
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, get_int_columns_from_schema, fix_int_columns_with_nans
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
    bronze_comp_path = Path(BRONZE_DIR_COMPETITIONS) / "competitions.parquet"
    silver_comp_path = Path(SILVER_DIR_COMPETITIONS) / "competitions.parquet"

    logger.info("Starting competitions data processing pipeline.")

    # Ensure the silver directory exists
    Path(SILVER_DIR_COMPETITIONS).mkdir(parents=True, exist_ok=True)

    # Delete the old file if it exists
    if silver_comp_path.exists():
        try:
            silver_comp_path.unlink()
            logger.info(f"Deleted old file: {silver_comp_path}")
        except Exception as e:
            logger.warning(f"Could not delete file {silver_comp_path}: {e}")

    logger.info(f"Reading {bronze_comp_path}")
    df = pl.read_parquet(bronze_comp_path)
    logger.info(f"Loaded {len(df)} competitions records")

    try:
        # Flatten columns (may not be needed for competitions but kept for consistency)
        df = flatten_columns(df)
        logger.info("Flattened columns for competitions")
        
        # Add missing columns
        expected_cols = set(competitions_schema.columns.keys())
        df = add_missing_columns(df, expected_cols)
        logger.info("Added missing columns for competitions")
        
        # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
        int_cols = get_int_columns_from_schema(competitions_schema)
        df = fix_int_columns_with_nans(df, int_cols)
        logger.info(f"Fixed {len(int_cols)} integer columns to preserve Int64 type for competitions")
        
        # Cast columns to schema types (excluding datetime columns we'll handle separately)
        df = cast_columns_to_schema_types(df, competitions_schema)
        logger.info("Cast columns to schema types for competitions")
        
        # Ensure correct dtypes for DateTime columns
        df = df.with_columns([
            pl.col("match_updated").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
            pl.col("match_available").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
        ])
        logger.info("Processed datetime columns for competitions")

        # Validate schema
        log_schema_differences(df, competitions_schema, logger, bronze_comp_path)
        logger.info("Schema validation skipped for competitions (validation disabled)")

        # Transformations go here

        df.write_parquet(silver_comp_path)
        logger.info(f"Saved cleaned competitions to {silver_comp_path}")

        logger.info("Competitions data processing completed.")
        
    except Exception as e:
        logger.error(f"Failed to process competitions data: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
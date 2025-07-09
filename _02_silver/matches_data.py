import logging
from pathlib import Path
import polars as pl
import pandera as pa
from schemas.matches_schema import matches_schema
from utils.constants import BRONZE_DIR_MATCHES, SILVER_DIR_MATCHES, SILVER_LOGS_MATCHES_PATH
from utils.dataframe import flatten_columns, log_schema_differences, validate_polars_with_pandera, add_missing_columns, cast_columns_to_schema_types, get_int_columns_from_schema, fix_int_columns_with_nans
from utils.logging import setup_logger

def process_matches_data():
    log_path = Path(SILVER_LOGS_MATCHES_PATH)
    logger = setup_logger(log_path, "matches")
    logger.info("Processing matches data...")

    bronze_matches_dir = Path(BRONZE_DIR_MATCHES)
    silver_matches_dir = Path(SILVER_DIR_MATCHES)

    # Ensure the silver directory exists
    Path(SILVER_DIR_MATCHES).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_MATCHES).glob("*.parquet"):
        try:
            file.unlink()
            logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_matches_dir.glob("*.parquet"):
        match_file = parquet_file.stem
        logger.info(f"Processing matches from {parquet_file}")
        
        try:
            df = pl.read_parquet(parquet_file)
            logger.info(f"Loaded {len(df)} matches from {match_file}")

            ### Transformations can be added here
            # Flatten columns
            df = flatten_columns(df)
            logger.info(f"Flattened columns for {match_file}")
            
            # Add missing columns
            expected_cols = set(matches_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            logger.info(f"Added missing columns for {match_file}")

            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(matches_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            logger.info(f"Fixed {len(int_cols)} integer columns to preserve Int64 type for {match_file}")

            # Cast to schema types
            df = cast_columns_to_schema_types(df, matches_schema)
            logger.info(f"Cast columns to schema types for {match_file}")

            # Validate the DataFrame against the schema
            log_schema_differences(df, matches_schema, logger, parquet_file)
            logger.info(f"Schema validation skipped for {match_file} (validation disabled)")

            df.write_parquet(silver_matches_dir / parquet_file.name)
            logger.info(f"Saved processed matches for {match_file} to {silver_matches_dir / parquet_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to process matches file {parquet_file}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            continue

    logger.info("Matches data processed successfully.")
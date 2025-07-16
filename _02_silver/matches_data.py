import polars as pl
from pathlib import Path

from utils.constants import BRONZE_DIR_MATCHES, SILVER_DIR_MATCHES, SILVER_LOGS_MATCHES_PATH
from utils.dataframe import flatten_columns, add_missing_columns, cast_columns_to_schema_types, log_schema_differences, get_int_columns_from_schema, fix_int_columns_with_nans, is_source_newer
from schemas.matches_schema import matches_schema
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_MATCHES_PATH)
logger = setup_logger(log_path, "matches")


###
# Process matches data from bronze to silver layer
###
def process_matches_data():
    logger.info("Starting silver matches processing pipeline...")
    
    bronze_matches_dir = Path(BRONZE_DIR_MATCHES)
    silver_matches_dir = Path(SILVER_DIR_MATCHES)

    # Ensure silver directory exists
    silver_matches_dir.mkdir(parents=True, exist_ok=True)

    # Get all bronze files
    bronze_files = list(bronze_matches_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze match files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_matches = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        match_file = parquet_file.stem
        silver_path = silver_matches_dir / parquet_file.name
        
        # Check if source is newer than output
        if silver_path.exists() and not is_source_newer(parquet_file, silver_path):
            skipped_count += 1
            continue
        
        try:
            df = pl.read_parquet(parquet_file)
            matches_count = len(df)
            total_matches += matches_count

            # Flatten columns
            df = flatten_columns(df)
            
            # Add missing columns
            expected_cols = set(matches_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)

            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(matches_schema)
            df = fix_int_columns_with_nans(df, int_cols)

            # Cast to schema types
            df = cast_columns_to_schema_types(df, matches_schema)

            # Validate the DataFrame against the schema
            log_schema_differences(df, matches_schema, logger, parquet_file)

            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 20 files (fewer match files than events)
            if processed_count % 20 == 0:
                logger.info(f"Processed {processed_count} match files...")
            
        except Exception as e:
            logger.warning(f"Failed to process matches file {match_file}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info("=== SILVER MATCHES PROCESSING SUMMARY ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total matches processed: {total_matches:,}")
    if processed_count > 0:
        logger.info(f"Average matches per file: {total_matches // processed_count:,}")
    logger.info("==========================================")
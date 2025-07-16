from pathlib import Path
import polars as pl
from schemas.lineups_schema import lineups_schema
from utils.constants import BRONZE_DIR_LINEUPS, SILVER_DIR_LINEUPS, SILVER_LOGS_LINEUPS_PATH
from utils.dataframe import flatten_columns, log_schema_differences, fix_int_columns_with_nans, add_missing_columns, cast_columns_to_schema_types, get_int_columns_from_schema, is_source_newer
from utils.logging import setup_logger

log_path = Path(SILVER_LOGS_LINEUPS_PATH)
logger = setup_logger(log_path, "lineups")



###
# Process lineups data from the bronze layer to the silver layer.
###
def process_lineups_data():
    logger.info("Starting silver lineups processing pipeline...")
    
    bronze_lineups_dir = Path(BRONZE_DIR_LINEUPS)
    silver_lineups_dir = Path(SILVER_DIR_LINEUPS)

    # Ensure silver directory exists
    silver_lineups_dir.mkdir(parents=True, exist_ok=True)

    # Get all bronze files
    bronze_files = list(bronze_lineups_dir.glob("*.parquet"))
    logger.info(f"Found {len(bronze_files)} bronze lineup files to process")
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    total_lineups = 0

    # Configure Polars for better performance
    pl.Config.set_tbl_rows(0)
    pl.Config.set_tbl_cols(0)

    for parquet_file in bronze_files:
        lineup_file = parquet_file.stem
        silver_path = silver_lineups_dir / parquet_file.name
        
        # Check if source is newer than output
        if silver_path.exists() and not is_source_newer(parquet_file, silver_path):
            skipped_count += 1
            continue
        
        try:
            df = pl.read_parquet(parquet_file)
            lineups_count = len(df)
            total_lineups += lineups_count

            # Flatten columns
            df = flatten_columns(df)
            
            # Add missing columns
            expected_cols = set(lineups_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            
            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(lineups_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            
            # Cast columns to schema types
            df = cast_columns_to_schema_types(df, lineups_schema)

            # Validate the DataFrame against the schema
            log_schema_differences(df, lineups_schema, logger, parquet_file)

            df.write_parquet(silver_path, compression="snappy")
            processed_count += 1
            
            # Log progress every 100 files to reduce I/O overhead
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} lineup files...")
            
        except Exception as e:
            logger.warning(f"Failed to process lineups file {lineup_file}: {e}")
            error_count += 1
            continue

    # Summary
    logger.info("=== SILVER LINEUPS PROCESSING SUMMARY ===")
    logger.info(f"Files processed: {processed_count}")
    logger.info(f"Files skipped: {skipped_count}")
    logger.info(f"Files with errors: {error_count}")
    logger.info(f"Total lineups processed: {total_lineups:,}")
    if processed_count > 0:
        logger.info(f"Average lineups per file: {total_lineups // processed_count:,}")
    logger.info("==========================================")



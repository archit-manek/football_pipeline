from pathlib import Path
import polars as pl
from schemas.lineups_schema import lineups_schema
from utils.constants import BRONZE_DIR_LINEUPS, SILVER_DIR_LINEUPS, SILVER_LOGS_LINEUPS_PATH
from utils.dataframe import flatten_columns, log_schema_differences, fix_int_columns_with_nans, add_missing_columns, cast_columns_to_schema_types, get_int_columns_from_schema
from utils.logging import setup_logger



###
# Process lineups data from the bronze layer to the silver layer.
###
def process_lineups_data():
    log_path = Path(SILVER_LOGS_LINEUPS_PATH)
    logger = setup_logger(log_path, "lineups")
    logger.info("Starting to process lineups data...")

    bronze_lineups_dir = Path(BRONZE_DIR_LINEUPS)
    silver_lineups_dir = Path(SILVER_DIR_LINEUPS)

    # Ensure the silver directory exists
    Path(SILVER_DIR_LINEUPS).mkdir(parents=True, exist_ok=True)

    # Delete old Parquet files in the silver directory
    for file in Path(SILVER_DIR_LINEUPS).glob("*.parquet"):
        try:
            file.unlink()
            logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Could not delete file {file}: {e}")

    for parquet_file in bronze_lineups_dir.glob("*.parquet"):
        lineup_file = parquet_file.stem
        logger.info(f"Processing lineups from {parquet_file}")
        
        try:
            df = pl.read_parquet(parquet_file)
            logger.info(f"Loaded {len(df)} lineup records from {lineup_file}")

            # Transformations can be added here
            df = flatten_columns(df)
            logger.info(f"Flattened columns for {lineup_file}")
            
            expected_cols = set(lineups_schema.columns.keys())
            df = add_missing_columns(df, expected_cols)
            logger.info(f"Added missing columns for {lineup_file}")
            
            # Fix integer columns to preserve Int64 type (prevent Float64 conversion)
            int_cols = get_int_columns_from_schema(lineups_schema)
            df = fix_int_columns_with_nans(df, int_cols)
            logger.info(f"Fixed {len(int_cols)} integer columns to preserve Int64 type for {lineup_file}")
            
            df = cast_columns_to_schema_types(df, lineups_schema)
            logger.info(f"Cast columns to schema types for {lineup_file}")

            # Validate the DataFrame against the schema
            log_schema_differences(df, lineups_schema, logger, parquet_file)
            logger.info(f"Schema validation skipped for {lineup_file} (validation disabled)")

            df.write_parquet(silver_lineups_dir / parquet_file.name)
            logger.info(f"Saved processed lineups for {lineup_file} to {silver_lineups_dir / parquet_file.name}")
            
        except Exception as e:
            logger.error(f"Failed to process lineups file {parquet_file}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            continue

    logger.info("Lineups data processing completed successfully.")



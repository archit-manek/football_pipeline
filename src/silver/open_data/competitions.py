import polars as pl
from src.utils.constants import get_open_data_dirs, as_relative_path
from src.utils.schema import enforce_schema
from src.schemas.open_data.competitions_schema import COMPETITIONS_SCHEMA
from src.utils.logging import setup_logger

OPEN_DATA_DIR = get_open_data_dirs()
logger = setup_logger(OPEN_DATA_DIR["silver_competitions"] / "competitions.log", "competitions")

def process_competitions_data():
    """
    Process and clean competitions data from bronze to silver.
    This function:
    - Enforces the schema
    - Cleans the competition names
    - Removes duplicates
    - Sorts the data
    """
    
    open_data_dirs = get_open_data_dirs()
    bronze_path = open_data_dirs["bronze_competitions"]
    silver_path = open_data_dirs["silver_competitions"] / "competitions.parquet"

    logger.info(f"Starting competitions cleaning: {bronze_path} -> {silver_path}")

    try:
        competitions = pl.read_parquet(bronze_path)
        logger.info(f"Loaded competitions parquet from {bronze_path} with shape {competitions.shape}")

        competitions = enforce_schema(competitions, COMPETITIONS_SCHEMA)

        competitions = competitions.with_columns(
            pl.col("competition_name")
              .map_elements(clean_comp_name, return_dtype=pl.Utf8)
              .map_elements(lambda x: COMP_NAME_MAP.get(x, x), return_dtype=pl.Utf8)
              .alias("competition_name")
        )
        logger.info("Applied competition name cleaning and mapping.")

        before = competitions.shape[0]
        competitions = competitions.unique(subset=["competition_id", "season_id"])
        after = competitions.shape[0]
        logger.info(f"Removed duplicates: {before-after} rows dropped.")

        competitions = competitions.sort(["competition_name", "season_name"])
        logger.info("Sorted competitions DataFrame.")

        competitions.write_parquet(str(silver_path))
        logger.info(f"Saved cleaned competitions to {as_relative_path(silver_path)} with shape {competitions.shape}")
    
    except Exception as e:
        logger.error(f"Error in processing competitions data: {e}", exc_info=True)
        raise

def clean_comp_name(name: str | None) -> str:
    """
    Clean the competition name.
    """
    if name is None:
        return ""
    return (
        name.strip()
            .replace("  ", " ")
            .replace("league", "League")
    )

COMP_NAME_MAP = {
    "1. Bundesliga": "Bundesliga",
    "Indian Super league": "Indian Super League",
}
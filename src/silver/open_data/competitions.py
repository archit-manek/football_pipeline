import polars as pl
from src.utils.constants import get_open_data_dirs
from src.utils.schema import enforce_schema
from src.schemas.open_data.competitions_schema import COMPETITIONS_SCHEMA
from src.utils.logging import setup_logger

OPEN_DATA_DIR = get_open_data_dirs()
logger = setup_logger(str(OPEN_DATA_DIR["silver_competitions"] / "competitions.log"), "competitions")

def process_competitions_data():
    """
    Process competitions data from bronze to silver layer.
    """
    bronze_path = OPEN_DATA_DIR["bronze_competitions"] / "competitions.parquet"
    silver_path = OPEN_DATA_DIR["silver_competitions"] / "competitions.parquet"
    
    if not bronze_path.exists():
        logger.warning(f"Bronze competitions file not found: {bronze_path}")
        return
    
    try:
        # Read bronze data
        competitions = pl.read_parquet(bronze_path)
        logger.info(f"Loaded competitions from bronze with shape {competitions.shape}")
        
        # Apply schema validation
        competitions = enforce_schema(competitions, COMPETITIONS_SCHEMA)
        
        # Save to silver layer
        competitions.write_parquet(str(silver_path))
        logger.info(f"Saved cleaned competitions to {silver_path} with shape {competitions.shape}")
        
    except Exception as e:
        logger.error(f"Error processing competitions: {e}")
        raise 
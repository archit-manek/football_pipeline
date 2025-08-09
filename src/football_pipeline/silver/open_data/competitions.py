import polars as pl
from football_pipeline.utils.constants import get_open_data_dirs
from football_pipeline.utils.schema import enforce_schema
from football_pipeline.utils.logging import setup_logger

OPEN_DATA_DIR = get_open_data_dirs()

def process_competitions_data():
    """
    Process competitions data from bronze to silver layer.
    """
    # Setup logger only when this function is called
    logger = setup_logger(str(OPEN_DATA_DIR["silver_competitions"] / "competitions.log"), "competitions")
    
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
        # competitions = enforce_schema(competitions, COMPETITIONS_SCHEMA)
        
        # Save to silver layer
        competitions.write_parquet(str(silver_path))
        logger.info(f"Saved cleaned competitions to {silver_path} with shape {competitions.shape}")
        
    except Exception as e:
        logger.error(f"Error processing competitions: {e}")
        raise 
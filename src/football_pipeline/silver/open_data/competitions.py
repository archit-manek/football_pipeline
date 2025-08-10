import polars as pl
from football_pipeline.utils.constants import DATA_DIR
from football_pipeline.utils.schema import enforce_schema
from football_pipeline.utils.logging import setup_logger

# Get paths dynamically when needed

def process_competitions_data():
    """
    Process competitions data from bronze to silver layer.
    """
    # Setup logger only when this function is called
    source_path = "open_data/data"
    bronze_path = DATA_DIR / "bronze" / source_path / "competitions.parquet"
    silver_path = DATA_DIR / "silver" / source_path / "competitions.parquet"
    logger = setup_logger(str(silver_path.parent / "competitions.log"), "competitions")
    
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
import logging
import utils.constants as constants

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/gold/pipeline.log", mode="w"),
        logging.StreamHandler()
    ]
)

def aggregate_match_data():
    """
    Aggregate match data to create gold layer.
    
    This function will read the silver layer data, perform aggregation,
    and write the results to the gold layer.
    """
    logging.info("Starting aggregation of match data for gold layer.")
    
    # Read silver layer data
    silver_events_dir = constants.ABS_PATH / constants.SILVER_DIR_EVENTS
    if not silver_events_dir.exists():
        logging.error(f"Silver events directory {silver_events_dir} does not exist.")
        return
    logging.info(f"Reading silver events from {silver_events_dir}")

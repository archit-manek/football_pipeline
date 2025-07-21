from utils.constants import SUPPORTED_SOURCES, ensure_directories_exist, get_open_data_dirs
from utils.logging import setup_logger
# Bronze layer imports
from _01_bronze.open_data.ingest import open_data_ingest
from _01_bronze.j1_league.ingest import j1_league_ingest

# Silver layer imports
from _02_silver.open_data.competitions import process_competitions_data

# Gold layer imports

OPEN_DATA_DIRS = get_open_data_dirs()
log_path = OPEN_DATA_DIRS["logs_bronze"] / "bronze.log"
logger = setup_logger(log_path, f"bronze_layer")

def run_bronze_layer(source_name: str | None = None):
    """
    Run bronze layer processing for specified source(s).
    """

    print(f"\n=== BRONZE LAYER PROCESSING ===")

    if source_name:
        sources = [source_name]
    else:
        sources = SUPPORTED_SOURCES
    
    for source in sources:
        print(f"\nProcessing {source} bronze layer...")
        
        match source:
            case "open_data":
                open_data_ingest()
            case "j1_league":
                j1_league_ingest()
            case _:
                print(f"Unknown source: {source}")

def run_silver_layer(source_name: str | None = None):
    """
    Run silver layer processing for specified source(s).
    """
    print(f"\n=== SILVER LAYER PROCESSING ===")
    
    if source_name:
        sources = [source_name]
    else:
        sources = SUPPORTED_SOURCES
    
    for source in sources:
        print(f"\nProcessing {source_name} silver layer...")
        
        match source:
            case "open_data":
                process_competitions_data()
            case "j1_league":
                pass
            case _:
                print(f"Unknown source: {source}")

def run_gold_layer(source_name: str | None = None):
    """
    Run gold layer processing for specified source(s).
    """
    print(f"\n=== GOLD LAYER PROCESSING ===")
    
    if source_name:
        sources = [source_name]
    else:
        sources = SUPPORTED_SOURCES
    
    for source in sources:
        print(f"\nProcessing {source} gold layer...")
        
        # TODO: Update build_xg_model to accept source_name parameter
        # build_xg_model(source)
        print(f"Gold layer processing for {source} not yet implemented")

if __name__ == "__main__":
    # Choose which layers and sources to process
    # You can modify these variables to control what gets processed
    
    
    PROCESS_BRONZE = False
    PROCESS_SILVER = True
    PROCESS_GOLD = False
    
    # Specify source to process (None = all sources)
    SOURCE_TO_PROCESS = "open_data"  # "open_data", "j1_league", or None for all
    ensure_directories_exist(SOURCE_TO_PROCESS)
    
    # BRONZE STAGE
    if PROCESS_BRONZE:
        run_bronze_layer(SOURCE_TO_PROCESS)

    # SILVER STAGE
    if PROCESS_SILVER:
        run_silver_layer(SOURCE_TO_PROCESS)

    # GOLD STAGE
    if PROCESS_GOLD:
        run_gold_layer(SOURCE_TO_PROCESS)
        
    print("\nPipeline execution complete!")
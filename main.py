from football_pipeline.config import get_processing_config
from football_pipeline.pipeline import run_pipeline

if __name__ == "__main__":
    # Load configuration
    processing_config = get_processing_config()
    
    # Get processing settings from config
    PROCESS_BRONZE = processing_config.get('bronze', True)
    PROCESS_SILVER = processing_config.get('silver', False)
    PROCESS_GOLD = processing_config.get('gold', False)
    
    # Specify source to process (None = all sources)
    SOURCE_TO_PROCESS = processing_config.get('source', None)
    
    # Run the pipeline
    run_pipeline(
        bronze=PROCESS_BRONZE,
        silver=PROCESS_SILVER, 
        gold=PROCESS_GOLD,
        source=SOURCE_TO_PROCESS
    )
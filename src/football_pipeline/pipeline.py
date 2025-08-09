"""
Core pipeline functions for the Football Analytics Pipeline.

This module contains the main pipeline execution functions that can be
imported and used both by the CLI and the main.py script.
"""

from football_pipeline.utils.constants import SUPPORTED_SOURCES, ensure_directories_exist, get_open_data_dirs
from football_pipeline.utils.logging import setup_logger
from football_pipeline.config import get_processing_config, get_logging_config

# Bronze layer imports
from football_pipeline.bronze.open_data.ingest import open_data_ingest
from football_pipeline.bronze.j1_league.ingest import j1_league_ingest

# Silver layer imports
from football_pipeline.silver.open_data.competitions import process_competitions_data

# Gold layer imports
# TODO: Import gold layer functions when implemented

OPEN_DATA_DIRS = get_open_data_dirs()

def run_bronze_layer(source_name: str | None = None):
    """
    Run bronze layer processing for specified source(s).
    """
    # Setup logger only when this function is called
    log_path = OPEN_DATA_DIRS["logs_bronze"] / "bronze.log"
    logger = setup_logger(log_path, "bronze_layer")

    # Get error handling configuration
    processing_config = get_processing_config()
    fail_fast = processing_config.get('fail_fast', True)
    continue_on_error = processing_config.get('continue_on_error', False)

    logger.info("=== BRONZE LAYER PROCESSING ===")
    logger.debug(f"Error handling: fail_fast={fail_fast}, continue_on_error={continue_on_error}")

    if source_name:
        sources = [source_name]
        logger.info(f"Processing single source: {source_name}")
    else:
        sources = SUPPORTED_SOURCES
        logger.info(f"Processing all sources: {', '.join(sources)}")
    
    errors = []
    
    for source in sources:
        logger.info(f"Starting {source} bronze layer processing...")
        logger.debug(f"Source: {source}, Available sources: {SUPPORTED_SOURCES}")
        
        try:
            match source:
                case "open_data":
                    open_data_ingest(logger)
                    logger.info(f"✓ {source} bronze layer completed successfully")
                case "j1_league":
                    j1_league_ingest(logger)
                    logger.info(f"✓ {source} bronze layer completed successfully")
                case _:
                    logger.error(f"Unknown source: {source}")
                    logger.debug(f"Supported sources: {SUPPORTED_SOURCES}")
                    if fail_fast:
                        raise ValueError(f"Unknown source: {source}")
        except Exception as e:
            error_msg = f"✗ Failed to process {source} bronze layer: {e}"
            logger.error(error_msg)
            logger.debug(f"Exception details", exc_info=True)
            
            errors.append((source, e))
            
            if fail_fast:
                raise
            elif not continue_on_error:
                logger.warning(f"Stopping bronze processing due to error in {source}")
                break
            else:
                logger.warning(f"Continuing with next source despite error in {source}")
    
    # Report final status
    if errors:
        logger.warning(f"Bronze layer completed with {len(errors)} errors: {[err[0] for err in errors]}")
        if not continue_on_error and not fail_fast:
            raise Exception(f"Bronze layer failed for sources: {[err[0] for err in errors]}")
    else:
        logger.info("✓ Bronze Layer Processing Complete")

def run_silver_layer(source_name: str | None = None):
    """
    Run silver layer processing for specified source(s).
    """
    # Setup logger for silver layer
    log_path = OPEN_DATA_DIRS["logs_silver"] / "silver.log"
    logger = setup_logger(log_path, "silver_layer")
    
    logger.info("=== SILVER LAYER PROCESSING ===")
    
    if source_name:
        sources = [source_name]
        logger.info(f"Processing single source: {source_name}")
    else:
        sources = SUPPORTED_SOURCES
        logger.info(f"Processing all sources: {', '.join(sources)}")
    
    for source in sources:
        logger.info(f"Starting {source} silver layer processing...")
        logger.debug(f"Source: {source}, Available sources: {SUPPORTED_SOURCES}")
        
        try:
            match source:
                case "open_data":
                    process_competitions_data()
                    logger.info(f"✓ {source} silver layer completed successfully")
                case "j1_league":
                    logger.info(f"⚠ {source} silver layer not yet implemented")
                case _:
                    logger.error(f"Unknown source: {source}")
                    logger.debug(f"Supported sources: {SUPPORTED_SOURCES}")
        except Exception as e:
            logger.error(f"✗ Failed to process {source} silver layer: {e}")
            logger.debug(f"Exception details", exc_info=True)
            raise

def run_gold_layer(source_name: str | None = None):
    """
    Run gold layer processing for specified source(s).
    """
    # Setup logger for gold layer
    log_path = OPEN_DATA_DIRS["logs_gold"] / "gold.log"
    logger = setup_logger(log_path, "gold_layer")
    
    logger.info("=== GOLD LAYER PROCESSING ===")
    
    if source_name:
        sources = [source_name]
        logger.info(f"Processing single source: {source_name}")
    else:
        sources = SUPPORTED_SOURCES
        logger.info(f"Processing all sources: {', '.join(sources)}")
    
    for source in sources:
        logger.info(f"Starting {source} gold layer processing...")
        logger.debug(f"Source: {source}, Available sources: {SUPPORTED_SOURCES}")
        
        try:
            # TODO: Update build_xg_model to accept source_name parameter
            # build_xg_model(source)
            logger.warning(f"⚠ Gold layer processing for {source} not yet implemented")
            logger.debug(f"TODO: Implement gold layer processing for {source}")
        except Exception as e:
            logger.error(f"✗ Failed to process {source} gold layer: {e}")
            logger.debug(f"Exception details", exc_info=True)
            raise

def run_pipeline(bronze: bool = True, silver: bool = False, gold: bool = False, source: str | None = None):
    """
    Run the complete pipeline with specified layers and sources.
    
    Args:
        bronze: Whether to run bronze layer
        silver: Whether to run silver layer  
        gold: Whether to run gold layer
        source: Source to process (None for all sources)
    """
    # Setup main pipeline logger
    main_log_path = OPEN_DATA_DIRS["logs"] / "pipeline.log"
    
    # Load configuration
    logging_config = get_logging_config()
    import logging
    
    # Set logging levels from config
    console_level = getattr(logging, logging_config.get('console_level', 'INFO'))
    file_level = getattr(logging, logging_config.get('file_level', 'DEBUG'))
    
    main_logger = setup_logger(main_log_path, "main_pipeline", console_level, file_level)
    
    main_logger.info("🚀 Football Pipeline Starting")
    main_logger.info(f"Configuration: BRONZE={bronze}, SILVER={silver}, GOLD={gold}")
    main_logger.info(f"Target sources: {source or 'ALL'}")
    
    try:
        # Ensure directories exist
        main_logger.debug("Ensuring directories exist...")
        ensure_directories_exist(source)
        main_logger.debug("✓ Directories verified")
        
        # BRONZE STAGE
        if bronze:
            main_logger.info("Starting Bronze Layer Processing")
            run_bronze_layer(source)
            main_logger.info("✓ Bronze Layer Processing Complete")

        # SILVER STAGE
        if silver:
            main_logger.info("Starting Silver Layer Processing")
            run_silver_layer(source)
            main_logger.info("✓ Silver Layer Processing Complete")

        # GOLD STAGE
        if gold:
            main_logger.info("Starting Gold Layer Processing")
            run_gold_layer(source)
            main_logger.info("✓ Gold Layer Processing Complete")
            
        main_logger.info("🎉 Pipeline execution completed successfully!")
        return True
        
    except Exception as e:
        main_logger.error(f"💥 Pipeline execution failed: {e}")
        main_logger.debug("Full exception details:", exc_info=True)
        raise

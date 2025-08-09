#!/usr/bin/env python3
"""
CLI for Football Pipeline

Run the medallion pipeline (bronze/silver/gold) against one or all data sources.
Keeps the surface area small and obvious.
"""

from __future__ import annotations

import argparse
import logging
import sys

from football_pipeline.utils.constants import SUPPORTED_SOURCES, get_open_data_dirs
from football_pipeline.utils.logging import setup_logger
from football_pipeline.config import get_processing_config, get_logging_config

def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        description="Football Analytics Pipeline — run bronze/silver/gold layers over one or all sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  football_pipeline                    # Run with default config settings
  football_pipeline --bronze          # Run only bronze layer
  football_pipeline --source j1_league # Process only J1 League data
  football_pipeline --all-layers      # Run all layers (bronze, silver, gold)
  football_pipeline --config-only     # Show current configuration and exit
        """
    )
    
    # Layer selection
    parser.add_argument(
        "--bronze", 
        action="store_true", 
        help="Run bronze layer processing"
    )
    parser.add_argument(
        "--silver", 
        action="store_true", 
        help="Run silver layer processing"
    )
    parser.add_argument(
        "--gold", 
        action="store_true", 
        help="Run gold layer processing"
    )
    parser.add_argument(
        "--all-layers", 
        action="store_true", 
        help="Run all layers (bronze, silver, gold)"
    )
    
    # Source selection
    parser.add_argument(
        "--source",
        choices=SUPPORTED_SOURCES + ["all"],
        default=None,
        help=f"Data source to process. Options: {', '.join(SUPPORTED_SOURCES)}, or 'all' for all sources"
    )
    
    # Configuration
    parser.add_argument(
        "--config-only", 
        action="store_true", 
        help="Show current configuration and exit"
    )
    
    return parser

def show_config() -> None:
    """Display current configuration."""
    from football_pipeline.config import load_config
    import yaml
    
    config = load_config()
    print("🔧 Current Football Pipeline Configuration")
    print("=" * 50)
    print(yaml.safe_dump(config, default_flow_style=False, indent=2))

def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Show config and exit if requested
    if args.config_only:
        show_config()
        return 0
    
    # Load configuration
    processing_config = get_processing_config()
    
    # Determine what layers to run
    if args.all_layers:
        run_bronze = run_silver = run_gold = True
    elif args.bronze or args.silver or args.gold:
        run_bronze = args.bronze
        run_silver = args.silver
        run_gold = args.gold
    else:
        # Use config defaults
        run_bronze = processing_config.get('bronze', True)
        run_silver = processing_config.get('silver', False)
        run_gold = processing_config.get('gold', False)
    
    # Determine source
    source = args.source if args.source else processing_config.get('source', 'open_data')
    if source == 'all':
        source = None
    
    # Import pipeline function
    from football_pipeline.pipeline import run_pipeline
    
    # Run the pipeline - it handles all logging and error handling
    try:
        success = run_pipeline(
            bronze=run_bronze,
            silver=run_silver, 
            gold=run_gold,
            source=source
        )
        return 0 if success else 1
        
    except SystemExit:
        # bubble up argparse exits cleanly
        raise
    except Exception:
        # Pipeline function handles logging, just return error code
        return 1

if __name__ == "__main__":
    sys.exit(main())
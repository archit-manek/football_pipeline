#!/usr/bin/env python3
"""
CLI for Football Pipeline

Run the medallion pipeline (bronze/silver/gold) against one or all data sources.
Keeps the surface area small and obvious.
"""

import argparse
import sys

from football_pipeline.utils.constants import SUPPORTED_SOURCES

def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        description="Football Analytics Pipeline — run bronze/silver/gold layers over one or all sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  football_pipeline                    # Run bronze for open_data
  football_pipeline --bronze           # Run only bronze layer
  football_pipeline --source all       # Process all data sources
  football_pipeline --all-layers       # Run all layers (bronze, silver, gold)
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
    
    return parser

def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Determine what layers to run
    if args.all_layers:
        run_bronze = run_silver = run_gold = True
    elif args.bronze or args.silver or args.gold:
        run_bronze = args.bronze
        run_silver = args.silver
        run_gold = args.gold
    else:
        # Defaults: bronze only
        run_bronze = True
        run_silver = False
        run_gold = False
    
    # Determine source
    source = args.source if args.source else 'open_data'
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
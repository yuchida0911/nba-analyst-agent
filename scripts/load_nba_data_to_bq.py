#!/usr/bin/env python3
"""
Load NBA data to BigQuery.

This script loads NBA CSV data from Google Cloud Storage into BigQuery tables
with comprehensive logging and error handling.

Usage:
    python scripts/load_nba_data_to_bq.py
    python scripts/load_nba_data_to_bq.py --log-level DEBUG
    python scripts/load_nba_data_to_bq.py --log-format structured
"""
import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from nba_analyst.ingestion.bq_loader import NBABigQueryLoader

logger = logging.getLogger(__name__)

def get_available_csv_files() -> List[str]:
    """
    Get list of available NBA CSV files to load.
    
    Returns:
        List of CSV file names in the expected GCS bucket
    """
    # Default files - can be extended to check GCS bucket contents
    csv_files = [
        # "regular_season_box_scores_2010_2024_part_1.csv",
        # "regular_season_box_scores_2010_2024_part_2.csv", 
        # "regular_season_box_scores_2010_2024_part_3.csv",
        # "play_off_box_scores_2010_2024.csv",
        "regular_season_totals_2010_2024.csv", # Different schema - team totals
        # "play_off_totals_2010_2024.csv" # Different schema - team totals
    ]
    
    logger.debug(f"Available CSV files for loading: {csv_files}")
    return csv_files


def get_available_totals_csv_files() -> List[str]:
    """
    Get list of available NBA team totals CSV files to load.
    
    Returns:
        List of CSV file names in the expected GCS bucket
    """
    # Team totals files - use the cleaned version
    csv_files = [
        "regular_season_totals_2010_2024_cleaned.csv",
        # "play_off_totals_2010_2024.csv"
    ]
    
    logger.debug(f"Available team totals CSV files for loading: {csv_files}")
    return csv_files


def load_nba_data(project_id: Optional[str] = None,
                 csv_files: Optional[List[str]] = None,
                 create_table: bool = True,
                 create_dataset: bool = True,
                 create_totals_table: bool = False,
                 load_totals_data: bool = False) -> bool:
    """
    Load NBA data into BigQuery with comprehensive logging.
    
    Args:
        project_id: GCP project ID (uses default if None)
        csv_files: List of CSV files to load (uses default set if None)  
        create_table: Whether to create table first
        create_dataset: Whether to create dataset first
        
    Returns:
        bool: True if successful, False otherwise
    """
    start_time = datetime.now()
    
    try:
        # Initialize loader
        logger.info("🏀 Starting NBA data load to BigQuery")
        logger.info(f"Project ID: {project_id or 'default'}")
        
        loader = NBABigQueryLoader(project_id=project_id) if project_id else NBABigQueryLoader()
        
        # Get CSV files to load
        files_to_load = csv_files or get_available_csv_files()
        logger.info(f"📁 Files to load: {len(files_to_load)} files")
        for i, file in enumerate(files_to_load, 1):
            logger.info(f"  {i}. {file}")
        
        # Create dataset if requested
        if create_dataset:
            logger.info("🏗️  Creating BigQuery dataset...")
            dataset_created = loader.create_dataset()
            
            if not dataset_created:
                logger.error("❌ Failed to create BigQuery dataset")
                return False
            
            logger.info("✅ Dataset creation completed")
        
        # Create table if requested
        if create_table:
            logger.info("🏗️  Creating BigQuery table...")
            table_created = loader.create_players_raw_table()
            
            if not table_created:
                logger.error("❌ Failed to create BigQuery table")
                return False
            
            logger.info("✅ Table creation completed")

        # Optionally create team totals table
        if create_totals_table:
            logger.info("🏗️  Creating BigQuery team totals table...")
            totals_created = loader.create_totals_table()

            if not totals_created:
                logger.error("❌ Failed to create BigQuery team totals table")
                return False

            logger.info("✅ Team totals table creation completed")
        
        # Load CSV files
        logger.info("📤 Starting CSV file loading...")
        load_results = loader.load_csv_files(files_to_load)
        
        # Optionally load team totals data
        if load_totals_data:
            logger.info("📤 Starting team totals CSV file loading...")
            totals_files = get_available_totals_csv_files()
            totals_load_results = loader.load_totals_csv_files(totals_files)
            
            # Combine results
            load_results["total_files"] += totals_load_results["total_files"]
            load_results["successful_loads"] += totals_load_results["successful_loads"]
            load_results["failed_loads"] += totals_load_results["failed_loads"]
            load_results["total_rows_loaded"] += totals_load_results["total_rows_loaded"]
            load_results["total_bytes_processed"] += totals_load_results["total_bytes_processed"]
            load_results["job_details"].extend(totals_load_results["job_details"])
            load_results["errors"].extend(totals_load_results["errors"])
        
        # Analyze results
        duration = (datetime.now() - start_time).total_seconds()
        success_rate = (load_results["successful_loads"] / load_results["total_files"]) * 100
        
        # Log summary
        logger.info("🏁 NBA data load operation completed!")
        logger.info(f"⏱️  Total duration: {duration:.2f} seconds")
        logger.info(f"📊 Results summary:")
        logger.info(f"   • Files processed: {load_results['total_files']}")
        logger.info(f"   • Successful loads: {load_results['successful_loads']}")
        logger.info(f"   • Failed loads: {load_results['failed_loads']}")
        logger.info(f"   • Success rate: {success_rate:.1f}%")
        logger.info(f"   • Total rows loaded: {load_results['total_rows_loaded']:,}")
        logger.info(f"   • Total bytes processed: {load_results['total_bytes_processed']:,}")
        
        # Log detailed job information
        if load_results["job_details"]:
            logger.info("📋 Job details:")
            for job in load_results["job_details"]:
                status_emoji = "✅" if job["status"] == "success" else "❌"
                logger.info(f"   {status_emoji} {job['file']}: "
                          f"{job['rows_loaded']:,} rows, "
                          f"{job['duration_seconds']:.1f}s")
                if job["error"]:
                    logger.warning(f"     Error: {job['error']}")
        
        # Get final table info
        logger.info("🔍 Retrieving final table information...")
        table_info = loader.get_table_info()
        if table_info:
            logger.info(f"📈 Final table stats:")
            logger.info(f"   • Total rows: {table_info['num_rows']:,}")
            logger.info(f"   • Table size: {table_info['num_bytes']:,} bytes")
            logger.info(f"   • Last modified: {table_info['modified']}")
        
        # Determine overall success
        overall_success = load_results["failed_loads"] == 0
        
        if overall_success:
            logger.info("🎉 All files loaded successfully!")
        else:
            logger.warning(f"⚠️  {load_results['failed_loads']} file(s) failed to load")
            for error in load_results["errors"]:
                logger.error(f"   • {error}")
        
        return overall_success
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ Critical error during NBA data load after {duration:.2f}s: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        return False


def main():
    """Main entry point with argument parsing and logging setup."""
    parser = argparse.ArgumentParser(
        description="Load NBA data from GCS to BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic load with default settings
    python scripts/load_nba_data_to_bq.py
    
    # Debug logging with structured format
    python scripts/load_nba_data_to_bq.py --log-level DEBUG --log-format structured
    
    # Load specific files only
    python scripts/load_nba_data_to_bq.py --files "file1.csv,file2.csv"
    
    # Skip dataset and table creation (both already exist)
    python scripts/load_nba_data_to_bq.py --no-create-dataset --no-create-table
    
    # Load team totals data only
    python scripts/load_nba_data_to_bq.py --create-totals-table --load-totals-data
    
    # Load both player and team data
    python scripts/load_nba_data_to_bq.py --create-totals-table --load-totals-data
        """
    )
    
    parser.add_argument(
        "--project-id",
        help="GCP project ID (uses default if not specified)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--log-format", 
        choices=["structured", "simple"],
        default="simple",
        help="Log format style (default: simple)"
    )
    
    parser.add_argument(
        "--files",
        help="Comma-separated list of CSV files to load (loads all by default)"
    )
    
    parser.add_argument(
        "--no-create-dataset",
        action="store_true",
        help="Skip dataset creation step (assumes dataset already exists)"
    )
    
    parser.add_argument(
        "--no-create-table",
        action="store_true",
        help="Skip table creation step (assumes table already exists)"
    )
    parser.add_argument(
        "--create-totals-table",
        action="store_true",
        help="Also create 'totals' team-level table based on totals_schema.json",
    )
    
    parser.add_argument(
        "--load-totals-data",
        action="store_true",
        help="Also load team totals CSV data into the totals table",
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true", 
        help="Enable verbose output (same as --log-level DEBUG)"
    )
    
    args = parser.parse_args()
    
    # Set log level
    log_level = "DEBUG" if args.verbose else args.log_level
    
    # Setup logging
    NBABigQueryLoader.setup_logging(level=log_level, format_style=args.log_format)
    
    logger.info("🚀 NBA BigQuery Data Loader")
    logger.info("=" * 40)
    logger.info(f"Log level: {log_level}")
    logger.info(f"Log format: {args.log_format}")
    logger.info(f"Project ID: {args.project_id or 'default'}")
    logger.info(f"Create dataset: {not args.no_create_dataset}")
    logger.info(f"Create table: {not args.no_create_table}")
    logger.info(f"Create totals table: {args.create_totals_table}")
    logger.info(f"Load totals data: {args.load_totals_data}")
    
    # Parse CSV files
    csv_files = None
    if args.files:
        csv_files = [f.strip() for f in args.files.split(",")]
        logger.info(f"Custom file list: {csv_files}")
    else:
        logger.info("Using default file list")
    
    # Execute data load
    try:
        success = load_nba_data(
            project_id=args.project_id,
            csv_files=csv_files,
            create_dataset=not args.no_create_dataset,
            create_table=not args.no_create_table,
            create_totals_table=args.create_totals_table,
            load_totals_data=args.load_totals_data,
        )
        
        if success:
            logger.info("✅ Script completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Script completed with errors!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("🛑 Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Complete NBA Data Loading Script

This script loads all NBA datasets from the NBA-Data-2010-2024 directory
into PostgreSQL with complete processing pipeline including:

1. Raw data ingestion from CSV files
2. Advanced metrics calculation  
3. Trend analysis and aggregation
4. AI-ready data preparation

Usage:
    python scripts/load_nba_data.py [--batch-size 1000] [--data-dir NBA-Data-2010-2024]
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nba_analyst.config.settings import Settings
from nba_analyst.config.database import DatabaseConfig
from nba_analyst.database.connection import DatabaseConnection
from nba_analyst.database.models import Base
from nba_analyst.processing.workflow import WorkflowManager
from nba_analyst.processing.pipeline import create_processing_pipeline


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.FileHandler(log_dir / "nba_data_loading.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )


def validate_setup(db_connection: DatabaseConnection) -> bool:
    """Validate database setup and connectivity."""
    
    print("🔧 Validating database setup...")
    
    # Test connection
    if not db_connection.test_connection():
        print("❌ Database connection failed!")
        print("💡 Make sure PostgreSQL is running and check your .env configuration")
        return False
    
    print("✅ Database connection successful")
    
    # Check if tables exist
    inspector = db_connection.get_inspector()
    tables = inspector.get_table_names()
    
    required_tables = ['players_raw', 'teams_raw', 'players_processed', 'player_monthly_trends']
    missing_tables = [table for table in required_tables if table not in tables]
    
    if missing_tables:
        print(f"⚠️  Missing tables: {missing_tables}")
        print("🔧 Creating database schema...")
        
        try:
            Base.metadata.create_all(db_connection.engine)
            print("✅ Database schema created successfully")
        except Exception as e:
            print(f"❌ Failed to create database schema: {str(e)}")
            return False
    else:
        print("✅ All required tables exist")
    
    return True


def validate_data_directory(data_dir: Path) -> bool:
    """Validate the NBA data directory."""
    
    print(f"📁 Validating data directory: {data_dir}")
    
    if not data_dir.exists():
        print(f"❌ Data directory does not exist: {data_dir}")
        return False
    
    if not data_dir.is_dir():
        print(f"❌ Path is not a directory: {data_dir}")
        return False
    
    # Check for CSV files
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"❌ No CSV files found in {data_dir}")
        return False
    
    print(f"✅ Found {len(csv_files)} CSV files:")
    for csv_file in sorted(csv_files):
        file_size = csv_file.stat().st_size / (1024 * 1024)  # Size in MB
        print(f"   • {csv_file.name} ({file_size:.1f} MB)")
    
    return True


def load_nba_data(data_dir: Path, batch_size: int = 1000) -> bool:
    """
    Load complete NBA dataset using the processing pipeline.
    
    Args:
        data_dir: Path to NBA data directory
        batch_size: Batch size for processing
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Setup database connection
        settings = Settings()
        db_config = DatabaseConfig(settings)
        db_connection = DatabaseConnection(db_config)
        
        # Validate setup
        if not validate_setup(db_connection):
            return False
        
        if not validate_data_directory(data_dir):
            return False
        
        print(f"\n🚀 Starting NBA data loading process...")
        print(f"📊 Batch size: {batch_size}")
        print(f"📁 Data directory: {data_dir}")
        
        # Create workflow manager
        workflow_manager = WorkflowManager(db_connection)
        
        # Create NBA data processing workflow
        workflow = workflow_manager.create_nba_data_processing_workflow(
            data_directory=data_dir,
            batch_size=batch_size
        )
        
        # Execute workflow
        print(f"\n⚙️  Executing data processing workflow...")
        start_time = datetime.now()
        
        execution = workflow_manager.execute_workflow(workflow)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Report results
        print(f"\n📋 Processing Results:")
        print(f"   Status: {execution.status.value}")
        print(f"   Duration: {duration}")
        print(f"   Progress: {execution.progress:.1%}")
        
        if execution.status.value == "completed" and execution.results:
            # Get detailed results from processing task
            processing_result = execution.results.get("process_nba_dataset")
            
            if processing_result:
                print(f"\n📈 Processing Statistics:")
                print(f"   Files processed: {processing_result.total_files_processed}")
                print(f"   Records ingested: {processing_result.total_records_ingested:,}")
                print(f"   Records with advanced metrics: {processing_result.total_records_processed:,}")
                print(f"   Success rate: {processing_result.overall_success_rate:.1%}")
                
                if processing_result.errors:
                    print(f"\n⚠️  Errors encountered:")
                    for error in processing_result.errors[:5]:  # Show first 5 errors
                        print(f"   • {error}")
                    
                    if len(processing_result.errors) > 5:
                        print(f"   ... and {len(processing_result.errors) - 5} more errors")
            
            # Show summary from other tasks
            validation_result = execution.results.get("validate_data_directory")
            if validation_result:
                print(f"\n📁 Data Discovery:")
                print(f"   Directory: {validation_result['directory']}")
                print(f"   CSV files: {validation_result['csv_file_count']}")
        
        if execution.error_message:
            print(f"\n❌ Error: {execution.error_message}")
            return False
        
        # Close database connection
        db_connection.close()
        
        print(f"\n✅ NBA data loading completed successfully!")
        print(f"🎯 Your PostgreSQL database is now ready for NBA analytics!")
        
        print(f"\n🔍 Next steps:")
        print(f"   • Query players_processed table for AI-ready player data")
        print(f"   • Use player_monthly_trends for trend analysis")
        print(f"   • Run analytics examples: python examples/analyze_player.py")
        
        return True
        
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        print(f"❌ Data loading failed: {str(e)}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Load NBA datasets into PostgreSQL with complete processing pipeline"
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("NBA-Data-2010-2024"),
        help="Path to NBA data directory (default: NBA-Data-2010-2024)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for processing (default: 1000)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    print("🏀 NBA Analyst Agent - Complete Data Loading")
    print("=" * 50)
    
    # Load NBA data
    success = load_nba_data(
        data_dir=args.data_dir,
        batch_size=args.batch_size
    )
    
    if success:
        print(f"\n🎉 Data loading completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Data loading failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Database table creation script for NBA Analyst Agent.

This script creates all database tables defined in the SQLAlchemy models.
It can be used for initial database setup or recreating tables.

Usage:
    python scripts/create_database.py [--drop-existing] [--config-file .env]
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analytics_pipeline.config.settings import load_settings
from analytics_pipeline.config.database import get_database_config
from analytics_pipeline.database.connection import get_database_connection
from analytics_pipeline.database.models import Base, PlayerBoxScore, TeamGameTotal


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def create_tables(drop_existing: bool = False, config_file: str = None) -> bool:
    """
    Create all database tables.
    
    Args:
        drop_existing: Whether to drop existing tables first
        config_file: Path to configuration file
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        settings = load_settings(config_file)
        db_config = get_database_config(settings)
        
        # Get database connection
        logger.info("Connecting to database...")
        db_conn = get_database_connection(db_config)
        
        # Log connection info (without password)
        conn_info = db_conn.get_connection_info()
        logger.info(f"Connected to: {conn_info['host']}:{conn_info['port']}/{conn_info['database']}")
        
        # Test connection
        if not db_conn.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("Database connection test successful")
        
        # Get SQLAlchemy engine
        engine = db_conn.engine
        
        # Drop existing tables if requested
        if drop_existing:
            logger.warning("Dropping existing tables...")
            Base.metadata.drop_all(engine)
            logger.info("Existing tables dropped")
        
        # Create all tables
        logger.info("Creating database tables...")
        Base.metadata.create_all(engine)
        
        # Verify tables were created
        logger.info("Verifying table creation...")
        tables_created = []
        
        # Check for players_raw table
        if db_conn.check_table_exists('players_raw'):
            tables_created.append('players_raw')
            row_count = db_conn.get_table_row_count('players_raw')
            logger.info(f"✓ players_raw table created (rows: {row_count})")
        else:
            logger.error("✗ players_raw table not found")
            return False
        
        # Check for teams_raw table  
        if db_conn.check_table_exists('teams_raw'):
            tables_created.append('teams_raw')
            row_count = db_conn.get_table_row_count('teams_raw')
            logger.info(f"✓ teams_raw table created (rows: {row_count})")
        else:
            logger.error("✗ teams_raw table not found")
            return False
        
        logger.info(f"Successfully created {len(tables_created)} tables: {', '.join(tables_created)}")
        return True
        
    except Exception as e:
        logger.error(f"Database table creation failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False
    
    finally:
        # Clean up database connections
        try:
            db_conn.close()
            logger.info("Database connections closed")
        except:
            pass


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create NBA Analyst Agent database tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Create tables with default configuration
    python scripts/create_database.py
    
    # Drop existing tables and recreate
    python scripts/create_database.py --drop-existing
    
    # Use custom configuration file
    python scripts/create_database.py --config-file /path/to/.env
    
    # Enable debug logging
    python scripts/create_database.py --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing tables before creating new ones"
    )
    
    parser.add_argument(
        "--config-file",
        type=str,
        help="Path to configuration file (default: .env)"
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
    logger = logging.getLogger(__name__)
    
    logger.info("NBA Analyst Agent - Database Table Creation")
    logger.info("=" * 50)
    
    if args.drop_existing:
        logger.warning("WARNING: This will drop existing tables and all data!")
        response = input("Are you sure you want to continue? (yes/no): ")
        if response.lower() not in ["yes", "y"]:
            logger.info("Operation cancelled")
            return 0
    
    # Create tables
    success = create_tables(
        drop_existing=args.drop_existing,
        config_file=args.config_file
    )
    
    if success:
        logger.info("Database table creation completed successfully!")
        return 0
    else:
        logger.error("Database table creation failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
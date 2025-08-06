#!/usr/bin/env python3
"""
PostgreSQL Database Setup Script for NBA Analyst Agent

This script sets up the local PostgreSQL database with all required tables
and indexes for the NBA analytics system.

Usage:
    python scripts/setup_postgres.py [--create-user] [--reset]
"""

import argparse
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nba_analyst.config.settings import Settings
from nba_analyst.config.database import DatabaseConfig
from nba_analyst.database.connection import DatabaseConnection
from nba_analyst.database.models import Base


def check_env_file() -> bool:
    """Check if .env file exists and has required configuration."""
    env_file = Path(".env")
    
    if not env_file.exists():
        print("❌ .env file not found!")
        print("📋 Please copy .env.example to .env and configure your database settings:")
        print("   cp .env.example .env")
        print("   # Then edit .env with your PostgreSQL credentials")
        return False
    
    print("✅ .env file found")
    return True


def test_database_connection(settings: Settings) -> bool:
    """Test database connection."""
    try:
        db_config = DatabaseConfig(settings)
        db_connection = DatabaseConnection(db_config)
        
        if db_connection.test_connection():
            print("✅ Database connection successful")
            db_connection.close()
            return True
        else:
            print("❌ Database connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Database connection error: {str(e)}")
        return False


def create_database_schema(settings: Settings, reset: bool = False) -> bool:
    """Create database schema with all tables and indexes."""
    try:
        db_config = DatabaseConfig(settings)
        db_connection = DatabaseConnection(db_config)
        
        print("🏗️  Creating database schema...")
        
        if reset:
            print("⚠️  Resetting database - dropping all existing tables...")
            Base.metadata.drop_all(db_connection.engine)
            print("✅ Existing tables dropped")
        
        # Create all tables
        Base.metadata.create_all(db_connection.engine)
        
        # Verify tables were created
        inspector = db_connection.get_inspector()
        tables = inspector.get_table_names()
        
        expected_tables = ['players_raw', 'teams_raw', 'players_processed', 'player_monthly_trends']
        created_tables = [table for table in expected_tables if table in tables]
        
        print("✅ Database schema created successfully!")
        print(f"📊 Tables created: {len(created_tables)}")
        
        for table in created_tables:
            # Get index information
            indexes = inspector.get_indexes(table)
            print(f"   • {table} ({len(indexes)} indexes)")
        
        db_connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database schema: {str(e)}")
        return False


def show_database_info(settings: Settings):
    """Show database configuration information."""
    print(f"📋 Database Configuration:")
    print(f"   Host: {settings.db_host}")
    print(f"   Port: {settings.db_port}")
    print(f"   Database: {settings.db_name}")
    print(f"   User: {settings.db_user}")
    print(f"   Schema: {settings.db_schema}")


def show_next_steps():
    """Show next steps after successful setup."""
    print(f"\n🎯 Next Steps:")
    print(f"1. Load NBA data:")
    print(f"   python scripts/load_nba_data.py")
    print(f"")
    print(f"2. Load specific dataset:")
    print(f"   python scripts/load_nba_data.py --data-dir NBA-Data-2010-2024 --batch-size 500")
    print(f"")
    print(f"3. Test with analytics examples:")
    print(f"   python examples/player_analysis.py 'LeBron James'")
    print(f"")
    print(f"📚 Documentation:")
    print(f"   • Usage guide: docs/USAGE.md")
    print(f"   • PRD: docs/PRD.md")


def main():
    """Main setup function."""
    parser = argparse.ArgumentParser(
        description="Setup PostgreSQL database for NBA Analyst Agent"
    )
    
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset database by dropping and recreating all tables"
    )
    
    parser.add_argument(
        "--create-user",
        action="store_true",
        help="Show instructions for creating PostgreSQL user (informational only)"
    )
    
    args = parser.parse_args()
    
    print("🏀 NBA Analyst Agent - PostgreSQL Database Setup")
    print("=" * 55)
    
    # Check environment file
    if not check_env_file():
        return False
    
    # Load settings
    try:
        settings = Settings()
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Configuration error: {str(e)}")
        return False
    
    # Show database configuration
    show_database_info(settings)
    
    # Show PostgreSQL user creation instructions if requested
    if args.create_user:
        print(f"\n🔧 PostgreSQL User Creation Commands:")
        print(f"Connect to PostgreSQL as superuser and run:")
        print(f"")
        print(f"  CREATE DATABASE {settings.db_name};")
        print(f"  CREATE USER {settings.db_user} WITH PASSWORD '{settings.db_password}';")
        print(f"  GRANT ALL PRIVILEGES ON DATABASE {settings.db_name} TO {settings.db_user};")
        print(f"  \\c {settings.db_name}")
        print(f"  GRANT ALL ON SCHEMA public TO {settings.db_user};")
        print(f"  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {settings.db_user};")
        print(f"")
        return True
    
    # Test database connection
    print(f"\n🔗 Testing database connection...")
    if not test_database_connection(settings):
        print(f"\n💡 Troubleshooting:")
        print(f"1. Make sure PostgreSQL is running:")
        print(f"   brew services start postgresql@15")
        print(f"")
        print(f"2. Create database and user (if not exists):")
        print(f"   python scripts/setup_postgres.py --create-user")
        print(f"")
        print(f"3. Check your .env file configuration")
        return False
    
    # Create database schema
    if not create_database_schema(settings, reset=args.reset):
        return False
    
    print(f"\n✅ PostgreSQL database setup completed successfully!")
    print(f"🚀 Your database is ready for NBA data processing!")
    
    show_next_steps()
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
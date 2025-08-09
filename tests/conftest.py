"""Pytest configuration and shared fixtures."""

import os
import sys
import tempfile
from pathlib import Path
from typing import Generator

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from analytics_pipeline.config.settings import Settings
from analytics_pipeline.config.database import DatabaseConfig
from analytics_pipeline.database.connection import DatabaseConnection
from analytics_pipeline.database.models import Base


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Create test settings."""
    return Settings(
        testing=True,
        db_host="",
        db_name=":memory:",  # In-memory SQLite for speed
        db_user="",
        db_password="",
        db_schema="",
        log_level="WARNING",  # Reduce log noise in tests
        debug=False
    )


@pytest.fixture(scope="function")
def test_db_connection(test_settings) -> Generator[DatabaseConnection, None, None]:
    """Create a test database connection with fresh tables for each test."""
    db_config = DatabaseConfig(test_settings)
    db_conn = DatabaseConnection(db_config)
    
    # Create all tables
    Base.metadata.create_all(db_conn.engine)
    
    yield db_conn
    
    # Clean up
    db_conn.close()


@pytest.fixture(scope="function")
def test_db_session(test_db_connection):
    """Create a test database session."""
    with test_db_connection.get_session() as session:
        yield session


@pytest.fixture
def sample_box_scores_data() -> pd.DataFrame:
    """Create sample box scores DataFrame for testing."""
    return pd.DataFrame([
        {
            'season_year': '2023-24',
            'game_date': '2024-01-15',
            'gameId': 22300123,
            'matchup': 'LAL @ GSW',
            'teamId': 1610612747,
            'teamCity': 'Los Angeles',
            'teamName': 'Lakers',
            'teamTricode': 'LAL',
            'teamSlug': 'lakers',
            'personId': 2544,
            'personName': 'LeBron James',
            'position': 'F',
            'comment': '',
            'jerseyNum': '6',
            'minutes': '35:24',
            'fieldGoalsMade': 12,
            'fieldGoalsAttempted': 20,
            'fieldGoalsPercentage': 0.600,
            'threePointersMade': 3,
            'threePointersAttempted': 8,
            'threePointersPercentage': 0.375,
            'freeThrowsMade': 8,
            'freeThrowsAttempted': 10,
            'freeThrowsPercentage': 0.800,
            'reboundsOffensive': 2,
            'reboundsDefensive': 8,
            'reboundsTotal': 10,
            'assists': 7,
            'steals': 2,
            'blocks': 1,
            'turnovers': 3,
            'foulsPersonal': 2,
            'points': 35,
            'plusMinusPoints': 12
        },
        {
            'season_year': '2023-24',
            'game_date': '2024-01-15',
            'gameId': 22300123,
            'matchup': 'LAL @ GSW',
            'teamId': 1610612747,
            'teamCity': 'Los Angeles',
            'teamName': 'Lakers',  
            'teamTricode': 'LAL',
            'teamSlug': 'lakers',
            'personId': 203999,
            'personName': 'Anthony Davis',
            'position': 'F-C',
            'comment': '',
            'jerseyNum': '3',
            'minutes': '32:15',
            'fieldGoalsMade': 8,
            'fieldGoalsAttempted': 15,
            'fieldGoalsPercentage': 0.533,
            'threePointersMade': 0,
            'threePointersAttempted': 2,
            'threePointersPercentage': 0.000,
            'freeThrowsMade': 6,
            'freeThrowsAttempted': 8,
            'freeThrowsPercentage': 0.750,
            'reboundsOffensive': 4,
            'reboundsDefensive': 11,
            'reboundsTotal': 15,
            'assists': 3,
            'steals': 1,
            'blocks': 3,
            'turnovers': 2,
            'foulsPersonal': 4,
            'points': 22,
            'plusMinusPoints': 8
        }
    ])


@pytest.fixture
def sample_totals_data() -> pd.DataFrame:
    """Create sample totals DataFrame for testing."""
    return pd.DataFrame([
        {
            'SEASON_YEAR': '2023-24',
            'TEAM_ID': 1610612747,
            'TEAM_ABBREVIATION': 'LAL',
            'TEAM_NAME': 'Los Angeles Lakers',
            'GAME_ID': 22300123,
            'GAME_DATE': '2024-01-15T00:00:00',
            'MATCHUP': 'LAL @ GSW',
            'WL': 'W',
            'MIN': 48.0,
            'FGM': 45,
            'FGA': 88,
            'FG_PCT': 0.511,
            'FG3M': 15,
            'FG3A': 35,
            'FG3_PCT': 0.429,
            'FTM': 18,
            'FTA': 22,
            'FT_PCT': 0.818,
            'OREB': 10,
            'DREB': 35,
            'REB': 45,
            'AST': 28,
            'TOV': 12.0,
            'STL': 8,
            'BLK': 6,
            'BLKA': 4,
            'PF': 20,
            'PFD': 18,
            'PTS': 123,
            'PLUS_MINUS': 15.0,
            'AVAILABLE_FLAG': 1.0
        }
    ])


@pytest.fixture
def temp_csv_file() -> Generator[Path, None, None]:
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_path = Path(f.name)
        
    yield temp_path
    
    # Clean up
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_box_scores_csv(sample_box_scores_data) -> Generator[Path, None, None]:
    """Create a temporary CSV file with sample box scores data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='_box_scores.csv', delete=False) as f:
        temp_path = Path(f.name)
    
    sample_box_scores_data.to_csv(temp_path, index=False)
    
    yield temp_path
    
    # Clean up
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_totals_csv(sample_totals_data) -> Generator[Path, None, None]:
    """Create a temporary CSV file with sample totals data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='_totals.csv', delete=False) as f:
        temp_path = Path(f.name)
    
    sample_totals_data.to_csv(temp_path, index=False)
    
    yield temp_path
    
    # Clean up
    if temp_path.exists():
        temp_path.unlink()
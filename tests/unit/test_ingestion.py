"""Unit tests for data ingestion pipeline."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch

from analytics_pipeline.ingestion.ingest import (
    NBADataIngestion, IngestionStats, IngestionResult, create_ingestion_pipeline
)


class TestIngestionStats:
    """Test cases for IngestionStats class."""
    
    def test_ingestion_stats_creation(self):
        """Test creating IngestionStats instance."""
        stats = IngestionStats(
            total_rows_read=100,
            rows_validated=95,
            rows_inserted=90,
            rows_updated=5,
            validation_errors=2,
            validation_warnings=8,
            processing_time_seconds=1.5
        )
        
        assert stats.total_rows_read == 100
        assert stats.rows_validated == 95
        assert stats.rows_inserted == 90
        assert stats.rows_updated == 5
        assert stats.validation_errors == 2
        assert stats.validation_warnings == 8
        assert stats.processing_time_seconds == 1.5
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        stats = IngestionStats(
            total_rows_read=100,
            rows_inserted=80,
            rows_updated=10
        )
        
        assert stats.success_rate == 0.9  # (80 + 10) / 100
    
    def test_success_rate_zero_division(self):
        """Test success rate with zero rows read."""
        stats = IngestionStats(total_rows_read=0)
        assert stats.success_rate == 0.0
    
    def test_validation_error_rate(self):
        """Test validation error rate calculation."""
        stats = IngestionStats(
            rows_validated=100,
            validation_errors=5
        )
        
        assert stats.validation_error_rate == 0.05
    
    def test_validation_error_rate_zero_division(self):
        """Test validation error rate with zero validated rows."""
        stats = IngestionStats(rows_validated=0, validation_errors=5)
        assert stats.validation_error_rate == 0.0


class TestIngestionResult:
    """Test cases for IngestionResult class."""
    
    def test_ingestion_result_creation(self):
        """Test creating IngestionResult instance."""
        stats = IngestionStats(rows_inserted=50)
        result = IngestionResult(
            success=True,
            stats=stats,
            errors=['warning1'],
            file_path=Path('test.csv'),
            table_name='players_raw'
        )
        
        assert result.success is True
        assert result.stats.rows_inserted == 50
        assert len(result.errors) == 1
        assert result.file_path == Path('test.csv')
        assert result.table_name == 'players_raw'
    
    def test_ingestion_result_string_representation(self):
        """Test IngestionResult string representation."""
        stats = IngestionStats(rows_inserted=25)
        result = IngestionResult(
            success=True,
            stats=stats,
            errors=['error1', 'error2']
        )
        
        result_str = str(result)
        assert 'success=True' in result_str
        assert 'inserted=25' in result_str
        assert 'errors=2' in result_str


class TestNBADataIngestion:
    """Test cases for NBADataIngestion class."""
    
    @pytest.fixture
    def mock_components(self):
        """Create mock components for testing."""
        mock_db_connection = Mock()
        mock_csv_reader = Mock()
        mock_validator = Mock()
        
        return {
            'db_connection': mock_db_connection,
            'csv_reader': mock_csv_reader,
            'validator': mock_validator
        }
    
    def test_ingestion_initialization(self, mock_components):
        """Test ingestion pipeline initialization."""
        pipeline = NBADataIngestion(
            db_connection=mock_components['db_connection'],
            csv_reader=mock_components['csv_reader'],
            validator=mock_components['validator'],
            batch_size=500,
            validate_data=False,
            upsert_mode=False
        )
        
        assert pipeline.batch_size == 500
        assert pipeline.validate_data is False
        assert pipeline.upsert_mode is False
        assert pipeline.db_connection == mock_components['db_connection']
        assert pipeline.csv_reader == mock_components['csv_reader']
        assert pipeline.validator == mock_components['validator']
    
    def test_box_score_row_to_dict_conversion(self):
        """Test box score row to dictionary conversion."""
        pipeline = NBADataIngestion()
        
        # Create a sample row
        row = pd.Series({
            'gameId': 123456,
            'personId': 2544,
            'season_year': '2023-24',
            'game_date': '2024-01-15',
            'teamId': 1610612747,
            'teamCity': 'Los Angeles',
            'teamName': 'Lakers',
            'teamTricode': 'LAL',
            'teamSlug': 'lakers',
            'personName': 'LeBron James',
            'position': 'F',
            'comment': '',
            'jerseyNum': '6',
            'minutes': '35:24',
            'fieldGoalsMade': 12,
            'fieldGoalsAttempted': 20,
            'fieldGoalsPercentage': 0.6,
            'threePointersMade': 3,
            'threePointersAttempted': 8,
            'threePointersPercentage': 0.375,
            'freeThrowsMade': 8,
            'freeThrowsAttempted': 10,
            'freeThrowsPercentage': 0.8,
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
        })
        
        result = pipeline._box_score_row_to_dict(row)
        
        assert result['game_id'] == 123456
        assert result['person_id'] == 2544
        assert result['season_year'] == '2023-24'
        assert result['person_name'] == 'LeBron James'
        assert result['points'] == 35
        assert result['assists'] == 7
        assert result['field_goals_made'] == 12
        assert result['three_pointers_made'] == 3
        assert result['rebounds_total'] == 10
    
    def test_totals_row_to_dict_conversion(self):
        """Test totals row to dictionary conversion."""
        pipeline = NBADataIngestion()
        
        # Create a sample totals row
        row = pd.Series({
            'GAME_ID': 22300123,
            'TEAM_ID': 1610612747,
            'SEASON_YEAR': '2023-24',
            'TEAM_ABBREVIATION': 'LAL',
            'TEAM_NAME': 'Los Angeles Lakers',
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
        })
        
        result = pipeline._totals_row_to_dict(row)
        
        assert result['game_id'] == 22300123
        assert result['team_id'] == 1610612747
        assert result['season_year'] == '2023-24'
        assert result['team_name'] == 'Los Angeles Lakers'
        assert result['pts'] == 123
        assert result['wl'] == 'W'
        assert result['fgm'] == 45
        assert result['reb'] == 45
    
    def test_row_conversion_with_missing_values(self):
        """Test row conversion with missing/null values."""
        pipeline = NBADataIngestion()
        
        # Create row with missing values
        row = pd.Series({
            'gameId': 123456,
            'personId': 2544,
            'season_year': '2023-24',
            'personName': 'Test Player',
            'points': None,  # Missing value
            'assists': pd.NA,  # Pandas NA
            'fieldGoalsMade': '',  # Empty string
            'rebounds_total': 'invalid'  # Invalid value
        })
        
        result = pipeline._box_score_row_to_dict(row)
        
        # Should handle missing values gracefully
        assert result['game_id'] == 123456
        assert result['person_name'] == 'Test Player'
        assert result['points'] == 0  # Default for missing int
        assert result['assists'] == 0  # Default for missing int
        assert result['field_goals_made'] == 0  # Default for invalid int
    
    def test_get_ingestion_summary_empty(self):
        """Test ingestion summary with empty results."""
        pipeline = NBADataIngestion()
        
        summary = pipeline.get_ingestion_summary([])
        
        assert 'No ingestion results' in summary['message']
    
    def test_get_ingestion_summary_with_results(self):
        """Test ingestion summary with multiple results."""
        pipeline = NBADataIngestion()
        
        # Create mock results
        stats1 = IngestionStats(
            total_rows_read=100,
            rows_inserted=95,
            processing_time_seconds=1.0
        )
        stats2 = IngestionStats(
            total_rows_read=50,
            rows_inserted=48,
            processing_time_seconds=0.5
        )
        
        result1 = IngestionResult(success=True, stats=stats1, errors=[])
        result2 = IngestionResult(success=True, stats=stats2, errors=['warning1'])
        
        summary = pipeline.get_ingestion_summary([result1, result2])
        
        assert summary['files_processed'] == 2
        assert summary['files_successful'] == 2
        assert summary['success_rate'] == 1.0
        assert summary['total_rows_read'] == 150
        assert summary['total_rows_inserted'] == 143
        assert summary['total_errors'] == 1
        assert summary['total_processing_time_seconds'] == 1.5
    
    @pytest.mark.database
    def test_ingest_csv_file_success(self, test_db_connection, sample_box_scores_csv):
        """Test successful CSV file ingestion."""
        pipeline = NBADataIngestion(
            db_connection=test_db_connection,
            batch_size=10,
            validate_data=False,  # Disable validation for faster test
            upsert_mode=False     # Use simple insert
        )
        
        result = pipeline.ingest_csv_file(
            file_path=sample_box_scores_csv,
            data_type='box_scores'
        )
        
        assert isinstance(result, IngestionResult)
        assert result.success is True
        assert result.stats.total_rows_read == 2
        assert result.stats.rows_inserted == 2
        assert len(result.errors) == 0
    
    def test_ingest_nonexistent_file(self):
        """Test ingestion of nonexistent file."""
        pipeline = NBADataIngestion()
        
        result = pipeline.ingest_csv_file('nonexistent_file.csv')
        
        assert result.success is False
        assert result.stats.total_rows_read == 0
        assert len(result.errors) > 0


class TestIngestionFactory:
    """Test cases for ingestion pipeline factory function."""
    
    def test_create_ingestion_pipeline_default(self):
        """Test creating ingestion pipeline with default parameters."""
        pipeline = create_ingestion_pipeline()
        
        assert isinstance(pipeline, NBADataIngestion)
        assert pipeline.batch_size == 1000
        assert pipeline.validate_data is True
        assert pipeline.upsert_mode is True
    
    def test_create_ingestion_pipeline_custom(self):
        """Test creating ingestion pipeline with custom parameters."""
        pipeline = create_ingestion_pipeline(
            batch_size=500,
            validate_data=False,
            upsert_mode=False
        )
        
        assert isinstance(pipeline, NBADataIngestion)
        assert pipeline.batch_size == 500
        assert pipeline.validate_data is False
        assert pipeline.upsert_mode is False
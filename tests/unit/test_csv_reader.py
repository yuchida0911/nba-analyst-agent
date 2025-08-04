"""Unit tests for CSV reader component."""

import pytest
import pandas as pd
from pathlib import Path
from datetime import date

from nba_analyst.ingestion.csv_reader import NBACSVReader, CSVReadResult, create_csv_reader


class TestNBACSVReader:
    """Test cases for NBACSVReader class."""
    
    def test_csv_reader_initialization(self):
        """Test CSV reader initialization with default parameters."""
        reader = NBACSVReader()
        
        assert reader.chunk_size == 1000
        assert reader.validate_data is True
        assert reader.strict_mode is False
        assert 'box_scores' in reader.dtype_converters
        assert 'totals' in reader.dtype_converters
    
    def test_csv_reader_custom_initialization(self):
        """Test CSV reader initialization with custom parameters."""
        reader = NBACSVReader(
            chunk_size=500,
            validate_data=False,
            strict_mode=True
        )
        
        assert reader.chunk_size == 500
        assert reader.validate_data is False
        assert reader.strict_mode is True
    
    def test_detect_file_type_by_filename(self):
        """Test file type detection based on filename."""
        reader = NBACSVReader()
        
        # Test box scores detection
        box_scores_path = Path("regular_season_box_scores_2023.csv")
        assert reader.detect_file_type(box_scores_path) == 'box_scores'
        
        # Test totals detection
        totals_path = Path("regular_season_totals_2023.csv")
        assert reader.detect_file_type(totals_path) == 'totals'
        
        # Test unknown file
        unknown_path = Path("unknown_file.csv")
        # This will return 'unknown' since file doesn't exist for header check
        result = reader.detect_file_type(unknown_path)
        assert result == 'unknown'
    
    def test_parse_date_function(self):
        """Test date parsing utility function."""
        # Test valid date
        assert NBACSVReader._parse_date('2024-01-15') == date(2024, 1, 15)
        
        # Test ISO datetime format
        assert NBACSVReader._parse_date('2024-01-15T00:00:00') == date(2024, 1, 15)
        
        # Test None/empty values
        assert NBACSVReader._parse_date('') is None
        assert NBACSVReader._parse_date(None) is None
        
        # Test invalid date
        assert NBACSVReader._parse_date('invalid-date') is None
    
    def test_read_nonexistent_file(self):
        """Test reading a file that doesn't exist."""
        reader = NBACSVReader()
        result = reader.read_csv_file('nonexistent_file.csv')
        
        assert isinstance(result, CSVReadResult)
        assert result.success is False
        assert result.row_count == 0
        assert len(result.errors) > 0
        assert 'File not found' in result.errors[0]
    
    def test_read_box_scores_csv(self, sample_box_scores_csv):
        """Test reading box scores CSV file."""
        reader = NBACSVReader(validate_data=False)  # Disable validation for unit test
        result = reader.read_csv_file(sample_box_scores_csv, file_type='box_scores')
        
        assert isinstance(result, CSVReadResult)
        assert result.success is True
        assert result.row_count == 2
        assert result.data is not None
        assert len(result.data) == 2
        
        # Check that required columns exist
        assert 'gameId' in result.data.columns
        assert 'personId' in result.data.columns
        assert 'personName' in result.data.columns
    
    def test_read_totals_csv(self, sample_totals_csv):
        """Test reading totals CSV file."""
        reader = NBACSVReader(validate_data=False)  # Disable validation for unit test
        result = reader.read_csv_file(sample_totals_csv, file_type='totals')
        
        assert isinstance(result, CSVReadResult)
        assert result.success is True
        assert result.row_count == 1
        assert result.data is not None
        assert len(result.data) == 1
        
        # Check that required columns exist
        assert 'GAME_ID' in result.data.columns
        assert 'TEAM_ID' in result.data.columns
        assert 'TEAM_NAME' in result.data.columns
    
    def test_read_csv_with_max_rows(self, sample_box_scores_csv):
        """Test reading CSV with max_rows parameter."""
        reader = NBACSVReader(validate_data=False)
        result = reader.read_csv_file(sample_box_scores_csv, max_rows=1)
        
        assert result.success is True
        assert result.row_count == 1
        assert len(result.data) == 1
    
    def test_data_type_conversions(self, sample_box_scores_csv):
        """Test that data type conversions are applied correctly."""
        reader = NBACSVReader(validate_data=True)
        result = reader.read_csv_file(sample_box_scores_csv, file_type='box_scores')
        
        assert result.success is True
        data = result.data
        
        # Check integer conversions
        assert data['gameId'].dtype in ['int64', 'int32']
        assert data['personId'].dtype in ['int64', 'int32']
        assert data['fieldGoalsMade'].dtype in ['int64', 'int32']
        
        # Check float conversions
        assert data['fieldGoalsPercentage'].dtype == 'float64'
        
        # Check string conversions
        assert data['season_year'].dtype == 'object'
        assert data['minutes'].dtype == 'object'  # Kept as string for MM:SS format
    
    def test_validation_basic_rules(self, sample_box_scores_csv):
        """Test basic validation rules for box scores."""
        reader = NBACSVReader(validate_data=True, strict_mode=False)
        result = reader.read_csv_file(sample_box_scores_csv, file_type='box_scores')
        
        # Should succeed even with warnings in non-strict mode
        assert result.success is True
        # May have validation warnings but not errors for good data
        assert len(result.errors) == 0 or 'Missing required columns' not in result.errors[0]
    
    def test_unknown_file_type_error(self, temp_csv_file):
        """Test error handling for unknown file type."""
        # Create a CSV with unknown structure
        with open(temp_csv_file, 'w') as f:
            f.write("unknown_col1,unknown_col2\nvalue1,value2\n")
        
        reader = NBACSVReader()
        result = reader.read_csv_file(temp_csv_file, file_type='unknown_type')
        
        assert result.success is False
        assert 'Unknown file type' in result.errors[0]


class TestCSVReaderFactory:
    """Test cases for CSV reader factory function."""
    
    def test_create_csv_reader_default(self):
        """Test creating CSV reader with default parameters."""
        reader = create_csv_reader()
        
        assert isinstance(reader, NBACSVReader)
        assert reader.chunk_size == 1000
        assert reader.validate_data is True
        assert reader.strict_mode is False
    
    def test_create_csv_reader_custom(self):
        """Test creating CSV reader with custom parameters."""
        reader = create_csv_reader(
            chunk_size=2000,
            validate_data=False,
            strict_mode=True
        )
        
        assert isinstance(reader, NBACSVReader)
        assert reader.chunk_size == 2000
        assert reader.validate_data is False
        assert reader.strict_mode is True


class TestCSVReadResult:
    """Test cases for CSVReadResult data class."""
    
    def test_csv_read_result_creation(self):
        """Test creating CSVReadResult instance."""
        result = CSVReadResult(
            success=True,
            row_count=100,
            data=pd.DataFrame({'col1': [1, 2, 3]}),
            errors=['warning1', 'warning2'],
            file_path=Path('test.csv')
        )
        
        assert result.success is True
        assert result.row_count == 100
        assert len(result.data) == 3
        assert len(result.errors) == 2
        assert result.file_path == Path('test.csv')
    
    def test_csv_read_result_post_init(self):
        """Test CSVReadResult post-init behavior."""
        # Test that errors list is initialized if None
        result = CSVReadResult(success=True, row_count=0)
        assert result.errors == []
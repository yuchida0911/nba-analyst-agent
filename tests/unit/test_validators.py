"""Unit tests for data validation component."""

import pytest
import pandas as pd
from pathlib import Path

from nba_analyst.ingestion.validators import (
    NBADataValidator, ValidationResult, ValidationError, ValidationSeverity, create_validator
)


class TestValidationError:
    """Test cases for ValidationError class."""
    
    def test_validation_error_creation(self):
        """Test creating ValidationError instance."""
        error = ValidationError(
            field="points",
            message="Negative points value",
            severity=ValidationSeverity.ERROR,
            row_index=5,
            value=-10,
            rule="non_negative_stats"
        )
        
        assert error.field == "points"
        assert error.message == "Negative points value"
        assert error.severity == ValidationSeverity.ERROR
        assert error.row_index == 5
        assert error.value == -10
        assert error.rule == "non_negative_stats"
    
    def test_validation_error_string_representation(self):
        """Test ValidationError string representation."""
        error = ValidationError(
            field="rebounds",
            message="Invalid total",
            severity=ValidationSeverity.WARNING,
            row_index=10
        )
        
        error_str = str(error)
        assert "[WARNING]" in error_str
        assert "rebounds" in error_str
        assert "(row 10)" in error_str
        assert "Invalid total" in error_str


class TestValidationResult:
    """Test cases for ValidationResult class."""
    
    def test_validation_result_creation(self):
        """Test creating ValidationResult instance."""
        errors = [ValidationError("field1", "error1", ValidationSeverity.ERROR)]
        warnings = [ValidationError("field2", "warning1", ValidationSeverity.WARNING)]
        
        result = ValidationResult(
            success=False,
            total_rows=100,
            errors=errors,
            warnings=warnings,
            summary={"error": 1, "warning": 1}
        )
        
        assert result.success is False
        assert result.total_rows == 100
        assert result.error_count == 1
        assert result.warning_count == 1
        assert result.is_valid is False
    
    def test_validation_result_properties(self):
        """Test ValidationResult computed properties."""
        result = ValidationResult(
            success=True,
            total_rows=50,
            errors=[],
            warnings=[],
            summary={}
        )
        
        assert result.error_count == 0
        assert result.warning_count == 0
        assert result.is_valid is True


class TestNBADataValidator:
    """Test cases for NBADataValidator class."""
    
    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = NBADataValidator(strict_mode=False, max_errors=50)
        
        assert validator.strict_mode is False
        assert validator.max_errors == 50
        assert 'box_scores' in validator.validation_rules
        assert 'totals' in validator.validation_rules
    
    def test_validate_unknown_data_type(self):
        """Test validation with unknown data type."""
        validator = NBADataValidator()
        df = pd.DataFrame({'col1': [1, 2, 3]})
        
        result = validator.validate_dataframe(df, 'unknown_type')
        
        assert result.success is False
        assert result.error_count > 0
        assert any('Unknown data type' in str(error) for error in result.errors)
    
    def test_validate_valid_box_scores_data(self, sample_box_scores_data):
        """Test validation of valid box scores data."""
        validator = NBADataValidator(strict_mode=False, max_errors=100)
        
        result = validator.validate_dataframe(sample_box_scores_data, 'box_scores')
        
        # Should succeed (may have warnings but no critical errors)
        assert result.total_rows == 2
        # Check that critical validation didn't fail
        assert not any(error.severity == ValidationSeverity.CRITICAL for error in result.errors)
    
    def test_validate_box_scores_missing_required_fields(self):
        """Test validation with missing required fields."""
        validator = NBADataValidator()
        
        # Create DataFrame missing required fields
        df = pd.DataFrame({
            'someField': [1, 2, 3],
            'anotherField': ['a', 'b', 'c']
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        assert result.error_count > 0
        # Should have errors about missing required fields
        assert any('missing' in str(error).lower() for error in result.errors)
    
    def test_validate_shooting_consistency_errors(self):
        """Test validation of shooting consistency."""
        validator = NBADataValidator()
        
        # Create data with shooting inconsistencies
        df = pd.DataFrame({
            'gameId': [123, 124],
            'personId': [456, 457],
            'season_year': ['2023-24', '2023-24'],
            'game_date': ['2024-01-15', '2024-01-15'],
            'teamId': [1, 1],
            'personName': ['Player A', 'Player B'],
            'points': [20, 15],
            'fieldGoalsMade': [10, 8],  # FGM > FGA in second row
            'fieldGoalsAttempted': [8, 6],  # This will cause validation error
            'threePointersMade': [3, 2],
            'fieldGoalsMade': [10, 8]  # Duplicate key, will use last value
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        # Should have shooting consistency errors
        assert result.error_count > 0 or result.warning_count > 0
    
    def test_validate_rebounds_consistency(self):
        """Test validation of rebounds consistency."""
        validator = NBADataValidator()
        
        # Create data with rebounds inconsistency
        df = pd.DataFrame({
            'gameId': [123],
            'personId': [456],
            'season_year': ['2023-24'],
            'game_date': ['2024-01-15'],
            'teamId': [1],
            'personName': ['Player A'],
            'points': [20],
            'reboundsOffensive': [5],
            'reboundsDefensive': [8],
            'reboundsTotal': [10]  # Should be 13 (5+8), not 10
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        # Should have rebounds consistency errors
        assert result.error_count > 0
        assert any('rebounds' in str(error).lower() for error in result.errors)
    
    def test_validate_non_negative_stats(self):
        """Test validation of non-negative statistics."""
        validator = NBADataValidator()
        
        # Create data with negative stats
        df = pd.DataFrame({
            'gameId': [123],
            'personId': [456],
            'season_year': ['2023-24'],
            'game_date': ['2024-01-15'],
            'teamId': [1],
            'personName': ['Player A'],
            'points': [-5],  # Negative points
            'fieldGoalsMade': [-2],  # Negative FGM
            'assists': [3]
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        # Should have non-negative validation errors
        assert result.error_count > 0
        assert any('negative' in str(error).lower() for error in result.errors)
    
    def test_validate_season_format(self):
        """Test validation of season year format."""
        validator = NBADataValidator()
        
        # Create data with invalid season format
        df = pd.DataFrame({
            'gameId': [123],
            'personId': [456],
            'season_year': ['2023'],  # Should be '2023-24'
            'game_date': ['2024-01-15'],
            'teamId': [1],
            'personName': ['Player A'],
            'points': [20]
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        # Should have season format warnings
        assert result.warning_count > 0
        assert any('season' in str(warning).lower() for warning in result.warnings)
    
    def test_validate_totals_data(self, sample_totals_data):
        """Test validation of totals data."""
        validator = NBADataValidator()
        
        result = validator.validate_dataframe(sample_totals_data, 'totals')
        
        # Should succeed with valid totals data
        assert result.total_rows == 1
        # May have warnings but shouldn't have critical errors
        assert not any(error.severity == ValidationSeverity.CRITICAL for error in result.errors)
    
    def test_validate_totals_missing_required_fields(self):
        """Test totals validation with missing required fields."""
        validator = NBADataValidator()
        
        # Create DataFrame missing required totals fields
        df = pd.DataFrame({
            'someField': [1, 2],
            'anotherField': ['a', 'b']
        })
        
        result = validator.validate_dataframe(df, 'totals')
        
        assert result.error_count > 0
        assert any('missing' in str(error).lower() for error in result.errors)
    
    def test_validate_win_loss_format(self):
        """Test validation of W/L format in totals."""
        validator = NBADataValidator()
        
        # Create data with invalid W/L values
        df = pd.DataFrame({
            'GAME_ID': [123],
            'TEAM_ID': [456],
            'SEASON_YEAR': ['2023-24'],
            'TEAM_NAME': ['Test Team'],
            'PTS': [100],
            'WL': ['X']  # Should be 'W' or 'L'
        })
        
        result = validator.validate_dataframe(df, 'totals')
        
        # Should have W/L format errors
        assert result.error_count > 0
        assert any('w/l' in str(error).lower() or 'wl' in str(error).lower() for error in result.errors)
    
    def test_max_errors_limit(self):
        """Test that validation stops at max_errors limit."""
        validator = NBADataValidator(max_errors=2)
        
        # Create data with many errors (negative stats)
        df = pd.DataFrame({
            'gameId': [123, 124, 125, 126],
            'personId': [456, 457, 458, 459],
            'season_year': ['2023-24'] * 4,
            'game_date': ['2024-01-15'] * 4,
            'teamId': [1] * 4,
            'personName': ['Player A', 'Player B', 'Player C', 'Player D'],
            'points': [-5, -10, -15, -20],  # All negative
            'fieldGoalsMade': [-1, -2, -3, -4]  # All negative
        })
        
        result = validator.validate_dataframe(df, 'box_scores')
        
        # Should stop at max_errors limit
        assert result.error_count <= validator.max_errors


class TestValidatorFactory:
    """Test cases for validator factory function."""
    
    def test_create_validator_default(self):
        """Test creating validator with default parameters."""
        validator = create_validator()
        
        assert isinstance(validator, NBADataValidator)
        assert validator.strict_mode is False
        assert validator.max_errors == 100
    
    def test_create_validator_custom(self):
        """Test creating validator with custom parameters."""
        validator = create_validator(
            strict_mode=True,
            max_errors=50
        )
        
        assert isinstance(validator, NBADataValidator)
        assert validator.strict_mode is True
        assert validator.max_errors == 50
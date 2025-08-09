"""
Data validation framework for NBA Analyst Agent.

This module provides comprehensive data validation based on the JSON schema
definitions and business rules for NBA data.
"""

import json
import logging
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from dataclasses import dataclass
from enum import Enum

import pandas as pd

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationError:
    """Single validation error with details."""
    
    field: str
    message: str
    severity: ValidationSeverity
    row_index: Optional[int] = None
    value: Any = None
    rule: Optional[str] = None
    
    def __str__(self) -> str:
        location = f" (row {self.row_index})" if self.row_index is not None else ""
        return f"[{self.severity.value.upper()}] {self.field}{location}: {self.message}"


@dataclass
class ValidationResult:
    """Result of data validation."""
    
    success: bool
    total_rows: int
    errors: List[ValidationError]
    warnings: List[ValidationError]
    summary: Dict[str, int]
    
    @property
    def error_count(self) -> int:
        """Get total error count."""
        return len(self.errors)
    
    @property
    def warning_count(self) -> int:
        """Get total warning count."""
        return len(self.warnings)
    
    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors, warnings OK)."""
        return self.error_count == 0


class NBADataValidator:
    """Comprehensive NBA data validator based on JSON schemas."""
    
    def __init__(self, 
                 schema_dir: Optional[Path] = None,
                 strict_mode: bool = False,
                 max_errors: int = 100):
        """
        Initialize NBA data validator.
        
        Args:
            schema_dir: Directory containing JSON schema files
            strict_mode: Whether to treat warnings as errors
            max_errors: Maximum number of errors to collect before stopping
        """
        self.strict_mode = strict_mode
        self.max_errors = max_errors
        
        # Load schemas
        if schema_dir is None:
            schema_dir = Path(__file__).parent.parent.parent.parent / "data"
        
        self.schemas = self._load_schemas(schema_dir)
        
        # Validation rules
        self.validation_rules = {
            'box_scores': self._get_box_scores_rules(),
            'totals': self._get_totals_rules()
        }
    
    def _load_schemas(self, schema_dir: Path) -> Dict[str, Dict]:
        """Load JSON schemas from directory."""
        schemas = {}
        
        try:
            # Load box scores schema
            box_scores_path = schema_dir / "box_scores_schema.json"
            if box_scores_path.exists():
                with open(box_scores_path, 'r') as f:
                    schemas['box_scores'] = json.load(f)
                logger.info("Loaded box_scores schema")
            
            # Load totals schema
            totals_path = schema_dir / "totals_schema.json"
            if totals_path.exists():
                with open(totals_path, 'r') as f:
                    schemas['totals'] = json.load(f)
                logger.info("Loaded totals schema")
                
        except Exception as e:
            logger.warning(f"Failed to load schemas: {e}")
        
        return schemas
    
    def _get_box_scores_rules(self) -> Dict[str, List[Callable]]:
        """Get validation rules for box scores data."""
        return {
            'required_fields': [self._validate_required_fields_box_scores],
            'data_types': [self._validate_data_types_box_scores],
            'business_rules': [
                self._validate_shooting_consistency,
                self._validate_rebounds_consistency,
                self._validate_non_negative_stats,
                self._validate_season_format,
                self._validate_team_tricode,
                self._validate_minutes_format
            ],
            'cross_field': [
                self._validate_points_calculation,
                self._validate_three_point_subset,
                self._validate_dnp_consistency
            ]
        }
    
    def _get_totals_rules(self) -> Dict[str, List[Callable]]:
        """Get validation rules for totals data."""
        return {
            'required_fields': [self._validate_required_fields_totals],
            'data_types': [self._validate_data_types_totals],
            'business_rules': [
                self._validate_win_loss_format,
                self._validate_team_abbreviation_format,
                self._validate_non_negative_team_stats
            ]
        }
    
    def validate_dataframe(self, 
                          df: pd.DataFrame, 
                          data_type: str) -> ValidationResult:
        """
        Validate a pandas DataFrame against NBA data rules.
        
        Args:
            df: DataFrame to validate
            data_type: Type of data ('box_scores' or 'totals')
            
        Returns:
            ValidationResult with errors and warnings
        """
        logger.info(f"Validating {data_type} DataFrame with {len(df)} rows")
        
        errors = []
        warnings = []
        
        if data_type not in self.validation_rules:
            errors.append(ValidationError(
                field="data_type",
                message=f"Unknown data type: {data_type}",
                severity=ValidationSeverity.CRITICAL
            ))
            return ValidationResult(
                success=False,
                total_rows=len(df),
                errors=errors,
                warnings=warnings,
                summary={"critical": 1}
            )
        
        # Run validation rules
        rules = self.validation_rules[data_type]
        
        for rule_category, rule_functions in rules.items():
            for rule_func in rule_functions:
                try:
                    rule_errors = rule_func(df)
                    
                    for error in rule_errors:
                        if error.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]:
                            errors.append(error)
                        else:
                            warnings.append(error)
                        
                        # Stop if too many errors
                        if len(errors) >= self.max_errors:
                            logger.warning(f"Stopping validation after {self.max_errors} errors")
                            break
                    
                    if len(errors) >= self.max_errors:
                        break
                        
                except Exception as e:
                    logger.error(f"Validation rule {rule_func.__name__} failed: {e}")
                    errors.append(ValidationError(
                        field="validation_system",
                        message=f"Internal validation error: {str(e)}",
                        severity=ValidationSeverity.CRITICAL,
                        rule=rule_func.__name__
                    ))
            
            if len(errors) >= self.max_errors:
                break
        
        # Create summary
        summary = {}
        for severity in ValidationSeverity:
            count = sum(1 for e in errors + warnings if e.severity == severity)
            if count > 0:
                summary[severity.value] = count
        
        success = len(errors) == 0 and (not self.strict_mode or len(warnings) == 0)
        
        return ValidationResult(
            success=success,
            total_rows=len(df),
            errors=errors,
            warnings=warnings,
            summary=summary
        )
    
    # Box scores validation rules
    
    def _validate_required_fields_box_scores(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate required fields for box scores."""
        errors = []
        
        required_fields = ['gameId', 'personId', 'season_year', 'game_date', 
                          'teamId', 'personName', 'points']
        
        for field in required_fields:
            if field not in df.columns:
                errors.append(ValidationError(
                    field=field,
                    message=f"Required field '{field}' is missing",
                    severity=ValidationSeverity.CRITICAL
                ))
            else:
                # Check for null values in required fields
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    errors.append(ValidationError(
                        field=field,
                        message=f"Required field '{field}' has {null_count} null values",
                        severity=ValidationSeverity.ERROR
                    ))
        
        return errors
    
    def _validate_data_types_box_scores(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate data types for box scores."""
        errors = []
        
        expected_types = {
            'gameId': ['int64', 'int32'],
            'personId': ['int64', 'int32'],
            'teamId': ['int64', 'int32'],
            'points': ['int64', 'int32'],
            'assists': ['int64', 'int32'],
            'season_year': ['object', 'string']
        }
        
        for field, valid_types in expected_types.items():
            if field in df.columns:
                actual_type = str(df[field].dtype)
                if actual_type not in valid_types:
                    errors.append(ValidationError(
                        field=field,
                        message=f"Expected type {valid_types}, got {actual_type}",
                        severity=ValidationSeverity.WARNING
                    ))
        
        return errors
    
    def _validate_shooting_consistency(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate shooting statistics consistency."""
        errors = []
        
        # Field goals
        if all(col in df.columns for col in ['fieldGoalsMade', 'fieldGoalsAttempted']):
            invalid_rows = df['fieldGoalsMade'] > df['fieldGoalsAttempted']
            for idx in df[invalid_rows].index:
                errors.append(ValidationError(
                    field="fieldGoals",
                    message=f"FGM ({df.loc[idx, 'fieldGoalsMade']}) > FGA ({df.loc[idx, 'fieldGoalsAttempted']})",
                    severity=ValidationSeverity.ERROR,
                    row_index=idx
                ))
        
        # Three-pointers vs field goals
        if all(col in df.columns for col in ['threePointersMade', 'fieldGoalsMade']):
            invalid_rows = df['threePointersMade'] > df['fieldGoalsMade']
            for idx in df[invalid_rows].index:
                errors.append(ValidationError(
                    field="threePointers",
                    message=f"3PM ({df.loc[idx, 'threePointersMade']}) > FGM ({df.loc[idx, 'fieldGoalsMade']})",
                    severity=ValidationSeverity.ERROR,
                    row_index=idx
                ))
        
        return errors
    
    def _validate_rebounds_consistency(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate rebounds consistency."""
        errors = []
        
        required_cols = ['reboundsOffensive', 'reboundsDefensive', 'reboundsTotal']
        if all(col in df.columns for col in required_cols):
            calculated_total = df['reboundsOffensive'] + df['reboundsDefensive']
            mismatched_rows = df['reboundsTotal'] != calculated_total
            
            for idx in df[mismatched_rows].index:
                errors.append(ValidationError(
                    field="rebounds",
                    message=f"Total rebounds ({df.loc[idx, 'reboundsTotal']}) != OREB ({df.loc[idx, 'reboundsOffensive']}) + DREB ({df.loc[idx, 'reboundsDefensive']})",
                    severity=ValidationSeverity.ERROR,
                    row_index=idx
                ))
        
        return errors
    
    def _validate_non_negative_stats(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate that statistics are non-negative."""
        errors = []
        
        stat_fields = ['fieldGoalsMade', 'fieldGoalsAttempted', 'threePointersMade',
                      'threePointersAttempted', 'freeThrowsMade', 'freeThrowsAttempted',
                      'reboundsOffensive', 'reboundsDefensive', 'reboundsTotal',
                      'assists', 'steals', 'blocks', 'turnovers', 'foulsPersonal', 'points']
        
        for field in stat_fields:
            if field in df.columns:
                negative_rows = df[field] < 0
                if negative_rows.any():
                    for idx in df[negative_rows].index:
                        errors.append(ValidationError(
                            field=field,
                            message=f"Negative value: {df.loc[idx, field]}",
                            severity=ValidationSeverity.ERROR,
                            row_index=idx,
                            value=df.loc[idx, field]
                        ))
        
        return errors
    
    def _validate_season_format(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate season year format (YYYY-YY)."""
        errors = []
        
        if 'season_year' in df.columns:
            season_pattern = re.compile(r'^\d{4}-\d{2}$')
            invalid_seasons = ~df['season_year'].astype(str).str.match(season_pattern)
            
            for idx in df[invalid_seasons].index:
                errors.append(ValidationError(
                    field="season_year",
                    message=f"Invalid season format: '{df.loc[idx, 'season_year']}' (expected YYYY-YY)",
                    severity=ValidationSeverity.WARNING,
                    row_index=idx,
                    value=df.loc[idx, 'season_year']
                ))
        
        return errors
    
    def _validate_team_tricode(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate team tricode format (3 uppercase letters)."""
        errors = []
        
        if 'teamTricode' in df.columns:
            tricode_pattern = re.compile(r'^[A-Z]{3}$')
            invalid_tricodes = ~df['teamTricode'].astype(str).str.match(tricode_pattern)
            
            for idx in df[invalid_tricodes].index:
                errors.append(ValidationError(
                    field="teamTricode",
                    message=f"Invalid tricode format: '{df.loc[idx, 'teamTricode']}' (expected 3 uppercase letters)",
                    severity=ValidationSeverity.WARNING,
                    row_index=idx,
                    value=df.loc[idx, 'teamTricode']
                ))
        
        return errors
    
    def _validate_minutes_format(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate minutes format (MM:SS or decimal)."""
        errors = []
        
        if 'minutes' in df.columns:
            # Check for valid formats: "MM:SS", "0", or empty for DNP
            minutes_pattern = re.compile(r'^(\d{1,2}:\d{2}|\d+\.?\d*|0?)$')
            
            for idx, minutes_val in df['minutes'].items():
                if pd.notna(minutes_val) and str(minutes_val).strip():
                    if not minutes_pattern.match(str(minutes_val)):
                        errors.append(ValidationError(
                            field="minutes",
                            message=f"Invalid minutes format: '{minutes_val}' (expected MM:SS or decimal)",
                            severity=ValidationSeverity.WARNING,
                            row_index=idx,
                            value=minutes_val
                        ))
        
        return errors
    
    def _validate_points_calculation(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate points calculation consistency."""
        errors = []
        
        required_cols = ['fieldGoalsMade', 'threePointersMade', 'freeThrowsMade', 'points']
        if all(col in df.columns for col in required_cols):
            calculated_points = (
                (df['fieldGoalsMade'] - df['threePointersMade']) * 2 +
                df['threePointersMade'] * 3 +
                df['freeThrowsMade']
            )
            
            mismatched_rows = df['points'] != calculated_points
            
            for idx in df[mismatched_rows].index:
                errors.append(ValidationError(
                    field="points",
                    message=f"Points calculation mismatch: reported {df.loc[idx, 'points']}, calculated {calculated_points.loc[idx]}",
                    severity=ValidationSeverity.WARNING,
                    row_index=idx
                ))
        
        return errors
    
    def _validate_three_point_subset(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate three-pointers are subset of field goals."""
        errors = []
        
        if all(col in df.columns for col in ['threePointersAttempted', 'fieldGoalsAttempted']):
            invalid_rows = df['threePointersAttempted'] > df['fieldGoalsAttempted']
            
            for idx in df[invalid_rows].index:
                errors.append(ValidationError(
                    field="threePointers",
                    message=f"3PA ({df.loc[idx, 'threePointersAttempted']}) > FGA ({df.loc[idx, 'fieldGoalsAttempted']})",
                    severity=ValidationSeverity.ERROR,
                    row_index=idx
                ))
        
        return errors
    
    def _validate_dnp_consistency(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate DNP (Did Not Play) consistency."""
        errors = []
        
        if 'minutes' in df.columns and 'points' in df.columns:
            # Players with 0 minutes should have 0 stats
            zero_minutes = (df['minutes'].astype(str).isin(['0', '0:00', '']) | df['minutes'].isnull())
            
            stat_cols = ['points', 'assists', 'rebounds', 'steals', 'blocks']
            available_stat_cols = [col for col in stat_cols if col in df.columns]
            
            for col in available_stat_cols:
                dnp_with_stats = zero_minutes & (df[col] > 0)
                
                for idx in df[dnp_with_stats].index:
                    errors.append(ValidationError(
                        field="dnp_consistency",
                        message=f"Player with 0 minutes has {col}: {df.loc[idx, col]}",
                        severity=ValidationSeverity.WARNING,
                        row_index=idx
                    ))
        
        return errors
    
    # Totals validation rules
    
    def _validate_required_fields_totals(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate required fields for totals."""
        errors = []
        
        required_fields = ['GAME_ID', 'TEAM_ID', 'SEASON_YEAR', 'TEAM_NAME', 'PTS', 'WL']
        
        for field in required_fields:
            if field not in df.columns:
                errors.append(ValidationError(
                    field=field,
                    message=f"Required field '{field}' is missing",
                    severity=ValidationSeverity.CRITICAL
                ))
        
        return errors
    
    def _validate_data_types_totals(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate data types for totals."""
        errors = []
        
        expected_types = {
            'GAME_ID': ['int64', 'int32'],
            'TEAM_ID': ['int64', 'int32'],
            'PTS': ['int64', 'int32'],
            'WL': ['object', 'string']
        }
        
        for field, valid_types in expected_types.items():
            if field in df.columns:
                actual_type = str(df[field].dtype)
                if actual_type not in valid_types:
                    errors.append(ValidationError(
                        field=field,
                        message=f"Expected type {valid_types}, got {actual_type}",
                        severity=ValidationSeverity.WARNING
                    ))
        
        return errors
    
    def _validate_win_loss_format(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate W/L format."""
        errors = []
        
        if 'WL' in df.columns:
            invalid_wl = ~df['WL'].isin(['W', 'L'])
            
            for idx in df[invalid_wl].index:
                errors.append(ValidationError(
                    field="WL",
                    message=f"Invalid W/L value: '{df.loc[idx, 'WL']}' (must be 'W' or 'L')",
                    severity=ValidationSeverity.ERROR,
                    row_index=idx,
                    value=df.loc[idx, 'WL']
                ))
        
        return errors
    
    def _validate_team_abbreviation_format(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate team abbreviation format."""
        errors = []
        
        if 'TEAM_ABBREVIATION' in df.columns:
            abbrev_pattern = re.compile(r'^[A-Z]{3}$')
            invalid_abbrevs = ~df['TEAM_ABBREVIATION'].astype(str).str.match(abbrev_pattern)
            
            for idx in df[invalid_abbrevs].index:
                errors.append(ValidationError(
                    field="TEAM_ABBREVIATION",
                    message=f"Invalid abbreviation: '{df.loc[idx, 'TEAM_ABBREVIATION']}' (expected 3 uppercase letters)",
                    severity=ValidationSeverity.WARNING,
                    row_index=idx
                ))
        
        return errors
    
    def _validate_non_negative_team_stats(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate team statistics are non-negative."""
        errors = []
        
        stat_fields = ['PTS', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA',
                      'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'PF']
        
        for field in stat_fields:
            if field in df.columns:
                negative_rows = df[field] < 0
                if negative_rows.any():
                    for idx in df[negative_rows].index:
                        errors.append(ValidationError(
                            field=field,
                            message=f"Negative team stat: {df.loc[idx, field]}",
                            severity=ValidationSeverity.ERROR,
                            row_index=idx
                        ))
        
        return errors


def create_validator(schema_dir: Optional[Path] = None,
                    strict_mode: bool = False,
                    max_errors: int = 100) -> NBADataValidator:
    """
    Create a configured NBA data validator.
    
    Args:
        schema_dir: Directory containing JSON schemas
        strict_mode: Whether to treat warnings as errors
        max_errors: Maximum errors to collect
        
    Returns:
        Configured NBADataValidator instance
    """
    return NBADataValidator(
        schema_dir=schema_dir,
        strict_mode=strict_mode,
        max_errors=max_errors
    )
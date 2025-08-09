"""
CSV reading utilities for NBA Analyst Agent.

This module provides utilities for reading and parsing NBA CSV data files
with proper error handling, data type conversion, and validation.
"""

import csv
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CSVReadResult:
    """Result of CSV reading operation."""
    
    success: bool
    row_count: int
    data: Optional[pd.DataFrame] = None
    errors: List[str] = None
    file_path: Optional[Path] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class NBACSVReader:
    """CSV reader specifically designed for NBA data files."""
    
    def __init__(self, 
                 chunk_size: int = 1000,
                 validate_data: bool = True,
                 strict_mode: bool = False):
        """
        Initialize NBA CSV reader.
        
        Args:
            chunk_size: Number of rows to read at a time for large files
            validate_data: Whether to validate data during reading
            strict_mode: Whether to fail on any data validation errors
        """
        self.chunk_size = chunk_size
        self.validate_data = validate_data
        self.strict_mode = strict_mode
        
        # Data type converters for common NBA data fields
        self.dtype_converters = {
            'box_scores': {
                'season_year': str,
                'game_date': self._parse_date,
                'gameId': int,
                'teamId': int,
                'personId': int,
                'minutes': str,  # Keep as string for MM:SS format
                'fieldGoalsMade': int,
                'fieldGoalsAttempted': int,
                'fieldGoalsPercentage': float,
                'threePointersMade': int,
                'threePointersAttempted': int,
                'threePointersPercentage': float,
                'freeThrowsMade': int,
                'freeThrowsAttempted': int,
                'freeThrowsPercentage': float,
                'reboundsOffensive': int,
                'reboundsDefensive': int,
                'reboundsTotal': int,
                'assists': int,
                'steals': int,
                'blocks': int,
                'turnovers': int,
                'foulsPersonal': int,
                'points': int,
                'plusMinusPoints': int,
            },
            'totals': {
                'SEASON_YEAR': str,
                'TEAM_ID': int,
                'GAME_ID': int,
                'GAME_DATE': self._parse_datetime,
                'MIN': float,
                'FGM': int,
                'FGA': int,
                'FG_PCT': float,
                'FG3M': int,
                'FG3A': int,
                'FG3_PCT': float,
                'FTM': int,
                'FTA': int,
                'FT_PCT': float,
                'OREB': int,
                'DREB': int,
                'REB': int,
                'AST': int,
                'TOV': float,
                'STL': int,
                'BLK': int,
                'BLKA': int,
                'PF': int,
                'PFD': int,
                'PTS': int,
                'PLUS_MINUS': float,
                'AVAILABLE_FLAG': float,
            }
        }
    
    @staticmethod
    def _parse_date(date_str: str) -> Optional[date]:
        """Parse date string to date object."""
        if not date_str or pd.isna(date_str):
            return None
        
        try:
            # Handle various date formats
            if 'T' in str(date_str):
                # ISO format with time
                return datetime.fromisoformat(str(date_str).replace('T', ' ')).date()
            else:
                # Simple date format
                return datetime.strptime(str(date_str), '%Y-%m-%d').date()
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
    
    @staticmethod
    def _parse_datetime(datetime_str: str) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not datetime_str or pd.isna(datetime_str):
            return None
        
        try:
            # Handle ISO format
            return datetime.fromisoformat(str(datetime_str).replace('T', ' '))
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse datetime '{datetime_str}': {e}")
            return None
    
    def detect_file_type(self, file_path: Path) -> str:
        """
        Detect the type of NBA CSV file based on filename and header.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            File type ('box_scores' or 'totals')
        """
        filename = file_path.name.lower()
        
        if 'box_scores' in filename:
            return 'box_scores'
        elif 'totals' in filename:
            return 'totals'
        
        # Check header to determine type
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                header = f.readline().strip().lower()
                if 'personid' in header and 'personname' in header:
                    return 'box_scores'
                elif 'team_id' in header and 'game_id' in header:
                    return 'totals'
        except Exception as e:
            logger.warning(f"Failed to detect file type for {file_path}: {e}")
        
        return 'unknown'
    
    def read_csv_file(self, 
                      file_path: Union[str, Path], 
                      file_type: Optional[str] = None,
                      max_rows: Optional[int] = None) -> CSVReadResult:
        """
        Read NBA CSV file with appropriate parsing and validation.
        
        Args:
            file_path: Path to the CSV file
            file_type: Type of file ('box_scores' or 'totals'), auto-detected if None
            max_rows: Maximum number of rows to read (for testing)
            
        Returns:
            CSVReadResult with data and metadata
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            return CSVReadResult(
                success=False,
                row_count=0,
                errors=[f"File not found: {file_path}"],
                file_path=file_path
            )
        
        # Detect file type if not provided
        if file_type is None:
            file_type = self.detect_file_type(file_path)
        
        if file_type not in self.dtype_converters:
            return CSVReadResult(
                success=False,
                row_count=0,
                errors=[f"Unknown file type: {file_type}"],
                file_path=file_path
            )
        
        logger.info(f"Reading {file_type} CSV file: {file_path}")
        
        try:
            # Read CSV with pandas
            read_kwargs = {
                'encoding': 'utf-8',
                'low_memory': False,
                'na_values': ['', 'NULL', 'null', 'N/A', 'n/a'],
                'keep_default_na': True,
            }
            
            if max_rows:
                read_kwargs['nrows'] = max_rows
            
            df = pd.read_csv(file_path, **read_kwargs)
            
            logger.info(f"Raw CSV read: {len(df)} rows, {len(df.columns)} columns")
            
            # Apply data type conversions
            if self.validate_data:
                df = self._apply_data_conversions(df, file_type)
                logger.info(f"Data conversion completed: {len(df)} rows retained")
            
            # Validate data if requested
            errors = []
            if self.validate_data:
                errors = self._validate_data(df, file_type)
                if errors and self.strict_mode:
                    return CSVReadResult(
                        success=False,
                        row_count=len(df),
                        errors=errors,
                        file_path=file_path
                    )
            
            return CSVReadResult(
                success=True,
                row_count=len(df),
                data=df,
                errors=errors,
                file_path=file_path
            )
            
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            return CSVReadResult(
                success=False,
                row_count=0,
                errors=[f"CSV reading error: {str(e)}"],
                file_path=file_path
            )
    
    def _apply_data_conversions(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """Apply data type conversions to DataFrame."""
        converters = self.dtype_converters.get(file_type, {})
        
        for column, converter in converters.items():
            if column in df.columns:
                try:
                    if callable(converter):
                        # Custom converter function
                        df[column] = df[column].apply(converter)
                    else:
                        # Built-in type converter
                        df[column] = df[column].astype(converter)
                except Exception as e:
                    logger.warning(f"Failed to convert column '{column}' to {converter}: {e}")
        
        return df
    
    def _validate_data(self, df: pd.DataFrame, file_type: str) -> List[str]:
        """Validate DataFrame data according to business rules."""
        errors = []
        
        if file_type == 'box_scores':
            errors.extend(self._validate_box_scores(df))
        elif file_type == 'totals':
            errors.extend(self._validate_totals(df))
        
        return errors
    
    def _validate_box_scores(self, df: pd.DataFrame) -> List[str]:
        """Validate box scores data."""
        errors = []
        
        # Check required columns
        required_columns = ['gameId', 'personId', 'season_year', 'game_date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return errors
        
        # Check for duplicate primary keys
        duplicates = df.duplicated(subset=['gameId', 'personId'])
        if duplicates.any():
            errors.append(f"Found {duplicates.sum()} duplicate gameId/personId combinations")
        
        # Validate shooting statistics
        shooting_errors = self._validate_shooting_stats(df)
        errors.extend(shooting_errors)
        
        # Validate rebounds
        rebound_errors = self._validate_rebounds(df)
        errors.extend(rebound_errors)
        
        return errors
    
    def _validate_totals(self, df: pd.DataFrame) -> List[str]:
        """Validate totals data."""
        errors = []
        
        # Check required columns
        required_columns = ['GAME_ID', 'TEAM_ID', 'SEASON_YEAR', 'GAME_DATE']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return errors
        
        # Check for duplicate primary keys
        duplicates = df.duplicated(subset=['GAME_ID', 'TEAM_ID'])
        if duplicates.any():
            errors.append(f"Found {duplicates.sum()} duplicate GAME_ID/TEAM_ID combinations")
        
        return errors
    
    def _validate_shooting_stats(self, df: pd.DataFrame) -> List[str]:
        """Validate shooting statistics consistency."""
        errors = []
        
        # Field goals validation
        if all(col in df.columns for col in ['fieldGoalsMade', 'fieldGoalsAttempted']):
            invalid_fg = df['fieldGoalsMade'] > df['fieldGoalsAttempted']
            if invalid_fg.any():
                errors.append(f"Found {invalid_fg.sum()} rows where FGM > FGA")
        
        # Three-pointers validation
        if all(col in df.columns for col in ['threePointersMade', 'threePointersAttempted', 'fieldGoalsMade']):
            invalid_3p = df['threePointersMade'] > df['fieldGoalsMade']
            if invalid_3p.any():
                errors.append(f"Found {invalid_3p.sum()} rows where 3PM > FGM")
        
        return errors
    
    def _validate_rebounds(self, df: pd.DataFrame) -> List[str]:
        """Validate rebounds consistency."""
        errors = []
        
        if all(col in df.columns for col in ['reboundsOffensive', 'reboundsDefensive', 'reboundsTotal']):
            calculated_total = df['reboundsOffensive'] + df['reboundsDefensive']
            mismatch = df['reboundsTotal'] != calculated_total
            if mismatch.any():
                errors.append(f"Found {mismatch.sum()} rows where total rebounds != offensive + defensive")
        
        return errors
    
    def read_multiple_files(self, 
                           file_paths: List[Union[str, Path]], 
                           combine: bool = True) -> Union[CSVReadResult, List[CSVReadResult]]:
        """
        Read multiple CSV files.
        
        Args:
            file_paths: List of CSV file paths
            combine: Whether to combine all data into a single DataFrame
            
        Returns:
            Single CSVReadResult if combine=True, list of results otherwise
        """
        results = []
        
        for file_path in file_paths:
            result = self.read_csv_file(file_path)
            results.append(result)
            
            if not result.success:
                logger.error(f"Failed to read {file_path}: {result.errors}")
        
        if not combine:
            return results
        
        # Combine successful results
        successful_results = [r for r in results if r.success and r.data is not None]
        
        if not successful_results:
            return CSVReadResult(
                success=False,
                row_count=0,
                errors=["No files were successfully read"]
            )
        
        try:
            combined_df = pd.concat([r.data for r in successful_results], ignore_index=True)
            combined_errors = []
            for r in results:
                combined_errors.extend(r.errors)
            
            return CSVReadResult(
                success=True,
                row_count=len(combined_df),
                data=combined_df,
                errors=combined_errors
            )
            
        except Exception as e:
            return CSVReadResult(
                success=False,
                row_count=0,
                errors=[f"Failed to combine DataFrames: {str(e)}"]
            )


def create_csv_reader(chunk_size: int = 1000, 
                     validate_data: bool = True, 
                     strict_mode: bool = False) -> NBACSVReader:
    """
    Create a configured NBA CSV reader.
    
    Args:
        chunk_size: Chunk size for reading large files
        validate_data: Whether to validate data
        strict_mode: Whether to fail on validation errors
        
    Returns:
        Configured NBACSVReader instance
    """
    return NBACSVReader(
        chunk_size=chunk_size,
        validate_data=validate_data,
        strict_mode=strict_mode
    )
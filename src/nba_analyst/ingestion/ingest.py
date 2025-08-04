"""
Data ingestion pipeline for NBA Analyst Agent.

This module provides the main data ingestion pipeline that reads CSV files,
validates the data, and loads it into the database.
"""

import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from .csv_reader import NBACSVReader, CSVReadResult, create_csv_reader
from .validators import NBADataValidator, ValidationResult, create_validator
from ..database.connection import DatabaseConnection, get_database_connection
from ..database.models import PlayerBoxScore, TeamGameTotal, Base
from ..config.settings import load_settings

logger = logging.getLogger(__name__)


@dataclass
class IngestionStats:
    """Statistics from data ingestion operation."""
    
    total_rows_read: int = 0
    rows_validated: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    validation_errors: int = 0
    validation_warnings: int = 0
    ingestion_errors: int = 0
    processing_time_seconds: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate successful ingestion rate."""
        if self.total_rows_read == 0:
            return 0.0
        return (self.rows_inserted + self.rows_updated) / self.total_rows_read
    
    @property
    def validation_error_rate(self) -> float:
        """Calculate validation error rate."""
        if self.rows_validated == 0:
            return 0.0
        return self.validation_errors / self.rows_validated


@dataclass
class IngestionResult:
    """Result of data ingestion operation."""
    
    success: bool
    stats: IngestionStats
    errors: List[str]
    file_path: Optional[Path] = None
    table_name: Optional[str] = None
    
    def __str__(self) -> str:
        return (
            f"IngestionResult(success={self.success}, "
            f"inserted={self.stats.rows_inserted}, "
            f"errors={len(self.errors)})"
        )


class NBADataIngestion:
    """Main data ingestion pipeline for NBA data."""
    
    def __init__(self,
                 db_connection: Optional[DatabaseConnection] = None,
                 csv_reader: Optional[NBACSVReader] = None,
                 validator: Optional[NBADataValidator] = None,
                 batch_size: int = 1000,
                 validate_data: bool = True,
                 upsert_mode: bool = True):
        """
        Initialize NBA data ingestion pipeline.
        
        Args:
            db_connection: Database connection instance
            csv_reader: CSV reader instance
            validator: Data validator instance
            batch_size: Number of rows to process in each batch
            validate_data: Whether to validate data before insertion
            upsert_mode: Whether to use upsert (insert or update) vs insert only
        """
        self.batch_size = batch_size
        self.validate_data = validate_data
        self.upsert_mode = upsert_mode
        
        # Initialize components
        self.db_connection = db_connection or get_database_connection()
        self.csv_reader = csv_reader or create_csv_reader(
            chunk_size=batch_size,
            validate_data=validate_data,
            strict_mode=False
        )
        self.validator = validator or create_validator(
            strict_mode=False,
            max_errors=100
        )
        
        # Model mappings
        self.model_mappings = {
            'box_scores': {
                'model': PlayerBoxScore,
                'table_name': 'players_raw',
                'primary_keys': ['game_id', 'person_id']
            },
            'totals': {
                'model': TeamGameTotal,
                'table_name': 'teams_raw',
                'primary_keys': ['game_id', 'team_id']
            }
        }
    
    def ingest_csv_file(self, 
                       file_path: Union[str, Path],
                       data_type: Optional[str] = None,
                       max_rows: Optional[int] = None) -> IngestionResult:
        """
        Ingest a single CSV file into the database.
        
        Args:
            file_path: Path to CSV file
            data_type: Type of data ('box_scores' or 'totals'), auto-detected if None
            max_rows: Maximum number of rows to process (for testing)
            
        Returns:
            IngestionResult with statistics and status
        """
        file_path = Path(file_path)
        start_time = datetime.now()
        
        logger.info(f"Starting ingestion of {file_path}")
        
        stats = IngestionStats()
        errors = []
        
        try:
            # Step 1: Read CSV file
            logger.info("Reading CSV file...")
            csv_result = self.csv_reader.read_csv_file(
                file_path, 
                file_type=data_type,
                max_rows=max_rows
            )
            
            if not csv_result.success or csv_result.data is None:
                errors.extend(csv_result.errors)
                return IngestionResult(
                    success=False,
                    stats=stats,
                    errors=errors,
                    file_path=file_path
                )
            
            stats.total_rows_read = csv_result.row_count
            detected_type = data_type or self.csv_reader.detect_file_type(file_path)
            
            logger.info(f"Read {stats.total_rows_read} rows of {detected_type} data")
            
            # Step 2: Validate data
            if self.validate_data:
                logger.info("Validating data...")
                validation_result = self.validator.validate_dataframe(
                    csv_result.data, 
                    detected_type
                )
                
                stats.rows_validated = validation_result.total_rows
                stats.validation_errors = validation_result.error_count
                stats.validation_warnings = validation_result.warning_count
                
                logger.info(f"Validation: {stats.validation_errors} errors, {stats.validation_warnings} warnings")
                
                # Log validation issues
                for error in validation_result.errors[:10]:  # Show first 10 errors
                    logger.warning(f"Validation error: {error}")
                
                # Stop if critical validation errors
                if stats.validation_errors > 0 and stats.validation_errors > stats.total_rows_read * 0.1:
                    errors.append(f"Too many validation errors: {stats.validation_errors}")
                    return IngestionResult(
                        success=False,
                        stats=stats,
                        errors=errors,
                        file_path=file_path
                    )
            
            # Step 3: Insert data into database
            logger.info("Inserting data into database...")
            insert_result = self._insert_dataframe(csv_result.data, detected_type)
            
            stats.rows_inserted = insert_result.get('inserted', 0)
            stats.rows_updated = insert_result.get('updated', 0)
            stats.rows_skipped = insert_result.get('skipped', 0)
            stats.ingestion_errors = len(insert_result.get('errors', []))
            
            errors.extend(insert_result.get('errors', []))
            
            # Step 4: Calculate final statistics
            end_time = datetime.now()
            stats.processing_time_seconds = (end_time - start_time).total_seconds()
            
            success = (stats.rows_inserted + stats.rows_updated) > 0 and stats.ingestion_errors == 0
            
            logger.info(f"Ingestion completed: {stats.rows_inserted} inserted, {stats.rows_updated} updated")
            
            return IngestionResult(
                success=success,
                stats=stats,
                errors=errors,
                file_path=file_path,
                table_name=self.model_mappings.get(detected_type, {}).get('table_name')
            )
            
        except Exception as e:
            logger.error(f"Ingestion failed for {file_path}: {e}")
            errors.append(f"Ingestion error: {str(e)}")
            
            end_time = datetime.now()
            stats.processing_time_seconds = (end_time - start_time).total_seconds()
            
            return IngestionResult(
                success=False,
                stats=stats,
                errors=errors,
                file_path=file_path
            )
    
    def _insert_dataframe(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Insert DataFrame into appropriate database table.
        
        Args:
            df: DataFrame to insert
            data_type: Type of data ('box_scores' or 'totals')
            
        Returns:
            Dictionary with insertion statistics
        """
        if data_type not in self.model_mappings:
            return {'errors': [f"Unknown data type: {data_type}"]}
        
        mapping = self.model_mappings[data_type]
        model_class = mapping['model']
        table_name = mapping['table_name']
        
        logger.info(f"Inserting {len(df)} rows into {table_name}")
        
        inserted = 0
        updated = 0
        skipped = 0
        errors = []
        
        try:
            with self.db_connection.get_session() as session:
                # Process in batches
                for start_idx in range(0, len(df), self.batch_size):
                    end_idx = min(start_idx + self.batch_size, len(df))
                    batch_df = df.iloc[start_idx:end_idx]
                    
                    logger.debug(f"Processing batch {start_idx}-{end_idx}")
                    
                    batch_result = self._insert_batch(
                        session, 
                        batch_df, 
                        model_class,
                        data_type
                    )
                    
                    inserted += batch_result.get('inserted', 0)
                    updated += batch_result.get('updated', 0)
                    skipped += batch_result.get('skipped', 0)
                    errors.extend(batch_result.get('errors', []))
                    
                    # Stop if too many errors
                    if len(errors) > 100:
                        logger.warning("Too many insertion errors, stopping")
                        break
                
                # Commit transaction
                session.commit()
                logger.info(f"Batch insertion completed: {inserted} inserted, {updated} updated")
                
        except SQLAlchemyError as e:
            logger.error(f"Database error during insertion: {e}")
            errors.append(f"Database error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during insertion: {e}")
            errors.append(f"Insertion error: {str(e)}")
        
        return {
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped,
            'errors': errors
        }
    
    def _insert_batch(self, 
                     session, 
                     batch_df: pd.DataFrame, 
                     model_class,
                     data_type: str) -> Dict[str, Any]:
        """Insert a batch of records."""
        inserted = 0
        updated = 0
        skipped = 0
        errors = []
        
        try:
            # Convert DataFrame to model instances
            records = []
            
            for _, row in batch_df.iterrows():
                try:
                    record_data = self._row_to_model_data(row, data_type)
                    if record_data:
                        records.append(record_data)
                    else:
                        skipped += 1
                except Exception as e:
                    logger.warning(f"Failed to convert row to model: {e}")
                    errors.append(f"Row conversion error: {str(e)}")
                    skipped += 1
            
            if not records:
                return {'inserted': 0, 'updated': 0, 'skipped': skipped, 'errors': errors}
            
            # Check if we're using PostgreSQL for upsert operations
            engine_dialect = session.bind.dialect.name
            
            if self.upsert_mode and engine_dialect == 'postgresql':
                # Use PostgreSQL UPSERT (ON CONFLICT DO UPDATE)
                mapping = self.model_mappings[data_type]
                stmt = insert(model_class.__table__).values(records)
                
                # Create update dict for conflict resolution
                update_dict = {
                    col.name: stmt.excluded[col.name] 
                    for col in model_class.__table__.columns 
                    if col.name not in mapping['primary_keys']
                }
                
                if update_dict:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=mapping['primary_keys'],
                        set_=update_dict
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=mapping['primary_keys']
                    )
                
                result = session.execute(stmt)
                inserted = result.rowcount
                
            else:
                # Simple bulk insert (works with SQLite and other DBs)
                session.bulk_insert_mappings(model_class, records)
                inserted = len(records)
            
        except SQLAlchemyError as e:
            logger.error(f"Batch insertion failed: {e}")
            errors.append(f"Batch error: {str(e)}")
        
        return {
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped,
            'errors': errors
        }
    
    def _row_to_model_data(self, row: pd.Series, data_type: str) -> Optional[Dict[str, Any]]:
        """Convert DataFrame row to model data dictionary."""
        try:
            if data_type == 'box_scores':
                return self._box_score_row_to_dict(row)
            elif data_type == 'totals':
                return self._totals_row_to_dict(row)
            else:
                logger.warning(f"Unknown data type: {data_type}")
                return None
        except Exception as e:
            logger.warning(f"Failed to convert row: {e}")
            return None
    
    def _box_score_row_to_dict(self, row: pd.Series) -> Dict[str, Any]:
        """Convert box score row to PlayerBoxScore model data."""
        # Handle missing values
        def safe_int(value, default=0):
            if pd.isna(value):
                return default
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        
        def safe_float(value, default=0.0):
            if pd.isna(value):
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        def safe_str(value, default=""):
            if pd.isna(value):
                return default
            return str(value)
        
        def safe_date(value):
            if pd.isna(value):
                return None
            if isinstance(value, date):
                return value
            try:
                return pd.to_datetime(value).date()
            except:
                return None
        
        return {
            'game_id': safe_int(row.get('gameId')),
            'person_id': safe_int(row.get('personId')),
            'season_year': safe_str(row.get('season_year')),
            'game_date': safe_date(row.get('game_date')),
            'matchup': safe_str(row.get('matchup', '')),  # Default to empty string if not present
            'team_id': safe_int(row.get('teamId')),
            'team_city': safe_str(row.get('teamCity')),
            'team_name': safe_str(row.get('teamName')),
            'team_tricode': safe_str(row.get('teamTricode')),
            'team_slug': safe_str(row.get('teamSlug')),
            'person_name': safe_str(row.get('personName')),
            'position': safe_str(row.get('position')),
            'comment': safe_str(row.get('comment')),
            'jersey_num': safe_str(row.get('jerseyNum')),
            'minutes': safe_str(row.get('minutes')),
            'field_goals_made': safe_int(row.get('fieldGoalsMade')),
            'field_goals_attempted': safe_int(row.get('fieldGoalsAttempted')),
            'field_goals_percentage': safe_float(row.get('fieldGoalsPercentage')),
            'three_pointers_made': safe_int(row.get('threePointersMade')),
            'three_pointers_attempted': safe_int(row.get('threePointersAttempted')),
            'three_pointers_percentage': safe_float(row.get('threePointersPercentage')),
            'free_throws_made': safe_int(row.get('freeThrowsMade')),
            'free_throws_attempted': safe_int(row.get('freeThrowsAttempted')),
            'free_throws_percentage': safe_float(row.get('freeThrowsPercentage')),
            'rebounds_offensive': safe_int(row.get('reboundsOffensive')),
            'rebounds_defensive': safe_int(row.get('reboundsDefensive')),
            'rebounds_total': safe_int(row.get('reboundsTotal')),
            'assists': safe_int(row.get('assists')),
            'steals': safe_int(row.get('steals')),
            'blocks': safe_int(row.get('blocks')),
            'turnovers': safe_int(row.get('turnovers')),
            'fouls_personal': safe_int(row.get('foulsPersonal')),
            'points': safe_int(row.get('points')),
            'plus_minus_points': safe_int(row.get('plusMinusPoints')),
        }
    
    def _totals_row_to_dict(self, row: pd.Series) -> Dict[str, Any]:
        """Convert totals row to TeamGameTotal model data."""
        # Implementation for totals data conversion
        # This would be similar to box_score_row_to_dict but for team data
        pass
    
    def get_ingestion_summary(self, results: List[IngestionResult]) -> Dict[str, Any]:
        """Generate summary statistics from multiple ingestion results."""
        if not results:
            return {'message': 'No ingestion results to summarize'}
        
        total_files = len(results)
        successful_files = sum(1 for r in results if r.success)
        total_rows_read = sum(r.stats.total_rows_read for r in results)
        total_rows_inserted = sum(r.stats.rows_inserted for r in results)
        total_errors = sum(len(r.errors) for r in results)
        total_processing_time = sum(r.stats.processing_time_seconds for r in results)
        
        return {
            'files_processed': total_files,
            'files_successful': successful_files,
            'success_rate': successful_files / total_files if total_files > 0 else 0,
            'total_rows_read': total_rows_read,
            'total_rows_inserted': total_rows_inserted,
            'ingestion_success_rate': total_rows_inserted / total_rows_read if total_rows_read > 0 else 0,
            'total_errors': total_errors,
            'total_processing_time_seconds': total_processing_time,
            'average_processing_time_per_file': total_processing_time / total_files if total_files > 0 else 0
        }


def create_ingestion_pipeline(batch_size: int = 1000,
                            validate_data: bool = True,
                            upsert_mode: bool = True) -> NBADataIngestion:
    """
    Create a configured NBA data ingestion pipeline.
    
    Args:
        batch_size: Batch size for processing
        validate_data: Whether to validate data
        upsert_mode: Whether to use upsert mode
        
    Returns:
        Configured NBADataIngestion instance
    """
    return NBADataIngestion(
        batch_size=batch_size,
        validate_data=validate_data,
        upsert_mode=upsert_mode
    )
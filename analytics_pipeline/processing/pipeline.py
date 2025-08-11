"""
Complete Data Processing Pipeline for NBA Analytics

This module orchestrates the complete data processing workflow from
raw CSV ingestion through advanced analytics-ready data transformation.
"""

from typing import List, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import logging

from ..ingestion.ingest import create_ingestion_pipeline, IngestionResult
from ..analytics.processor import create_advanced_metrics_processor, ProcessingResult
from ..database.connection import DatabaseConnection


@dataclass
class PipelineResult:
    """Result of complete pipeline processing."""
    
    success: bool
    ingestion_results: List[IngestionResult]
    processing_results: List[ProcessingResult] 
    total_files_processed: int
    total_records_ingested: int
    total_records_processed: int
    errors: List[str]
    
    @property
    def overall_success_rate(self) -> float:
        """Calculate overall pipeline success rate."""
        if self.total_files_processed == 0:
            return 0.0
        
        successful_files = sum(1 for r in self.ingestion_results if r.success)
        return successful_files / self.total_files_processed


class DataProcessingPipeline:
    """
    Complete data processing pipeline for NBA analytics.
    
    Orchestrates the full workflow:
    1. Raw CSV ingestion into database
    2. Data transformation and validation  
    3. Advanced metrics calculation
    4. AI-ready data preparation
    """
    
    def __init__(self, db_connection: DatabaseConnection, batch_size: int = 1000):
        """
        Initialize the data processing pipeline.
        
        Args:
            db_connection: Database connection for all operations
            batch_size: Batch size for processing operations
        """
        self.db_connection = db_connection
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
        # Initialize sub-pipelines
        self.ingestion_pipeline = create_ingestion_pipeline(
            batch_size=batch_size,
            validate_data=True,
            upsert_mode=True
        )
        self.ingestion_pipeline.db_connection = db_connection
        
        self.metrics_processor = create_advanced_metrics_processor(db_connection)
    
    def process_nba_dataset(self, data_directory: Path) -> PipelineResult:
        """
        Process complete NBA dataset directory.
        
        Args:
            data_directory: Path to directory containing NBA CSV files
            
        Returns:
            PipelineResult with complete processing statistics
        """
        self.logger.info(f"Starting complete NBA dataset processing from {data_directory}")
        
        ingestion_results = []
        processing_results = []
        errors = []
        
        try:
            # Step 1: Discover and categorize CSV files
            csv_files = self._discover_csv_files(data_directory)
            self.logger.info(f"Discovered {len(csv_files)} CSV files for processing")
            
            # Step 2: Ingest all CSV files
            self.logger.info("Phase 1: Raw data ingestion")
            for file_path, data_type in csv_files:
                self.logger.info(f"Ingesting {file_path.name} as {data_type}")
                
                result = self.ingestion_pipeline.ingest_csv_file(
                    file_path=file_path,
                    data_type=data_type
                )
                ingestion_results.append(result)
                
                if not result.success:
                    errors.extend(result.errors)
                    self.logger.error(f"Failed to ingest {file_path.name}: {result.errors}")
            
            # Step 3: Process ingested data into advanced metrics
            self.logger.info("Phase 2: Advanced metrics processing")
            
            # Get unique seasons from ingested data
            seasons = self._get_ingested_seasons()
            self.logger.info(f"Processing {len(seasons)} seasons: {seasons}")
            
            for season in seasons:
                self.logger.info(f"Processing advanced metrics for season {season}")
                
                result = self.metrics_processor.process_season_data(
                    season_year=season,
                    batch_size=self.batch_size
                )
                processing_results.append(result)
                
                if not result.success:
                    errors.extend(result.errors)
                    self.logger.error(f"Failed to process season {season}: {result.errors}")
            
            # Calculate summary statistics
            total_files = len(csv_files)
            total_ingested = sum(r.stats.rows_inserted for r in ingestion_results if r.success)
            total_processed = sum(r.processed_count for r in processing_results if r.success)
            
            overall_success = len(errors) == 0
            
            result = PipelineResult(
                success=overall_success,
                ingestion_results=ingestion_results,
                processing_results=processing_results,
                total_files_processed=total_files,
                total_records_ingested=total_ingested,
                total_records_processed=total_processed,
                errors=errors
            )
            
            self.logger.info(f"Pipeline complete: {total_ingested} records ingested, "
                           f"{total_processed} records processed with advanced metrics")
            
            return result
            
        except Exception as e:
            error_msg = f"Pipeline failed with unexpected error: {str(e)}"
            self.logger.error(error_msg)
            errors.append(error_msg)
            
            return PipelineResult(
                success=False,
                ingestion_results=ingestion_results,
                processing_results=processing_results,
                total_files_processed=len(csv_files) if 'csv_files' in locals() else 0,
                total_records_ingested=0,
                total_records_processed=0,
                errors=errors
            )
    
    def _discover_csv_files(self, data_directory: Path) -> List[tuple[Path, str]]:
        """
        Discover and categorize CSV files in the data directory.
        
        Args:
            data_directory: Directory to scan for CSV files
            
        Returns:
            List of (file_path, data_type) tuples
        """
        csv_files = []
        
        # Map file patterns to data types
        file_patterns = {
            'box_scores': ['box_scores', 'boxscores'],
            'totals': ['totals']
        }
        
        for csv_file in data_directory.glob("*.csv"):
            file_name = csv_file.name.lower()
            
            # Determine data type from filename
            data_type = None
            for dtype, patterns in file_patterns.items():
                if any(pattern in file_name for pattern in patterns):
                    data_type = dtype
                    break
            
            if data_type:
                csv_files.append((csv_file, data_type))
                self.logger.debug(f"Categorized {csv_file.name} as {data_type}")
            else:
                self.logger.warning(f"Could not categorize CSV file: {csv_file.name}")
        
        # Sort by filename to ensure consistent processing order
        csv_files.sort(key=lambda x: x[0].name)
        
        return csv_files
    
    def _get_ingested_seasons(self) -> List[str]:
        """
        Get list of unique seasons from ingested data.
        
        Returns:
            List of season years (e.g., ['2023-24', '2022-23'])
        """
        from ..database.models import PlayerBoxScore
        
        with self.db_connection.get_session() as session:
            seasons = session.query(PlayerBoxScore.season_year).distinct().all()
            return sorted([season[0] for season in seasons], reverse=True)
    
    def get_pipeline_summary(self, result: PipelineResult) -> Dict[str, Any]:
        """
        Generate detailed pipeline summary statistics.
        
        Args:
            result: PipelineResult from pipeline execution
            
        Returns:
            Dictionary with comprehensive summary statistics
        """
        # Ingestion summary
        ingestion_summary = self.ingestion_pipeline.get_ingestion_summary(result.ingestion_results)
        
        # Processing summary  
        processing_summary = self.metrics_processor.get_processing_summary(result.processing_results)
        
        return {
            'pipeline_success': result.success,
            'overall_success_rate': result.overall_success_rate,
            'total_files_processed': result.total_files_processed,
            'total_records_ingested': result.total_records_ingested,
            'total_records_processed': result.total_records_processed,
            'ingestion_summary': ingestion_summary,
            'processing_summary': processing_summary,
            'error_count': len(result.errors),
            'errors': result.errors[:10]  # Limit error display
        }


def create_processing_pipeline(
    db_connection: DatabaseConnection,
    batch_size: int = 1000
) -> DataProcessingPipeline:
    """
    Factory function to create a configured data processing pipeline.
    
    Args:
        db_connection: Database connection
        batch_size: Batch size for processing operations
        
    Returns:
        Configured DataProcessingPipeline instance
    """
    return DataProcessingPipeline(
        db_connection=db_connection,
        batch_size=batch_size
    )
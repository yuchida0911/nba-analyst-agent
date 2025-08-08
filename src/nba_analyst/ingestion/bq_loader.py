import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

DEFAULT_PROJECT_ID = "yuchida-dev"

logger = logging.getLogger(__name__)

class NBABigQueryLoader:
    def __init__(self, project_id: str = DEFAULT_PROJECT_ID):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = "nba_analytics"
        
        logger.info(f"Initialized NBABigQueryLoader for project: {project_id}, dataset: {self.dataset_id}")
        
        # Verify dataset exists or create it
        try:
            dataset_ref = self.client.get_dataset(self.dataset_id)
            logger.info(f"Connected to existing dataset: {dataset_ref.dataset_id}")
        except NotFound:
            logger.warning(f"Dataset {self.dataset_id} not found - will be created automatically")
            self._create_dataset()
        except Exception as e:
            logger.error(f"Error checking dataset {self.dataset_id}: {str(e)}")
            raise

    def _create_dataset(self) -> bool:
        """
        Create the BigQuery dataset if it doesn't exist.
        
        Returns:
            bool: True if dataset was created or already exists, False on failure
        """
        start_time = datetime.now()
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        logger.info(f"🏗️  Creating BigQuery dataset: {dataset_id}")
        
        try:
            # Configure dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"  # Set to US region for better performance
            dataset.description = "NBA analytics data warehouse for player and team statistics"
            
            # Set default table expiration to 1 year
            dataset.default_table_expiration_ms = 31536000000  # 365 days in milliseconds
            
            logger.debug(f"Dataset configuration: location=US, description set, table_expiration=365 days")
            
            # Create the dataset
            created_dataset = self.client.create_dataset(dataset, exists_ok=True)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Successfully created dataset {dataset_id} in {duration:.2f}s")
            logger.info(f"Dataset details - Location: {created_dataset.location}, "
                       f"Created: {created_dataset.created}")
            
            return True
            
        except Conflict:
            # Dataset already exists
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Dataset {dataset_id} already exists (resolved in {duration:.2f}s)")
            return True
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ Failed to create dataset {dataset_id} after {duration:.2f}s: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            return False

    def create_dataset(self) -> bool:
        """
        Public method to create the BigQuery dataset.
        
        Returns:
            bool: True if dataset was created or already exists, False on failure
        """
        return self._create_dataset()

    def create_players_raw_table(self) -> bool:
        """
        Create the players_raw table in BigQuery with comprehensive logging.
        
        Returns:
            bool: True if table was created or already exists, False on failure
        """
        start_time = datetime.now()
        table_id = f"{self.project_id}.{self.dataset_id}.players_raw"
        
        logger.info(f"Starting table creation process for {table_id}")
        
        # Ensure dataset exists first
        try:
            self.client.get_dataset(self.dataset_id)
        except NotFound:
            logger.info(f"Dataset {self.dataset_id} not found, creating it first...")
            if not self._create_dataset():
                logger.error(f"Failed to create required dataset {self.dataset_id}")
                return False
        
        try:
            # Check if table already exists
            try:
                existing_table = self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists. Created: {existing_table.created}, "
                           f"Rows: {existing_table.num_rows}, Size: {existing_table.num_bytes} bytes")
                return True
            except NotFound:
                logger.info(f"Table {table_id} not found, proceeding with creation")
            
            # Define schema with ALL 34 columns to match CSV structure
            schema = [
                bigquery.SchemaField("season_year", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("game_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("gameId", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("matchup", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("teamId", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("teamCity", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("teamName", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("teamTricode", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("teamSlug", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("personId", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("personName", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("comment", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("jerseyNum", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("minutes", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("fieldGoalsMade", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("fieldGoalsAttempted", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("fieldGoalsPercentage", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("threePointersMade", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("threePointersAttempted", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("threePointersPercentage", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("freeThrowsMade", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("freeThrowsAttempted", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("freeThrowsPercentage", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("reboundsOffensive", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("reboundsDefensive", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("reboundsTotal", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("assists", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("steals", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("blocks", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("turnovers", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("foulsPersonal", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("points", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("plusMinusPoints", "INT64", mode="NULLABLE"),
            ]
            
            logger.debug(f"Defined schema with {len(schema)} fields: "
                        f"{[f.name for f in schema]}")

            # Create table configuration
            table = bigquery.Table(table_id, schema=schema)

            # Configure time partitioning
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="game_date",
            )
            logger.debug(f"Configured time partitioning by game_date (DAY)")

            # Configure clustering
            table.clustering_fields = ["personId", "season_year"]
            logger.debug(f"Configured clustering by: {table.clustering_fields}")
            
            # Set expiration policies
            table.default_partition_expiration = 31536000  # 365 days
            table.default_table_expiration = 31536000      # 365 days
            logger.debug(f"Set expiration policies: 365 days for partitions and table")

            # Set table description
            table.description = "Raw player box score data imported from CSV files"

            # Create the table
            logger.info(f"Creating table {table_id} with partitioning and clustering...")
            created_table = self.client.create_table(table, exists_ok=True)
            
            # Log success
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Successfully created table {table_id} in {duration:.2f}s")
            logger.info(f"Table details - Location: {created_table.location}, "
                       f"Schema fields: {len(created_table.schema)}")
            
            return True
            
        except Conflict as e:
            # Table already exists with different configuration
            logger.warning(f"Table {table_id} exists with conflicting configuration: {str(e)}")
            return True
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ Failed to create table {table_id} after {duration:.2f}s: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            return False

    def load_csv_files(self, csv_patterns: List[str]) -> Dict[str, Any]:
        """
        Load CSV files into BigQuery with comprehensive logging and error handling.
        
        Args:
            csv_patterns: List of CSV file patterns to load from GCS
            
        Returns:
            Dict containing load statistics and results
        """
        overall_start_time = datetime.now()
        table_id = f"{self.dataset_id}.players_raw"
        
        logger.info(f"Starting CSV load operation for {len(csv_patterns)} file(s) into {table_id}")
        logger.debug(f"File patterns: {csv_patterns}")
        
        results = {
            "total_files": len(csv_patterns),
            "successful_loads": 0,
            "failed_loads": 0,
            "total_rows_loaded": 0,
            "total_bytes_processed": 0,
            "job_details": [],
            "errors": []
        }
        
        for i, csv_pattern in enumerate(csv_patterns, 1):
            file_start_time = datetime.now()
            uri = f"gs://nba-analytics-csv-staging/{csv_pattern}"
            
            logger.info(f"📁 Processing file {i}/{len(csv_patterns)}: {csv_pattern}")
            logger.debug(f"Source URI: {uri}")
            
            try:
                # Configure load job
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    autodetect=False,  # Use our defined schema
                    allow_quoted_newlines=True,
                    allow_jagged_rows=False,
                    max_bad_records=1000,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                )
                
                logger.debug(f"Job config: skip_rows=1, max_bad_records=1000, "
                           f"disposition=APPEND, autodetect=False")
                
                # Submit load job
                logger.info(f"🚀 Submitting load job for {csv_pattern}...")
                load_job = self.client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
                
                logger.info(f"Job submitted with ID: {load_job.job_id}")
                logger.debug(f"Job location: {load_job.location}")
                
                # Wait for job completion with progress logging
                logger.info(f"⏳ Waiting for job {load_job.job_id} to complete...")
                
                try:
                    result = load_job.result(timeout=300)  # 5 minute timeout
                    
                    # Calculate metrics
                    file_duration = (datetime.now() - file_start_time).total_seconds()
                    rows_loaded = getattr(load_job, 'output_rows', 0) or 0
                    
                    # Handle different attribute names across BigQuery Python client versions
                    bytes_processed = 0
                    for attr in ['total_bytes_processed', 'input_file_bytes', 'input_files']:
                        if hasattr(load_job, attr):
                            val = getattr(load_job, attr)
                            if isinstance(val, (int, float)) and val > 0:
                                bytes_processed = val
                                break
                    
                    # Log success metrics
                    logger.info(f"✅ Successfully loaded {csv_pattern}")
                    logger.info(f"📊 Load metrics: {rows_loaded:,} rows, "
                              f"{bytes_processed:,} bytes, {file_duration:.2f}s")
                    
                    if bytes_processed > 0:
                        throughput_mb_per_sec = (bytes_processed / (1024 * 1024)) / file_duration
                        logger.debug(f"Throughput: {throughput_mb_per_sec:.2f} MB/s")
                    
                    # Update results
                    results["successful_loads"] += 1
                    results["total_rows_loaded"] += rows_loaded
                    results["total_bytes_processed"] += bytes_processed
                    
                    job_detail = {
                        "file": csv_pattern,
                        "job_id": load_job.job_id,
                        "status": "success",
                        "rows_loaded": rows_loaded,
                        "bytes_processed": bytes_processed,
                        "duration_seconds": file_duration,
                        "error": None
                    }
                    results["job_details"].append(job_detail)
                    
                    # Log any warnings from the job
                    if hasattr(load_job, 'errors') and load_job.errors:
                        logger.warning(f"Job completed with {len(load_job.errors)} warning(s):")
                        for error in load_job.errors[:5]:  # Show first 5 warnings
                            error_msg = error.get('message', str(error)) if isinstance(error, dict) else str(error)
                            logger.warning(f"  - {error_msg}")
                    
                    # If we couldn't get bytes_processed, try to get it from job statistics
                    if bytes_processed == 0 and hasattr(load_job, '_properties'):
                        try:
                            stats = load_job._properties.get('statistics', {}).get('load', {})
                            bytes_processed = stats.get('inputFileBytes', 0) or stats.get('inputFiles', 0)
                            if isinstance(bytes_processed, str):
                                bytes_processed = int(bytes_processed)
                        except (AttributeError, ValueError, TypeError):
                            pass
                    
                except Exception as job_error:
                    # Job execution failed
                    file_duration = (datetime.now() - file_start_time).total_seconds()
                    error_msg = f"Job execution failed: {str(job_error)}"
                    
                    logger.error(f"❌ Load job failed for {csv_pattern}: {error_msg}")
                    logger.error(f"Job ID: {load_job.job_id}, Duration: {file_duration:.2f}s")
                    
                    # Log detailed error information if available
                    if hasattr(load_job, 'errors') and load_job.errors:
                        logger.error(f"Job errors ({len(load_job.errors)}):")
                        for error in load_job.errors[:3]:  # Show first 3 errors
                            if isinstance(error, dict):
                                logger.error(f"  - Location: {error.get('location', 'N/A')}")
                                logger.error(f"    Message: {error.get('message', 'N/A')}")
                                logger.error(f"    Reason: {error.get('reason', 'N/A')}")
                            else:
                                logger.error(f"  - Error: {str(error)}")
                    
                    # Check if job actually succeeded despite the exception
                    try:
                        if hasattr(load_job, 'state') and load_job.state == 'DONE':
                            rows_loaded = getattr(load_job, 'output_rows', 0) or 0
                            logger.info(f"✅ Job completed successfully despite exception: {rows_loaded:,} rows loaded")
                            results["successful_loads"] += 1
                            results["total_rows_loaded"] += rows_loaded
                            
                            job_detail = {
                                "file": csv_pattern,
                                "job_id": load_job.job_id,
                                "status": "success_with_warning",
                                "rows_loaded": rows_loaded,
                                "bytes_processed": 0,  # Unknown due to API issues
                                "duration_seconds": file_duration,
                                "error": f"API warning: {str(job_error)}"
                            }
                            results["job_details"].append(job_detail)
                            continue  # Skip adding to failed jobs
                    except Exception:
                        pass  # Proceed with failed job handling
                    
                    results["failed_loads"] += 1
                    results["errors"].append(error_msg)
                    
                    job_detail = {
                        "file": csv_pattern,
                        "job_id": load_job.job_id,
                        "status": "failed",
                        "rows_loaded": 0,
                        "bytes_processed": 0,
                        "duration_seconds": file_duration,
                        "error": error_msg
                    }
                    results["job_details"].append(job_detail)
                    
            except Exception as e:
                # Job submission failed
                file_duration = (datetime.now() - file_start_time).total_seconds()
                error_msg = f"Failed to submit load job: {str(e)}"
                
                logger.error(f"❌ Failed to process {csv_pattern}: {error_msg}")
                logger.error(f"Error type: {type(e).__name__}, Duration: {file_duration:.2f}s")
                
                results["failed_loads"] += 1
                results["errors"].append(error_msg)
                
                job_detail = {
                    "file": csv_pattern,
                    "job_id": None,
                    "status": "submission_failed",
                    "rows_loaded": 0,
                    "bytes_processed": 0,
                    "duration_seconds": file_duration,
                    "error": error_msg
                }
                results["job_details"].append(job_detail)
        
        # Log overall results
        overall_duration = (datetime.now() - overall_start_time).total_seconds()
        success_rate = (results["successful_loads"] / results["total_files"]) * 100
        
        logger.info(f"🏁 Load operation completed in {overall_duration:.2f}s")
        logger.info(f"📈 Overall results: {results['successful_loads']}/{results['total_files']} files "
                   f"({success_rate:.1f}% success rate)")
        logger.info(f"📊 Total data: {results['total_rows_loaded']:,} rows, "
                   f"{results['total_bytes_processed']:,} bytes")
        
        if results["failed_loads"] > 0:
            logger.warning(f"⚠️  {results['failed_loads']} file(s) failed to load")
            for error in results["errors"]:
                logger.warning(f"  - {error}")
        
        if results["total_bytes_processed"] > 0:
            overall_throughput = (results["total_bytes_processed"] / (1024 * 1024)) / overall_duration
            logger.debug(f"Overall throughput: {overall_throughput:.2f} MB/s")
        
        return results
    
    def get_table_info(self, table_name: str = "players_raw") -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a BigQuery table.
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            Dict with table information or None if table doesn't exist
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        try:
            table = self.client.get_table(table_id)
            
            table_info = {
                "table_id": table_id,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "location": table.location,
                "schema_fields": len(table.schema),
                "partitioning": {
                    "type": table.time_partitioning.type_.name if table.time_partitioning and hasattr(table.time_partitioning.type_, 'name') else str(table.time_partitioning.type_) if table.time_partitioning else None,
                    "field": table.time_partitioning.field if table.time_partitioning else None
                },
                "clustering_fields": table.clustering_fields,
                "description": table.description,
                "expires": table.expires.isoformat() if table.expires else None
            }
            
            logger.debug(f"Retrieved table info for {table_id}: {table.num_rows:,} rows, "
                        f"{table.num_bytes:,} bytes")
            
            return table_info
            
        except NotFound:
            logger.warning(f"Table {table_id} not found")
            return None
        except Exception as e:
            logger.error(f"Error getting table info for {table_id}: {str(e)}")
            return None
    
    @staticmethod
    def setup_logging(level: str = "INFO", 
                     format_style: str = "structured") -> None:
        """
        Set up logging configuration for BigQuery operations.
        
        Args:
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_style: Either 'structured' for JSON-like logs or 'simple' for basic format
        """
        log_level = getattr(logging, level.upper(), logging.INFO)
        
        if format_style == "structured":
            formatter = logging.Formatter(
                '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
                '"logger":"%(name)s","message":"%(message)s",'
                '"module":"%(module)s","function":"%(funcName)s","line":%(lineno)d}',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Set Google Cloud library logging to WARNING to reduce noise
        logging.getLogger('google.cloud').setLevel(logging.WARNING)
        logging.getLogger('google.auth').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        logger.info(f"Logging configured: level={level}, format={format_style}")
"""
Processing Workflow Management and Scheduling

This module provides workflow management for NBA data processing,
including scheduling, monitoring, and orchestrating complex processing tasks.
"""

from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging
from pathlib import Path

from .pipeline import DataProcessingPipeline, PipelineResult
from ..database.connection import DatabaseConnection


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowTask:
    """Individual workflow task definition."""
    
    name: str
    description: str
    task_function: Callable
    dependencies: List[str]  # Task names this task depends on
    parameters: Dict[str, Any]
    priority: int = 1  # Higher number = higher priority
    timeout_minutes: int = 60
    retry_count: int = 3


@dataclass
class WorkflowExecution:
    """Workflow execution tracking."""
    
    workflow_id: str
    name: str
    status: WorkflowStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    progress: float = 0.0  # 0.0 to 1.0
    results: Optional[Dict[str, Any]] = None


class ProcessingWorkflow:
    """
    Defines a processing workflow with dependent tasks.
    
    Manages the execution of complex, multi-step data processing
    operations with dependencies, error handling, and monitoring.
    """
    
    def __init__(self, name: str, description: str):
        """
        Initialize a processing workflow.
        
        Args:
            name: Workflow name
            description: Workflow description
        """
        self.name = name
        self.description = description
        self.tasks: Dict[str, WorkflowTask] = {}
        self.logger = logging.getLogger(__name__)
    
    def add_task(self, task: WorkflowTask) -> None:
        """Add a task to the workflow."""
        self.tasks[task.name] = task
        self.logger.debug(f"Added task '{task.name}' to workflow '{self.name}'")
    
    def validate_dependencies(self) -> List[str]:
        """
        Validate that all task dependencies exist.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        for task_name, task in self.tasks.items():
            for dependency in task.dependencies:
                if dependency not in self.tasks:
                    errors.append(f"Task '{task_name}' depends on non-existent task '{dependency}'")
        
        return errors
    
    def get_execution_order(self) -> List[str]:
        """
        Get tasks in dependency-resolved execution order.
        
        Returns:
            List of task names in execution order
        """
        # Topological sort for dependency resolution
        visited = set()
        temp_visited = set()
        order = []
        
        def visit(task_name: str):
            if task_name in temp_visited:
                raise ValueError(f"Circular dependency detected involving task '{task_name}'")
            
            if task_name not in visited:
                temp_visited.add(task_name)
                
                # Visit all dependencies first
                for dependency in self.tasks[task_name].dependencies:
                    visit(dependency)
                
                temp_visited.remove(task_name)
                visited.add(task_name)
                order.append(task_name)
        
        # Start with tasks that have no dependencies
        for task_name in self.tasks:
            if task_name not in visited:
                visit(task_name)
        
        return order


class WorkflowManager:
    """
    Manages execution of processing workflows.
    
    Provides scheduling, monitoring, and execution management
    for complex NBA data processing workflows.
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize workflow manager.
        
        Args:
            db_connection: Database connection for processing operations
        """
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
        self.active_executions: Dict[str, WorkflowExecution] = {}
    
    def execute_workflow(
        self, 
        workflow: ProcessingWorkflow,
        workflow_id: Optional[str] = None
    ) -> WorkflowExecution:
        """
        Execute a processing workflow.
        
        Args:
            workflow: Workflow to execute
            workflow_id: Optional custom workflow ID
            
        Returns:
            WorkflowExecution with execution status and results
        """
        if workflow_id is None:
            workflow_id = f"{workflow.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            name=workflow.name,
            status=WorkflowStatus.PENDING,
            started_at=datetime.now()
        )
        
        self.active_executions[workflow_id] = execution
        self.logger.info(f"Starting workflow execution: {workflow_id}")
        
        try:
            # Validate workflow
            validation_errors = workflow.validate_dependencies()
            if validation_errors:
                raise ValueError(f"Workflow validation failed: {validation_errors}")
            
            # Get execution order
            execution_order = workflow.get_execution_order()
            self.logger.info(f"Workflow execution order: {execution_order}")
            
            execution.status = WorkflowStatus.RUNNING
            task_results = {}
            
            # Execute tasks in dependency order
            for i, task_name in enumerate(execution_order):
                task = workflow.tasks[task_name]
                self.logger.info(f"Executing task: {task_name}")
                
                try:
                    # Execute the task
                    task_result = task.task_function(**task.parameters)
                    task_results[task_name] = task_result
                    
                    # Update progress
                    execution.progress = (i + 1) / len(execution_order)
                    
                    self.logger.info(f"Task '{task_name}' completed successfully")
                    
                except Exception as task_error:
                    error_msg = f"Task '{task_name}' failed: {str(task_error)}"
                    self.logger.error(error_msg)
                    
                    execution.status = WorkflowStatus.FAILED
                    execution.error_message = error_msg
                    execution.completed_at = datetime.now()
                    
                    return execution
            
            # Workflow completed successfully
            execution.status = WorkflowStatus.COMPLETED
            execution.progress = 1.0
            execution.results = task_results
            execution.completed_at = datetime.now()
            
            self.logger.info(f"Workflow '{workflow_id}' completed successfully")
            
        except Exception as e:
            error_msg = f"Workflow execution failed: {str(e)}"
            self.logger.error(error_msg)
            
            execution.status = WorkflowStatus.FAILED
            execution.error_message = error_msg
            execution.completed_at = datetime.now()
        
        return execution
    
    def create_nba_data_processing_workflow(
        self, 
        data_directory: Path,
        batch_size: int = 1000
    ) -> ProcessingWorkflow:
        """
        Create a complete NBA data processing workflow.
        
        Args:
            data_directory: Directory containing NBA CSV files
            batch_size: Batch size for processing operations
            
        Returns:
            Configured ProcessingWorkflow for NBA data processing
        """
        workflow = ProcessingWorkflow(
            name="nba_data_processing",
            description="Complete NBA data processing from CSV ingestion to advanced analytics"
        )
        
        # Task 1: Initialize pipeline
        pipeline_init_task = WorkflowTask(
            name="initialize_pipeline",
            description="Initialize data processing pipeline",
            task_function=self._create_pipeline,
            dependencies=[],
            parameters={'batch_size': batch_size},
            priority=1
        )
        workflow.add_task(pipeline_init_task)
        
        # Task 2: Validate data directory
        validation_task = WorkflowTask(
            name="validate_data_directory",
            description="Validate data directory and discover CSV files",
            task_function=self._validate_data_directory,
            dependencies=[],
            parameters={'data_directory': data_directory},
            priority=1
        )
        workflow.add_task(validation_task)
        
        # Task 3: Process NBA dataset
        processing_task = WorkflowTask(
            name="process_nba_dataset",
            description="Process complete NBA dataset with ingestion and analytics",
            task_function=self._process_nba_dataset,
            dependencies=["initialize_pipeline", "validate_data_directory"],
            parameters={
                'data_directory': data_directory,
                'batch_size': batch_size
            },
            priority=2,
            timeout_minutes=120  # Allow more time for large datasets
        )
        workflow.add_task(processing_task)
        
        # Task 4: Generate summary report
        report_task = WorkflowTask(
            name="generate_summary_report",
            description="Generate processing summary report",
            task_function=self._generate_summary_report,
            dependencies=["process_nba_dataset"],
            parameters={},
            priority=3
        )
        workflow.add_task(report_task)
        
        return workflow
    
    def _create_pipeline(self, batch_size: int) -> DataProcessingPipeline:
        """Create and return data processing pipeline."""
        from .pipeline import create_processing_pipeline
        
        pipeline = create_processing_pipeline(
            db_connection=self.db_connection,
            batch_size=batch_size
        )
        
        self.logger.info(f"Created processing pipeline with batch size: {batch_size}")
        return pipeline
    
    def _validate_data_directory(self, data_directory: Path) -> Dict[str, Any]:
        """Validate data directory and return file information."""
        if not data_directory.exists():
            raise ValueError(f"Data directory does not exist: {data_directory}")
        
        if not data_directory.is_dir():
            raise ValueError(f"Path is not a directory: {data_directory}")
        
        csv_files = list(data_directory.glob("*.csv"))
        
        if not csv_files:
            raise ValueError(f"No CSV files found in directory: {data_directory}")
        
        self.logger.info(f"Found {len(csv_files)} CSV files in {data_directory}")
        
        return {
            'directory': str(data_directory),
            'csv_file_count': len(csv_files),
            'csv_files': [f.name for f in csv_files]
        }
    
    def _process_nba_dataset(
        self, 
        data_directory: Path, 
        batch_size: int
    ) -> PipelineResult:
        """Process the NBA dataset using the pipeline."""
        pipeline = DataProcessingPipeline(
            db_connection=self.db_connection,
            batch_size=batch_size
        )
        
        self.logger.info(f"Starting NBA dataset processing from {data_directory}")
        result = pipeline.process_nba_dataset(data_directory)
        
        self.logger.info(f"Dataset processing complete: "
                        f"{result.total_records_ingested} ingested, "
                        f"{result.total_records_processed} processed")
        
        return result
    
    def _generate_summary_report(self) -> Dict[str, Any]:
        """Generate processing summary report."""
        # This would generate a comprehensive summary report
        # For now, return basic summary information
        
        summary = {
            'report_generated_at': datetime.now().isoformat(),
            'database_tables': ['players_raw', 'teams_raw', 'players_processed'],
            'processing_complete': True
        }
        
        self.logger.info("Generated processing summary report")
        return summary
    
    def get_workflow_status(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """Get current status of a workflow execution."""
        return self.active_executions.get(workflow_id)
    
    def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        execution = self.active_executions.get(workflow_id)
        if execution and execution.status == WorkflowStatus.RUNNING:
            execution.status = WorkflowStatus.CANCELLED
            execution.completed_at = datetime.now()
            self.logger.info(f"Cancelled workflow: {workflow_id}")
            return True
        return False
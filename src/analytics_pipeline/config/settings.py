"""
Settings module for NBA Analyst Agent.

This module handles loading and validation of environment variables
and application configuration.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator


class Settings(BaseModel):
    """Application settings loaded from environment variables."""
    
    # Database Configuration
    db_host: str = Field(default="localhost", description="Database host")
    db_port: int = Field(default=5432, description="Database port")
    db_name: str = Field(default="nba_analyst", description="Database name")
    db_user: str = Field(default="nba_user", description="Database user")
    db_password: str = Field(default="", description="Database password")
    db_schema: str = Field(default="public", description="Database schema")
    
    # Database Connection Pool Settings
    db_pool_size: int = Field(default=5, description="Database connection pool size")
    db_max_overflow: int = Field(default=10, description="Max overflow connections")
    db_pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    
    # Data Processing Configuration
    data_dir: Path = Field(default=Path("./NBA-Data-2010-2024"), description="Data directory path")
    processed_data_dir: Path = Field(default=Path("./data/processed"), description="Processed data directory")
    batch_size: int = Field(default=1000, description="Batch size for data processing")
    max_workers: int = Field(default=4, description="Maximum worker threads")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[Path] = Field(default=Path("./logs/nba_analyst.log"), description="Log file path")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string"
    )
    
    # Development Settings
    debug: bool = Field(default=False, description="Debug mode")
    testing: bool = Field(default=False, description="Testing mode")
    
    # Performance Settings
    query_timeout: int = Field(default=30, description="Query timeout in seconds")
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
    
    # Data Validation Settings
    data_validation_strict: bool = Field(default=True, description="Strict data validation")
    allow_missing_values: bool = Field(default=False, description="Allow missing values")
    max_error_rate: float = Field(default=0.01, description="Maximum error rate")
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()
    
    @field_validator("max_error_rate")
    @classmethod
    def validate_error_rate(cls, v: float) -> float:
        """Validate error rate is between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError("Error rate must be between 0 and 1")
        return v
    
    @field_validator("data_dir", "processed_data_dir")
    @classmethod
    def validate_directories(cls, v: Path) -> Path:
        """Ensure directories exist or can be created."""
        if v.exists() and not v.is_dir():
            raise ValueError(f"Path {v} exists but is not a directory")
        return v
    
    @property
    def database_url(self) -> str:
        """Generate database URL from components."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


def load_settings(env_file: Optional[str] = None) -> Settings:
    """
    Load application settings from environment variables.
    
    Args:
        env_file: Optional path to .env file to load
        
    Returns:
        Settings object with loaded configuration
    """
    if env_file:
        load_dotenv(env_file)
    else:
        # Try to load from common locations
        for env_path in [".env", "../.env", "../../.env"]:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                break
    
    return Settings()
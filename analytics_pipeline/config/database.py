"""
Database configuration module for NBA Analyst Agent.

This module provides database connection configuration and
connection pool management utilities.
"""

from typing import Dict, Any, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, Engine
from sqlalchemy.engine import URL
from sqlalchemy.pool import QueuePool

from .settings import Settings


class DatabaseConfig:
    """Database configuration and connection management."""
    
    def __init__(self, settings: Settings):
        """
        Initialize database configuration.
        
        Args:
            settings: Application settings instance
        """
        self.settings = settings
        self._engine: Optional[Engine] = None
    
    def get_database_url(self, include_password: bool = True) -> str:
        """
        Get database URL for connection.
        
        Args:
            include_password: Whether to include password in URL
            
        Returns:
            Database connection URL
        """
        # Use SQLite for testing or when no host is specified
        if self.settings.testing or not self.settings.db_host:
            return f"sqlite:///{self.settings.db_name}"
            
        if include_password:
            password = quote_plus(self.settings.db_password)
            return (
                f"postgresql://{self.settings.db_user}:{password}@"
                f"{self.settings.db_host}:{self.settings.db_port}/{self.settings.db_name}"
            )
        else:
            return (
                f"postgresql://{self.settings.db_user}@"
                f"{self.settings.db_host}:{self.settings.db_port}/{self.settings.db_name}"
            )
    
    def get_sqlalchemy_url(self) -> URL:
        """
        Get SQLAlchemy URL object for connection.
        
        Returns:
            SQLAlchemy URL object
        """
        # Use SQLite for testing or when no host is specified
        if self.settings.testing or not self.settings.db_host:
            return URL.create(
                drivername="sqlite",
                database=self.settings.db_name,
            )
            
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.settings.db_user,
            password=self.settings.db_password,
            host=self.settings.db_host,
            port=self.settings.db_port,
            database=self.settings.db_name,
        )
    
    def get_engine_kwargs(self) -> Dict[str, Any]:
        """
        Get SQLAlchemy engine configuration.
        
        Returns:
            Dictionary of engine configuration parameters
        """
        # SQLite configuration for testing
        if self.settings.testing or not self.settings.db_host:
            return {
                "echo": self.settings.debug,  # Log SQL queries in debug mode
                "connect_args": {"check_same_thread": False},  # Allow SQLite from multiple threads
            }
            
        # PostgreSQL configuration
        return {
            "poolclass": QueuePool,
            "pool_size": self.settings.db_pool_size,
            "max_overflow": self.settings.db_max_overflow,
            "pool_timeout": self.settings.db_pool_timeout,
            "pool_recycle": 3600,  # Recycle connections after 1 hour
            "pool_pre_ping": True,  # Verify connections before use
            "echo": self.settings.debug,  # Log SQL queries in debug mode
            "connect_args": {
                "options": f"-csearch_path={self.settings.db_schema}",
                "connect_timeout": 10,
            },
        }
    
    def create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine with proper configuration.
        
        Returns:
            Configured SQLAlchemy engine
        """
        if self._engine is None:
            url = self.get_sqlalchemy_url()
            kwargs = self.get_engine_kwargs()
            self._engine = create_engine(url, **kwargs)
        
        return self._engine
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get connection information for logging/debugging.
        
        Returns:
            Dictionary with connection details (no password)
        """
        return {
            "host": self.settings.db_host,
            "port": self.settings.db_port,
            "database": self.settings.db_name,
            "user": self.settings.db_user,
            "schema": self.settings.db_schema,
            "pool_size": self.settings.db_pool_size,
            "max_overflow": self.settings.db_max_overflow,
            "pool_timeout": self.settings.db_pool_timeout,
        }
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self.create_engine()
            with engine.connect() as conn:
                # Use text() for raw SQL to avoid SQLAlchemy 2.0 warnings
                from sqlalchemy import text
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception:
            return False
    
    def close_connections(self) -> None:
        """Close all database connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


def get_database_config(settings: Optional[Settings] = None) -> DatabaseConfig:
    """
    Get database configuration instance.
    
    Args:
        settings: Optional settings instance, will load default if not provided
        
    Returns:
        DatabaseConfig instance
    """
    if settings is None:
        from .settings import settings as default_settings
        settings = default_settings
    
    return DatabaseConfig(settings)
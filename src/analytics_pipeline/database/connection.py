"""
Database connection utilities for NBA Analyst Agent.

This module provides database connection management, session handling,
and connection pool utilities using SQLAlchemy.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional, Any, Dict

from sqlalchemy import create_engine, Engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import Pool
from sqlalchemy.engine import Inspector

from ..config.database import DatabaseConfig
from ..config.settings import Settings, load_settings

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection manager with session handling."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database connection manager.
        
        Args:
            config: Database configuration, will load default if not provided
        """
        if config is None:
            settings = load_settings()
            config = DatabaseConfig(settings)
        
        self.config = config
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine, creating if necessary."""
        if self._engine is None:
            self._engine = self.config.create_engine()
            logger.info(f"Created database engine: {self.config.get_database_url(include_password=False)}")
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory, creating if necessary."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                success = result.scalar() == 1
                if success:
                    logger.info("Database connection test successful")
                else:
                    logger.warning("Database connection test returned unexpected result")
                return success
        except OperationalError as e:
            logger.error(f"Database connection failed (operational): {e}")
            return False
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed (SQLAlchemy): {e}")
            return False
        except Exception as e:
            logger.error(f"Database connection failed (unexpected): {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get detailed connection information.
        
        Returns:
            Dictionary with connection details
        """
        info = self.config.get_connection_info()
        
        # Add engine-specific information if available
        if self._engine is not None:
            pool: Pool = self._engine.pool
            info.update({
                "engine_created": True,
                "pool_size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "invalidated": pool.invalidated(),
            })
        else:
            info["engine_created"] = False
        
        return info
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get database session with automatic cleanup.
        
        Yields:
            SQLAlchemy session
            
        Example:
            with db.get_session() as session:
                result = session.query(Model).all()
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error, rolled back: {e}")
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Get raw database connection with automatic cleanup.
        
        Yields:
            SQLAlchemy connection
            
        Example:
            with db.get_connection() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM players"))
        """
        conn = self.engine.connect()
        try:
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query result
        """
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_scalar(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query and return a single scalar value.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Single scalar result
        """
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result.scalar()
        except SQLAlchemyError as e:
            logger.error(f"Scalar query execution failed: {e}")
            raise
    
    def check_table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            True if table exists, False otherwise
        """
        if schema is None:
            schema = self.config.settings.db_schema
        
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema 
            AND table_name = :table_name
        )
        """
        
        try:
            return self.execute_scalar(query, {"schema": schema, "table_name": table_name})
        except SQLAlchemyError:
            return False
    
    def get_table_row_count(self, table_name: str, schema: Optional[str] = None) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            schema: Optional schema name
            
        Returns:
            Number of rows in the table
        """
        if schema is None:
            schema = self.config.settings.db_schema
        
        # Use quoted identifiers to handle reserved words
        full_table_name = f'"{schema}"."{table_name}"'
        query = f"SELECT COUNT(*) FROM {full_table_name}"
        
        try:
            return self.execute_scalar(query) or 0
        except SQLAlchemyError as e:
            logger.error(f"Failed to get row count for {full_table_name}: {e}")
            return 0
    
    def get_inspector(self) -> Inspector:
        """
        Get SQLAlchemy inspector for database metadata operations.
        
        Returns:
            SQLAlchemy Inspector instance
        """
        return inspect(self.engine)
    
    def close(self) -> None:
        """Close all database connections and clean up resources."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("Database connections closed")


# Global database connection instance
_db_connection: Optional[DatabaseConnection] = None


def get_database_connection(config: Optional[DatabaseConfig] = None) -> DatabaseConnection:
    """
    Get global database connection instance.
    
    Args:
        config: Optional database configuration
        
    Returns:
        DatabaseConnection instance
    """
    global _db_connection
    
    if _db_connection is None:
        _db_connection = DatabaseConnection(config)
    
    return _db_connection


def close_database_connection() -> None:
    """Close the global database connection."""
    global _db_connection
    
    if _db_connection is not None:
        _db_connection.close()
        _db_connection = None
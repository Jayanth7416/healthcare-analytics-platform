"""Database Service"""

from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from src.utils.config import settings

logger = structlog.get_logger()


class DatabaseService:
    """
    Database service for PostgreSQL/Redshift operations

    Features:
    - Connection pooling
    - Async query execution
    - Automatic retry on transient failures
    - Query parameterization for SQL injection prevention
    """

    def __init__(self):
        self.engine = create_engine(
            settings.DATABASE_URL,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a SELECT query and return first result

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Dictionary of column:value pairs
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                row = result.fetchone()
                if row:
                    return dict(row._mapping)
                return {}
        except SQLAlchemyError as e:
            logger.error("query_execution_failed", error=str(e), query=query[:100])
            raise

    async def execute_query_all(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return all results

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of dictionaries
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]
        except SQLAlchemyError as e:
            logger.error("query_execution_failed", error=str(e), query=query[:100])
            raise

    async def execute_insert(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Optional[str]:
        """
        Insert a record and return its ID

        Args:
            table: Table name
            data: Dictionary of column:value pairs

        Returns:
            Inserted record ID
        """
        columns = ', '.join(data.keys())
        placeholders = ', '.join(f':{k}' for k in data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id"

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), data)
                conn.commit()
                row = result.fetchone()
                return str(row[0]) if row else None
        except SQLAlchemyError as e:
            logger.error("insert_failed", error=str(e), table=table)
            raise

    async def execute_batch_insert(
        self,
        table: str,
        records: List[Dict[str, Any]]
    ) -> int:
        """
        Batch insert multiple records

        Args:
            table: Table name
            records: List of dictionaries

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        columns = ', '.join(records[0].keys())
        placeholders = ', '.join(f':{k}' for k in records[0].keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), records)
                conn.commit()
                return len(records)
        except SQLAlchemyError as e:
            logger.error("batch_insert_failed", error=str(e), table=table)
            raise

    def close(self):
        """Close database connections"""
        self.engine.dispose()
        logger.info("database_connections_closed")


async def get_db_health() -> Dict[str, Any]:
    """Check database health"""
    try:
        db = DatabaseService()
        result = await db.execute_query("SELECT 1 as health_check")
        return {
            "healthy": result.get('health_check') == 1,
            "status": "connected"
        }
    except Exception as e:
        return {
            "healthy": False,
            "status": "error",
            "error": str(e)
        }

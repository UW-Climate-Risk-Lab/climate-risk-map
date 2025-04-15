# data/database.py
import logging
import psycopg2 as pg

from psycopg2 import pool, sql
from contextlib import contextmanager
from typing import Tuple, List, Any, Optional

from config.settings import PG_USER, PG_PASSWORD, PG_HOST, PG_PORT

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections using connection pooling"""
    
    _pools = {}  # Dictionary to store connection pools per database
    
    @classmethod
    def get_pool(cls, dbname):
        """Get or create a connection pool for the specified database"""
        if dbname not in cls._pools:
            logger.info(f"Creating connection pool for database: {dbname}")
            try:
                # Create a ThreadedConnectionPool for better concurrency
                cls._pools[dbname] = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=10,  # Adjust based on expected load
                    database=dbname,
                    user=PG_USER,
                    password=PG_PASSWORD,
                    host=PG_HOST,
                    port=PG_PORT
                )
            except Exception as e:
                logger.error(f"Error creating connection pool for {dbname}: {str(e)}")
                raise
        
        return cls._pools[dbname]
    
    @classmethod
    @contextmanager
    def get_connection(cls, dbname):
        """Context manager for database connections"""
        conn_pool = cls.get_pool(dbname)
        conn = None
        try:
            # Get connection from pool
            conn = conn_pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            # Return connection to pool
            if conn is not None:
                conn_pool.putconn(conn)
    
    @classmethod
    def close_all_pools(cls):
        """Close all connection pools (use during application shutdown)"""
        for dbname, cls_pool in cls._pools.items():
            logger.info(f"Closing connection pool for {dbname}")
            cls_pool.closeall()
        cls._pools = {}

    @classmethod
    def execute_query(cls, dbname: str, query: sql.SQL, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple]:
        """Execute a SQL query on the specified database
        
        Args:
            dbname: Database name to connect to
            query: SQL query object (use psycopg2.sql.SQL for safety)
            params: Query parameters (optional)
            
        Returns:
            List of query results for SELECT queries
            Empty list for other query types (INSERT, UPDATE, DELETE)
            
        Raises:
            Exception: If a database error occurs
        """
        with cls.get_connection(dbname) as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    try:
                        results = cursor.fetchall()
                        return results
                    except pg.ProgrammingError:
                        # No results to fetch (e.g., for INSERT, UPDATE, DELETE)
                        return []
            except Exception as e:
                conn.rollback()
                logger.error(f"Error executing query: {str(e)}")
                raise
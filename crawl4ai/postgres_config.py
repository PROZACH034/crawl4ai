import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
import asyncio
from contextlib import asynccontextmanager
from .async_logger import AsyncLogger


@dataclass
class PostgreSQLConfig:
    """PostgreSQL database configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "crawl4ai"
    min_connections: int = 1
    max_connections: int = 20
    
    @classmethod
    def from_env(cls) -> "PostgreSQLConfig":
        """Create configuration from environment variables"""
        return cls(
            host=os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "localhost")),
            port=int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432"))),
            database=os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "crawl4ai")),
            username=os.getenv("POSTGRES_USER", os.getenv("DB_USER", "postgres")),
            password=os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "")),
            schema=os.getenv("POSTGRES_SCHEMA", "crawl4ai"),
            min_connections=int(os.getenv("POSTGRES_MIN_CONN", "1")),
            max_connections=int(os.getenv("POSTGRES_MAX_CONN", "20"))
        )
    
    @classmethod
    def for_supabase(cls) -> "PostgreSQLConfig":
        """Create configuration for Supabase"""
        supabase_url = os.getenv("SUPABASE_URL", "")
        supabase_key = os.getenv("SUPABASE_ANON_KEY", "")
        
        # Extract host from Supabase URL if provided
        if supabase_url:
            # Supabase URL format: https://xxxxx.supabase.co
            host = supabase_url.replace("https://", "").replace("http://", "")
            if not host.endswith(".supabase.co"):
                host = f"{host}.supabase.co"
        else:
            host = os.getenv("SUPABASE_HOST", "")
        
        return cls(
            host=host,
            port=int(os.getenv("SUPABASE_PORT", "5432")),
            database=os.getenv("SUPABASE_DB", "postgres"),
            username=os.getenv("SUPABASE_USER", "postgres"),
            password=os.getenv("SUPABASE_PASSWORD", supabase_key),
            schema=os.getenv("SUPABASE_SCHEMA", "crawl4ai"),
            min_connections=int(os.getenv("SUPABASE_MIN_CONN", "1")),
            max_connections=int(os.getenv("SUPABASE_MAX_CONN", "10"))
        )
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def validate(self) -> bool:
        """Validate configuration"""
        required_fields = [self.host, self.database, self.username, self.password]
        return all(field for field in required_fields)


class PostgreSQLConnectionManager:
    """PostgreSQL connection pool manager"""
    
    def __init__(self, config: PostgreSQLConfig, logger: Optional[AsyncLogger] = None):
        self.config = config
        self.pool: Optional[ThreadedConnectionPool] = None
        self.logger = logger or AsyncLogger(
            log_file=os.path.join(Path.home(), ".crawl4ai", "postgres.log"),
            verbose=False,
            tag_width=10,
        )
        self._initialized = False
    
    async def initialize(self):
        """Initialize the connection pool"""
        if self._initialized:
            return
        
        if not self.config.validate():
            raise ValueError("Invalid PostgreSQL configuration. Missing required fields.")
        
        try:
            loop = asyncio.get_event_loop()
            self.pool = await loop.run_in_executor(
                None, self._create_pool
            )
            self._initialized = True
            self.logger.info("PostgreSQL connection pool initialized", tag="INIT")
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL pool: {str(e)}", tag="ERROR")
            raise
    
    def _create_pool(self) -> ThreadedConnectionPool:
        """Create PostgreSQL connection pool (sync)"""
        return ThreadedConnectionPool(
            minconn=self.config.min_connections,
            maxconn=self.config.max_connections,
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.username,
            password=self.config.password,
            options=f'-c search_path={self.config.schema},public'
        )
    
    def _execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute database query synchronously"""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        conn = None
        try:
            conn = self.pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    if cur.description:
                        return cur.fetchall()
                    return None
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)
    
    async def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute database query asynchronously"""
        if not self._initialized:
            await self.initialize()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_query, query, params, fetch)
    
    async def execute_many(self, query: str, params_list: list):
        """Execute multiple queries with different parameters"""
        if not self._initialized:
            await self.initialize()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_many_sync, query, params_list)
    
    def _execute_many_sync(self, query: str, params_list: list):
        """Execute multiple queries synchronously"""
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        conn = None
        try:
            conn = self.pool.getconn()
            with conn.cursor() as cur:
                cur.executemany(query, params_list)
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool (async context manager)"""
        if not self._initialized:
            await self.initialize()
        
        if not self.pool:
            raise RuntimeError("Connection pool not initialized")
        
        conn = None
        try:
            loop = asyncio.get_event_loop()
            conn = await loop.run_in_executor(None, self.pool.getconn)
            yield conn
        finally:
            if conn:
                await loop.run_in_executor(None, self.pool.putconn, conn)
    
    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = await self.execute_query("SELECT 1", fetch=True)
            return result is not None
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}", tag="ERROR")
            return False
    
    async def close(self):
        """Close all connections in the pool"""
        if self.pool:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.pool.closeall)
            self._initialized = False
            self.logger.info("PostgreSQL connection pool closed", tag="CLEANUP")


# Global instance
_postgres_manager: Optional[PostgreSQLConnectionManager] = None


def get_postgres_manager(config: Optional[PostgreSQLConfig] = None) -> PostgreSQLConnectionManager:
    """Get global PostgreSQL manager instance"""
    global _postgres_manager
    
    if _postgres_manager is None:
        if config is None:
            # Try Supabase first, then fall back to general PostgreSQL
            try:
                config = PostgreSQLConfig.for_supabase()
                if not config.validate():
                    config = PostgreSQLConfig.from_env()
            except:
                config = PostgreSQLConfig.from_env()
        
        _postgres_manager = PostgreSQLConnectionManager(config)
    
    return _postgres_manager


async def close_postgres_manager():
    """Close global PostgreSQL manager"""
    global _postgres_manager
    if _postgres_manager:
        await _postgres_manager.close()
        _postgres_manager = None 
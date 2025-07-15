import os
import json
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, Json

from .postgres_config import PostgreSQLConfig, PostgreSQLConnectionManager, get_postgres_manager
from .postgres_migrations import run_postgres_migrations, get_postgres_migration_status
from .async_logger import AsyncLogger
from .models import CrawlResult


class PostgreSQLDatabaseManager:
    """PostgreSQL database manager for crawl4ai"""
    
    def __init__(self, config: Optional[PostgreSQLConfig] = None, logger: Optional[AsyncLogger] = None):
        self.config = config
        self.connection_manager = get_postgres_manager(config)
        self.logger = logger or AsyncLogger(
            log_file=os.path.join(Path.home(), ".crawl4ai", "postgres_db.log"),
            verbose=False,
            tag_width=10,
        )
        self._initialized = False
    
    async def initialize(self):
        """Initialize the database and run migrations"""
        if self._initialized:
            return
        
        try:
            self.logger.info("Initializing PostgreSQL database", tag="INIT")
            
            # Initialize connection manager
            await self.connection_manager.initialize()
            
            # Test connection
            if not await self.connection_manager.test_connection():
                raise RuntimeError("Failed to connect to PostgreSQL database")
            
            # Run migrations
            self.logger.info("Running database migrations", tag="INIT")
            migration_success = await run_postgres_migrations(self.config)
            
            if not migration_success:
                raise RuntimeError("Database migrations failed")
            
            # Get migration status for logging
            migration_status = await get_postgres_migration_status(self.config)
            self.logger.info(
                f"Database ready - {migration_status.get('applied_count', 0)} migrations applied",
                tag="INIT"
            )
            
            self._initialized = True
            self.logger.success("PostgreSQL database initialized successfully", tag="COMPLETE")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {str(e)}", tag="ERROR")
            raise
    
    async def save_crawl_metadata(self, url: str, raw_html: str = "", text_content: str = "", 
                                  http_headers: Optional[Dict] = None, lang: str = "en") -> int:
        """Save crawl metadata and return the ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Convert headers to JSONB if provided
            headers_json = Json(http_headers) if http_headers else None
            
            result = await self.connection_manager.execute_query(
                """
                INSERT INTO crawl4ai.crawl_metadata (source_url, raw_html, text_content, http_headers, lang)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    raw_html = EXCLUDED.raw_html,
                    text_content = EXCLUDED.text_content,
                    http_headers = EXCLUDED.http_headers,
                    lang = EXCLUDED.lang,
                    scraped_at = now()
                RETURNING id
                """,
                (url, raw_html, text_content, headers_json, lang),
                fetch=True
            )
            
            metadata_id = result[0]['id'] if result else None
            self.logger.debug(f"Saved crawl metadata for {url} with ID {metadata_id}", tag="SAVE")
            return metadata_id
            
        except Exception as e:
            self.logger.error(f"Failed to save crawl metadata: {str(e)}", tag="ERROR")
            raise
    
    async def get_crawl_metadata(self, url: str) -> Optional[Dict]:
        """Get crawl metadata by URL"""
        if not self._initialized:
            await self.initialize()
        
        try:
            result = await self.connection_manager.execute_query(
                "SELECT * FROM crawl4ai.crawl_metadata WHERE source_url = %s",
                (url,),
                fetch=True
            )
            
            return dict(result[0]) if result else None
            
        except Exception as e:
            self.logger.error(f"Failed to get crawl metadata: {str(e)}", tag="ERROR")
            return None
    
    async def save_event_code(self, event_id: int, provider: str = "", channel: str = "", 
                              level: Optional[int] = None, level_name: str = "",
                              task: str = "", opcode: str = "", keywords: Optional[int] = None,
                              version: Optional[int] = None) -> int:
        """Save event code and return the ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            result = await self.connection_manager.execute_query(
                """
                INSERT INTO crawl4ai.event_codes 
                (event_id, provider, channel, level, level_name, task, opcode, keywords, version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, provider, channel, version) DO UPDATE SET
                    level = EXCLUDED.level,
                    level_name = EXCLUDED.level_name,
                    task = EXCLUDED.task,
                    opcode = EXCLUDED.opcode,
                    keywords = EXCLUDED.keywords,
                    created_at = now()
                RETURNING id
                """,
                (event_id, provider, channel, level, level_name, task, opcode, keywords, version),
                fetch=True
            )
            
            code_id = result[0]['id'] if result else None
            self.logger.debug(f"Saved event code {event_id} with ID {code_id}", tag="SAVE")
            return code_id
            
        except Exception as e:
            self.logger.error(f"Failed to save event code: {str(e)}", tag="ERROR")
            raise
    
    async def save_event_description(self, code_id: int, title: str = "", description: str = "",
                                     fix: str = "", source_url: str = "", source_type: str = "",
                                     lang: str = "en") -> int:
        """Save event description and return the ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            result = await self.connection_manager.execute_query(
                """
                INSERT INTO crawl4ai.event_descriptions 
                (code_id, title, description, fix, source_url, source_type, lang)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code_id, lang) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    fix = EXCLUDED.fix,
                    source_url = EXCLUDED.source_url,
                    source_type = EXCLUDED.source_type,
                    scraped_at = now()
                RETURNING id
                """,
                (code_id, title, description, fix, source_url, source_type, lang),
                fetch=True
            )
            
            desc_id = result[0]['id'] if result else None
            self.logger.debug(f"Saved event description for code_id {code_id} with ID {desc_id}", tag="SAVE")
            return desc_id
            
        except Exception as e:
            self.logger.error(f"Failed to save event description: {str(e)}", tag="ERROR")
            raise
    
    async def save_event_log(self, code_id: int, record_id: Optional[int] = None, 
                             computer: str = "", user_sid: str = "", timestamp: Optional[datetime] = None,
                             raw_xml: str = "", text_message: str = "", event_data: Optional[Dict] = None) -> int:
        """Save event log entry and return the ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Convert event_data to JSONB if provided
            event_data_json = Json(event_data) if event_data else None
            
            result = await self.connection_manager.execute_query(
                """
                INSERT INTO crawl4ai.event_logs 
                (code_id, record_id, computer, user_sid, timestamp, raw_xml, text_message, event_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (code_id, record_id, computer, user_sid, timestamp, raw_xml, text_message, event_data_json),
                fetch=True
            )
            
            log_id = result[0]['id'] if result else None
            self.logger.debug(f"Saved event log for code_id {code_id} with ID {log_id}", tag="SAVE")
            return log_id
            
        except Exception as e:
            self.logger.error(f"Failed to save event log: {str(e)}", tag="ERROR")
            raise
    
    async def save_event_reference(self, description_id: int, ref_url: str, note: str = "") -> int:
        """Save event reference and return the ID"""
        if not self._initialized:
            await self.initialize()
        
        try:
            result = await self.connection_manager.execute_query(
                """
                INSERT INTO crawl4ai.event_references (description_id, ref_url, note)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (description_id, ref_url, note),
                fetch=True
            )
            
            ref_id = result[0]['id'] if result else None
            self.logger.debug(f"Saved event reference for description_id {description_id} with ID {ref_id}", tag="SAVE")
            return ref_id
            
        except Exception as e:
            self.logger.error(f"Failed to save event reference: {str(e)}", tag="ERROR")
            raise
    
    async def get_event_codes_with_descriptions(self, event_id: Optional[int] = None, 
                                                provider: Optional[str] = None,
                                                lang: str = "en") -> List[Dict]:
        """Get event codes with their descriptions using the view"""
        if not self._initialized:
            await self.initialize()
        
        try:
            where_conditions = ["lang = %s OR lang IS NULL"]
            params = [lang]
            
            if event_id is not None:
                where_conditions.append("event_id = %s")
                params.append(event_id)
            
            if provider is not None:
                where_conditions.append("provider = %s")
                params.append(provider)
            
            where_clause = " AND ".join(where_conditions)
            
            result = await self.connection_manager.execute_query(
                f"""
                SELECT * FROM crawl4ai.event_codes_with_descriptions
                WHERE {where_clause}
                ORDER BY event_id, provider
                """,
                tuple(params),
                fetch=True
            )
            
            return [dict(row) for row in (result or [])]
            
        except Exception as e:
            self.logger.error(f"Failed to get event codes with descriptions: {str(e)}", tag="ERROR")
            return []
    
    async def search_events(self, search_term: str, lang: str = "en") -> List[Dict]:
        """Search events by title, description, or fix content"""
        if not self._initialized:
            await self.initialize()
        
        try:
            result = await self.connection_manager.execute_query(
                """
                SELECT * FROM crawl4ai.event_codes_with_descriptions
                WHERE (lang = %s OR lang IS NULL)
                AND (
                    title ILIKE %s OR 
                    description ILIKE %s OR 
                    fix ILIKE %s OR
                    CAST(event_id AS TEXT) = %s
                )
                ORDER BY event_id
                """,
                (lang, f"%{search_term}%", f"%{search_term}%", f"%{search_term}%", search_term),
                fetch=True
            )
            
            return [dict(row) for row in (result or [])]
            
        except Exception as e:
            self.logger.error(f"Failed to search events: {str(e)}", tag="ERROR")
            return []
    
    async def get_database_stats(self) -> Dict:
        """Get database statistics"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Get table counts
            tables = ['crawl_metadata', 'event_codes', 'event_descriptions', 'event_logs', 'event_references']
            stats = {}
            
            for table in tables:
                result = await self.connection_manager.execute_query(
                    f"SELECT COUNT(*) as count FROM crawl4ai.{table}",
                    fetch=True
                )
                stats[f"{table}_count"] = result[0]['count'] if result else 0
            
            # Get migration status
            migration_status = await get_postgres_migration_status(self.config)
            stats['migration_status'] = migration_status
            
            # Get recent activity
            result = await self.connection_manager.execute_query(
                """
                SELECT 
                    DATE_TRUNC('day', scraped_at) as date,
                    COUNT(*) as crawls
                FROM crawl4ai.crawl_metadata 
                WHERE scraped_at >= NOW() - INTERVAL '7 days'
                GROUP BY DATE_TRUNC('day', scraped_at)
                ORDER BY date DESC
                """,
                fetch=True
            )
            stats['recent_crawl_activity'] = [dict(row) for row in (result or [])]
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get database stats: {str(e)}", tag="ERROR")
            return {"error": str(e)}
    
    async def save_crawl_result(self, result: CrawlResult) -> Optional[int]:
        """Save a complete CrawlResult to the database"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # Extract HTTP headers if available
            headers = {}
            if hasattr(result, 'response_headers') and result.response_headers:
                headers = result.response_headers
            
            # Save crawl metadata
            metadata_id = await self.save_crawl_metadata(
                url=result.url,
                raw_html=result.html or "",
                text_content=result.cleaned_html or "",
                http_headers=headers,
                lang="en"  # Could be extracted from result if available
            )
            
            self.logger.info(f"Saved crawl result for {result.url} with metadata ID {metadata_id}", tag="SAVE")
            return metadata_id
            
        except Exception as e:
            self.logger.error(f"Failed to save crawl result: {str(e)}", tag="ERROR")
            return None
    
    async def cleanup(self):
        """Cleanup database connections"""
        if self.connection_manager:
            await self.connection_manager.close()
        self._initialized = False
        self.logger.info("PostgreSQL database manager cleaned up", tag="CLEANUP")


# Global instance
_postgres_db_manager: Optional[PostgreSQLDatabaseManager] = None


def get_postgres_db_manager(config: Optional[PostgreSQLConfig] = None) -> PostgreSQLDatabaseManager:
    """Get global PostgreSQL database manager instance"""
    global _postgres_db_manager
    
    if _postgres_db_manager is None:
        _postgres_db_manager = PostgreSQLDatabaseManager(config)
    
    return _postgres_db_manager


async def close_postgres_db_manager():
    """Close global PostgreSQL database manager"""
    global _postgres_db_manager
    if _postgres_db_manager:
        await _postgres_db_manager.cleanup()
        _postgres_db_manager = None 
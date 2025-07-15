import os
import re
import json
import asyncio
from typing import Dict, Optional, Tuple
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

from crawl4ai import BrowserConfig, AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.hub import BaseCrawler
from crawl4ai import JsonCssExtractionStrategy


__meta__ = {
    "version": "1.0.0",
    "tested_on": ["learn.microsoft.com/en-us/windows/security/threat-protection/auditing/*"],
    "rate_limit": "30 RPM",
    "description": "Crawls Microsoft Docs event pages and stores structured data in PostgreSQL"
}


class MicrosoftEventsCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.pool = None
        self._init_db_pool()
        
    def _init_db_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            self.logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize DB pool: {str(e)}")
            raise

    def _execute_db_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute database query synchronously (for use with run_in_executor)"""
        conn = None
        try:
            conn = self.pool.getconn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall() if cur.description else None
                    return result
                conn.commit()
                return cur.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)

    async def _async_db_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Execute database query asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_db_query, query, params, fetch)

    async def _init_database_schema(self):
        """Initialize database tables if they don't exist"""
        try:
            # Create crawl_metadata table
            await self._async_db_query("""
                CREATE TABLE IF NOT EXISTS crawl_metadata (
                    id BIGSERIAL PRIMARY KEY,
                    source_url TEXT NOT NULL,
                    raw_html TEXT,
                    text_content TEXT,
                    lang CHAR(2) NOT NULL
                )
            """)

            # Create event_codes table
            await self._async_db_query("""
                CREATE TABLE IF NOT EXISTS event_codes (
                    id SERIAL PRIMARY KEY,
                    event_id VARCHAR(64) NOT NULL UNIQUE,
                    provider VARCHAR(64) NOT NULL,
                    channel VARCHAR(64) NOT NULL,
                    level_name VARCHAR(32) NOT NULL
                )
            """)

            # Create event_descriptions table
            await self._async_db_query("""
                CREATE TABLE IF NOT EXISTS event_descriptions (
                    id SERIAL PRIMARY KEY,
                    code_id INTEGER NOT NULL REFERENCES event_codes(id),
                    title TEXT NOT NULL,
                    description TEXT,
                    fix TEXT,
                    source_url TEXT,
                    source_type VARCHAR(32),
                    lang CHAR(2) NOT NULL,
                    UNIQUE(code_id, lang)
                )
            """)

            self.logger.info("Database schema initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize database schema: {str(e)}")
            raise

    def _extract_event_id(self, url: str) -> Optional[str]:
        """Extract event ID from Microsoft Docs URL"""
        match = re.search(r'event-(\d+)', url)
        return match.group(1) if match else None

    def _build_extraction_schema(self) -> Dict:
        """Build CSS extraction schema for Microsoft Docs event pages"""
        return {
            "name": "microsoft_event_data",
            "baseSelector": "body",
            "fields": [
                {
                    "name": "title",
                    "selector": "h1",
                    "type": "text"
                },
                {
                    "name": "main_content",
                    "selector": "main.content, #main",
                    "type": "text"
                },
                {
                    "name": "event_details_table",
                    "selector": "table.table.table-bordered",
                    "type": "nested",
                    "fields": [
                        {
                            "name": "rows",
                            "selector": "tr",
                            "type": "nested",
                            "fields": [
                                {
                                    "name": "cells",
                                    "selector": "td, th",
                                    "type": "text",
                                    "multiple": True
                                }
                            ],
                            "multiple": True
                        }
                    ]
                },
                {
                    "name": "description_paragraphs",
                    "selector": "p",
                    "type": "text",
                    "multiple": True
                },
                {
                    "name": "example_logs",
                    "selector": "pre > code.language-text",
                    "type": "text",
                    "multiple": True
                }
            ]
        }

    async def _save_to_db(self, url: str, event_id: str, extracted_data: Dict, raw_html: str, text_content: str, lang: str = "en") -> Dict:
        """Save extracted data to database with atomic transactions"""
        try:
            # Start transaction by getting connection
            conn = None
            try:
                loop = asyncio.get_event_loop()
                conn = await loop.run_in_executor(None, self.pool.getconn)
                
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Insert crawl metadata
                    cur.execute("""
                        INSERT INTO crawl_metadata (source_url, raw_html, text_content, lang)
                        VALUES (%s, %s, %s, %s) RETURNING id
                    """, (url, raw_html, text_content, lang))
                    metadata_id = cur.fetchone()['id']

                    # Extract event details from table data
                    event_details = self._parse_event_details(extracted_data)
                    
                    # Insert event code with conflict handling
                    cur.execute("""
                        INSERT INTO event_codes (event_id, provider, channel, level_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                        RETURNING id
                    """, (
                        event_id,
                        event_details.get('provider', 'Microsoft'),
                        event_details.get('channel', 'Security'),
                        event_details.get('level', 'Information')
                    ))
                    
                    result = cur.fetchone()
                    if result:
                        code_id = result['id']
                    else:
                        # Get existing code_id
                        cur.execute("SELECT id FROM event_codes WHERE event_id = %s", (event_id,))
                        code_id = cur.fetchone()['id']

                    # Prepare description data
                    title = extracted_data.get('title', '')
                    description = self._build_description(extracted_data)
                    fix_info = self._extract_fix_info(extracted_data)

                    # Insert event description
                    cur.execute("""
                        INSERT INTO event_descriptions (code_id, title, description, fix, source_url, source_type, lang)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (code_id, lang) DO UPDATE SET
                            title = EXCLUDED.title,
                            description = EXCLUDED.description,
                            fix = EXCLUDED.fix,
                            source_url = EXCLUDED.source_url,
                            source_type = EXCLUDED.source_type
                    """, (code_id, title, description, fix_info, url, 'microsoft_docs', lang))

                    conn.commit()
                    
                    return {
                        "success": True,
                        "metadata_id": metadata_id,
                        "code_id": code_id,
                        "event_id": event_id,
                        "message": "Data saved successfully"
                    }

            except Exception as e:
                if conn:
                    conn.rollback()
                raise e
            finally:
                if conn:
                    self.pool.putconn(conn)

        except Exception as e:
            self.logger.error(f"Database save failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "event_id": event_id
            }

    def _parse_event_details(self, extracted_data: Dict) -> Dict:
        """Parse event details from extracted table data"""
        details = {}
        table_data = extracted_data.get('event_details_table', {})
        
        if 'rows' in table_data:
            for row in table_data['rows']:
                cells = row.get('cells', [])
                if len(cells) >= 2:
                    key = cells[0].lower().strip()
                    value = cells[1].strip()
                    
                    if 'provider' in key:
                        details['provider'] = value
                    elif 'channel' in key:
                        details['channel'] = value
                    elif 'level' in key:
                        details['level'] = value
        
        return details

    def _build_description(self, extracted_data: Dict) -> str:
        """Build comprehensive description from extracted data"""
        description_parts = []
        
        # Add main content
        main_content = extracted_data.get('main_content', '')
        if main_content:
            description_parts.append(main_content)
        
        # Add description paragraphs
        paragraphs = extracted_data.get('description_paragraphs', [])
        if paragraphs:
            description_parts.extend(paragraphs)
        
        return '\n\n'.join(description_parts)

    def _extract_fix_info(self, extracted_data: Dict) -> str:
        """Extract fix/remediation information from the content"""
        # Look for fix-related content in paragraphs
        paragraphs = extracted_data.get('description_paragraphs', [])
        fix_keywords = ['fix', 'resolve', 'solution', 'remediation', 'mitigation']
        
        fix_paragraphs = []
        for paragraph in paragraphs:
            if any(keyword in paragraph.lower() for keyword in fix_keywords):
                fix_paragraphs.append(paragraph)
        
        return '\n\n'.join(fix_paragraphs) if fix_paragraphs else None

    async def run(self, url: str = "", **kwargs) -> str:
        """Main crawler method"""
        try:
            if not url:
                return json.dumps({"error": "URL is required"})

            # Extract event ID from URL
            event_id = self._extract_event_id(url)
            if not event_id:
                return json.dumps({"error": "Could not extract event ID from URL"})

            self.logger.info(f"Crawling Microsoft event page: {url} (Event ID: {event_id})")

            # Initialize database schema
            await self._init_database_schema()

            # Configure browser and crawler
            browser_config = BrowserConfig(headless=True, verbose=True)
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                config = CrawlerRunConfig(
                    cache_mode=kwargs.get("cache_mode", CacheMode.BYPASS),
                    keep_attrs=["id", "class"],
                    keep_data_attributes=True,
                    delay_before_return_html=kwargs.get("delay", 2)
                )

                # Crawl the page
                result = await crawler.arun(url=url, config=config)
                if not result.success:
                    return json.dumps({"error": f"Crawl failed: {result.error}"})

                # Extract structured data
                schema = self._build_extraction_schema()
                extraction_strategy = JsonCssExtractionStrategy(schema=schema)
                extracted_data = extraction_strategy.run(url=url, sections=[result.html])

                # Save to database
                save_result = await self._save_to_db(
                    url=url,
                    event_id=event_id,
                    extracted_data=extracted_data,
                    raw_html=result.html,
                    text_content=result.cleaned_html,
                    lang=kwargs.get("lang", "en")
                )

                # Prepare response
                response = {
                    "event_id": event_id,
                    "url": url,
                    "extracted_data": extracted_data,
                    "database_result": save_result
                }

                return json.dumps(response, indent=4)

        except Exception as e:
            self.logger.error(f"Crawler run failed: {str(e)}")
            return json.dumps({
                "error": str(e),
                "url": url,
                "metadata": __meta__
            })

    def __del__(self):
        """Cleanup connection pool on destruction"""
        if hasattr(self, 'pool') and self.pool:
            try:
                self.pool.closeall()
            except:
                pass
import os
import re
import asyncio
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from .postgres_config import PostgreSQLConfig, PostgreSQLConnectionManager, get_postgres_manager
from .async_logger import AsyncLogger


class PostgreSQLMigrationManager:
    """PostgreSQL migration manager for crawl4ai"""
    
    def __init__(self, config: Optional[PostgreSQLConfig] = None, logger: Optional[AsyncLogger] = None):
        self.config = config
        self.db_manager = get_postgres_manager(config)
        self.logger = logger or AsyncLogger(
            log_file=os.path.join(Path.home(), ".crawl4ai", "migrations.log"),
            verbose=True,
            tag_width=10,
        )
        
        # Migration paths
        self.base_dir = Path(__file__).parent
        self.migrations_dir = self.base_dir / "migrations" / "postgres"
        self.migrations_dir.mkdir(parents=True, exist_ok=True)
    
    async def initialize_migration_table(self):
        """Create the migration tracking table if it doesn't exist"""
        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS crawl4ai;
        
        CREATE TABLE IF NOT EXISTS crawl4ai.migrations (
            id SERIAL PRIMARY KEY,
            version VARCHAR(50) NOT NULL UNIQUE,
            filename VARCHAR(255) NOT NULL,
            applied_at TIMESTAMPTZ DEFAULT now(),
            checksum VARCHAR(64),
            execution_time_ms INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_migrations_version ON crawl4ai.migrations(version);
        CREATE INDEX IF NOT EXISTS idx_migrations_applied_at ON crawl4ai.migrations(applied_at);
        """
        
        try:
            await self.db_manager.execute_query(create_table_sql)
            self.logger.info("Migration tracking table initialized", tag="INIT")
        except Exception as e:
            self.logger.error(f"Failed to initialize migration table: {str(e)}", tag="ERROR")
            raise
    
    async def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions"""
        try:
            result = await self.db_manager.execute_query(
                "SELECT version FROM crawl4ai.migrations ORDER BY applied_at",
                fetch=True
            )
            return [row['version'] for row in (result or [])]
        except Exception as e:
            self.logger.error(f"Failed to get applied migrations: {str(e)}", tag="ERROR")
            return []
    
    def get_migration_files(self) -> List[Tuple[str, Path]]:
        """Get all migration files sorted by version"""
        migration_files = []
        
        if not self.migrations_dir.exists():
            return migration_files
        
        for file_path in self.migrations_dir.glob("*.sql"):
            # Extract version from filename (e.g., "001_initial_schema.sql" -> "001")
            match = re.match(r"^(\d+)_.*\.sql$", file_path.name)
            if match:
                version = match.group(1)
                migration_files.append((version, file_path))
        
        # Sort by version number
        migration_files.sort(key=lambda x: int(x[0]))
        return migration_files
    
    def calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate checksum of migration file content"""
        import hashlib
        content = file_path.read_text(encoding='utf-8')
        return hashlib.sha256(content.encode()).hexdigest()
    
    async def execute_migration_file(self, file_path: Path, version: str) -> bool:
        """Execute a single migration file"""
        try:
            start_time = datetime.now()
            
            # Read migration file
            sql_content = file_path.read_text(encoding='utf-8')
            checksum = self.calculate_file_checksum(file_path)
            
            self.logger.info(f"Executing migration {version}: {file_path.name}", tag="MIGRATE")
            
            # Split SQL content into individual statements
            # Handle multiple statements separated by semicolons
            statements = self._split_sql_statements(sql_content)
            
            # Execute each statement
            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        await self.db_manager.execute_query(statement)
                        self.logger.debug(f"Statement {i+1}/{len(statements)} executed", tag="MIGRATE")
                    except Exception as e:
                        self.logger.error(f"Failed to execute statement {i+1}: {str(e)}", tag="ERROR")
                        raise
            
            # Record migration as applied
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            await self.db_manager.execute_query(
                """
                INSERT INTO crawl4ai.migrations (version, filename, checksum, execution_time_ms)
                VALUES (%s, %s, %s, %s)
                """,
                (version, file_path.name, checksum, execution_time)
            )
            
            self.logger.success(
                f"Migration {version} completed in {execution_time}ms", 
                tag="MIGRATE"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Migration {version} failed: {str(e)}", tag="ERROR")
            raise
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """Split SQL content into individual statements"""
        # Remove comments and split by semicolon
        # This is a simple approach - for more complex SQL, consider using a proper SQL parser
        
        # Remove single-line comments
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
        
        # Remove multi-line comments
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split by semicolon and filter empty statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        return statements
    
    async def run_pending_migrations(self) -> bool:
        """Run all pending migrations"""
        try:
            # Initialize migration tracking table
            await self.initialize_migration_table()
            
            # Get applied migrations
            applied_migrations = await self.get_applied_migrations()
            self.logger.info(f"Found {len(applied_migrations)} applied migrations", tag="MIGRATE")
            
            # Get all migration files
            migration_files = self.get_migration_files()
            self.logger.info(f"Found {len(migration_files)} migration files", tag="MIGRATE")
            
            if not migration_files:
                self.logger.warning("No migration files found", tag="MIGRATE")
                return True
            
            # Find pending migrations
            pending_migrations = [
                (version, file_path) for version, file_path in migration_files
                if version not in applied_migrations
            ]
            
            if not pending_migrations:
                self.logger.info("No pending migrations", tag="MIGRATE")
                return True
            
            self.logger.info(f"Running {len(pending_migrations)} pending migrations", tag="MIGRATE")
            
            # Execute pending migrations
            for version, file_path in pending_migrations:
                await self.execute_migration_file(file_path, version)
            
            self.logger.success("All migrations completed successfully", tag="COMPLETE")
            return True
            
        except Exception as e:
            self.logger.error(f"Migration process failed: {str(e)}", tag="ERROR")
            return False
    
    async def rollback_migration(self, version: str) -> bool:
        """Rollback a specific migration (if rollback file exists)"""
        rollback_file = self.migrations_dir / f"{version}_rollback.sql"
        
        if not rollback_file.exists():
            self.logger.error(f"Rollback file not found for migration {version}", tag="ERROR")
            return False
        
        try:
            self.logger.info(f"Rolling back migration {version}", tag="ROLLBACK")
            
            # Execute rollback SQL
            sql_content = rollback_file.read_text(encoding='utf-8')
            statements = self._split_sql_statements(sql_content)
            
            for statement in statements:
                if statement.strip():
                    await self.db_manager.execute_query(statement)
            
            # Remove migration record
            await self.db_manager.execute_query(
                "DELETE FROM crawl4ai.migrations WHERE version = %s",
                (version,)
            )
            
            self.logger.success(f"Migration {version} rolled back successfully", tag="ROLLBACK")
            return True
            
        except Exception as e:
            self.logger.error(f"Rollback failed: {str(e)}", tag="ERROR")
            return False
    
    async def get_migration_status(self) -> Dict:
        """Get current migration status"""
        try:
            await self.initialize_migration_table()
            
            applied_migrations = await self.get_applied_migrations()
            migration_files = self.get_migration_files()
            
            pending_migrations = [
                version for version, _ in migration_files
                if version not in applied_migrations
            ]
            
            # Get detailed migration info
            detailed_migrations = await self.db_manager.execute_query(
                """
                SELECT version, filename, applied_at, execution_time_ms
                FROM crawl4ai.migrations
                ORDER BY applied_at DESC
                LIMIT 10
                """,
                fetch=True
            )
            
            return {
                "total_migrations": len(migration_files),
                "applied_count": len(applied_migrations),
                "pending_count": len(pending_migrations),
                "pending_migrations": pending_migrations,
                "recent_migrations": detailed_migrations or [],
                "status": "up_to_date" if not pending_migrations else "pending_migrations"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get migration status: {str(e)}", tag="ERROR")
            return {"error": str(e)}
    
    def create_migration_file(self, name: str, content: str) -> Path:
        """Create a new migration file"""
        # Get next version number
        existing_migrations = self.get_migration_files()
        next_version = "001"
        
        if existing_migrations:
            last_version = max(int(version) for version, _ in existing_migrations)
            next_version = f"{last_version + 1:03d}"
        
        # Create filename
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        filename = f"{next_version}_{clean_name}.sql"
        file_path = self.migrations_dir / filename
        
        # Write content
        file_path.write_text(content, encoding='utf-8')
        
        self.logger.info(f"Created migration file: {filename}", tag="CREATE")
        return file_path


# Global migration manager instance
_migration_manager: Optional[PostgreSQLMigrationManager] = None


def get_migration_manager(config: Optional[PostgreSQLConfig] = None) -> PostgreSQLMigrationManager:
    """Get global migration manager instance"""
    global _migration_manager
    
    if _migration_manager is None:
        _migration_manager = PostgreSQLMigrationManager(config)
    
    return _migration_manager


async def run_postgres_migrations(config: Optional[PostgreSQLConfig] = None) -> bool:
    """Run PostgreSQL migrations (convenience function)"""
    manager = get_migration_manager(config)
    return await manager.run_pending_migrations()


async def get_postgres_migration_status(config: Optional[PostgreSQLConfig] = None) -> Dict:
    """Get PostgreSQL migration status (convenience function)"""
    manager = get_migration_manager(config)
    return await manager.get_migration_status() 
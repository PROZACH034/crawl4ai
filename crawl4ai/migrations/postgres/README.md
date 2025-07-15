# Crawl4AI PostgreSQL Migrations

This directory contains PostgreSQL migrations for the crawl4ai project. The migration system automatically manages database schema creation and updates.

## Overview

The crawl4ai project now supports PostgreSQL as an alternative to SQLite, with full migration support for Supabase and other PostgreSQL providers.

### Database Schema

The following tables are created by the migration system:

1. **crawl_metadata** - Stores metadata about crawled web pages
2. **event_codes** - Windows Event codes information  
3. **event_descriptions** - Human-readable descriptions for event codes
4. **event_logs** - Actual Windows event log entries
5. **event_references** - External references for event descriptions
6. **migrations** - Migration tracking table

### Views

- **event_codes_with_descriptions** - Consolidated view combining event codes and descriptions

## Migration Files

Migration files follow the naming convention: `XXX_description.sql`

- `001_initial_schema.sql` - Creates the initial database schema with all tables, indexes, and views

## Automatic Migration

Migrations run automatically when:
1. The application starts up (if PostgreSQL is configured)
2. The `run_postgres_migrations()` function is called

## Manual Migration

You can run migrations manually:

```python
import asyncio
from crawl4ai import run_postgres_migrations

# Run all pending migrations
asyncio.run(run_postgres_migrations())
```

## Configuration

See `crawl4ai/config/postgres.env.example` for configuration options.

### Supabase Configuration

```bash
SUPABASE_URL=https://xxxxxxxxxxxx.supabase.co
SUPABASE_PASSWORD=your_database_password
```

### Generic PostgreSQL Configuration

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crawl4ai
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

## Migration Status

Check migration status:

```python
import asyncio
from crawl4ai import get_postgres_migration_status

status = asyncio.run(get_postgres_migration_status())
print(status)
```

## Creating New Migrations

To create a new migration:

```python
from crawl4ai.postgres_migrations import get_migration_manager

manager = get_migration_manager()
migration_file = manager.create_migration_file(
    name="add_new_table",
    content="CREATE TABLE new_table (id SERIAL PRIMARY KEY);"
)
```

## Rollback Support

Rollback files can be created alongside migration files:
- `001_initial_schema.sql` (forward migration)
- `001_rollback.sql` (rollback migration)

## Security Notes

- Use environment variables for database credentials
- Ensure your PostgreSQL user has appropriate permissions
- The migration system creates a `crawl4ai` schema by default
- All tables are created within this schema for isolation

## Troubleshooting

### Connection Issues
- Verify your database credentials
- Ensure the database server is accessible
- Check firewall settings

### Migration Failures
- Check the migration logs in `~/.crawl4ai/migrations.log`
- Verify database permissions
- Ensure the database exists before running migrations

### Supabase Specific
- Ensure you're using the database password, not the project API key
- The default database name is `postgres`
- Connection string format: `postgresql://postgres:password@host:5432/postgres` 
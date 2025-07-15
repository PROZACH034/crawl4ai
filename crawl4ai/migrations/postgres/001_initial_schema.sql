-- =====================================================
-- Crawl4AI PostgreSQL Schema Migration
-- Version: 001
-- Description: Initial schema creation
-- Created: 2025-01-15
-- =====================================================

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS crawl4ai;

-- Set search path to use crawl4ai schema by default
SET search_path TO crawl4ai, public;

-- =====================================================
-- 1. crawl_metadata table
-- =====================================================
CREATE TABLE IF NOT EXISTS crawl_metadata (
    id SERIAL PRIMARY KEY,
    source_url TEXT NOT NULL,
    http_headers JSONB,
    raw_html TEXT,
    text_content TEXT,
    lang CHAR(2) NOT NULL DEFAULT 'en',
    scraped_at TIMESTAMPTZ DEFAULT now()
);

-- Create unique index on source_url
CREATE UNIQUE INDEX IF NOT EXISTS idx_crawl_metadata_source_url ON crawl_metadata(source_url);

-- Additional indexes for performance
CREATE INDEX IF NOT EXISTS idx_crawl_metadata_lang ON crawl_metadata(lang);
CREATE INDEX IF NOT EXISTS idx_crawl_metadata_scraped_at ON crawl_metadata(scraped_at);

-- =====================================================
-- 2. event_codes table
-- =====================================================
CREATE TABLE IF NOT EXISTS event_codes (
    id SERIAL PRIMARY KEY,
    event_id INT NOT NULL,
    provider VARCHAR(255),
    channel VARCHAR(255),
    level INT,
    level_name VARCHAR(255),
    task VARCHAR(255),
    opcode VARCHAR(255),
    keywords BIGINT,
    version INT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Create unique constraint on combination of event_id, provider, channel, version
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_codes_unique ON event_codes(event_id, provider, channel, version);

-- Additional indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_codes_event_id ON event_codes(event_id);
CREATE INDEX IF NOT EXISTS idx_event_codes_provider ON event_codes(provider);
CREATE INDEX IF NOT EXISTS idx_event_codes_channel ON event_codes(channel);
CREATE INDEX IF NOT EXISTS idx_event_codes_level ON event_codes(level);
CREATE INDEX IF NOT EXISTS idx_event_codes_created_at ON event_codes(created_at);

-- =====================================================
-- 3. event_descriptions table
-- =====================================================
CREATE TABLE IF NOT EXISTS event_descriptions (
    id SERIAL PRIMARY KEY,
    code_id INT NOT NULL REFERENCES event_codes(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    fix TEXT,
    source_url TEXT,
    source_type VARCHAR(255),
    lang CHAR(2) DEFAULT 'en',
    scraped_at TIMESTAMPTZ DEFAULT now()
);

-- Create unique constraint on code_id and lang combination
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_descriptions_unique ON event_descriptions(code_id, lang);

-- Additional indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_descriptions_code_id ON event_descriptions(code_id);
CREATE INDEX IF NOT EXISTS idx_event_descriptions_lang ON event_descriptions(lang);
CREATE INDEX IF NOT EXISTS idx_event_descriptions_source_type ON event_descriptions(source_type);
CREATE INDEX IF NOT EXISTS idx_event_descriptions_scraped_at ON event_descriptions(scraped_at);

-- =====================================================
-- 4. event_logs table
-- =====================================================
CREATE TABLE IF NOT EXISTS event_logs (
    id SERIAL PRIMARY KEY,
    code_id INT NOT NULL REFERENCES event_codes(id) ON DELETE CASCADE,
    record_id BIGINT,
    computer TEXT,
    user_sid TEXT,
    timestamp TIMESTAMPTZ,
    raw_xml TEXT,
    text_message TEXT,
    event_data JSONB,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_logs_code_id ON event_logs(code_id);
CREATE INDEX IF NOT EXISTS idx_event_logs_timestamp ON event_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_event_logs_computer ON event_logs(computer);
CREATE INDEX IF NOT EXISTS idx_event_logs_record_id ON event_logs(record_id);
CREATE INDEX IF NOT EXISTS idx_event_logs_inserted_at ON event_logs(inserted_at);

-- GIN index for JSONB event_data
CREATE INDEX IF NOT EXISTS idx_event_logs_event_data_gin ON event_logs USING GIN (event_data);

-- =====================================================
-- 5. event_references table
-- =====================================================
CREATE TABLE IF NOT EXISTS event_references (
    id SERIAL PRIMARY KEY,
    description_id INT NOT NULL REFERENCES event_descriptions(id) ON DELETE CASCADE,
    ref_url TEXT,
    note TEXT
);

-- Index for foreign key
CREATE INDEX IF NOT EXISTS idx_event_references_description_id ON event_references(description_id);

-- =====================================================
-- 6. Views
-- =====================================================

-- Create view for event codes with descriptions
CREATE OR REPLACE VIEW event_codes_with_descriptions AS
SELECT 
    ec.id AS event_code_dbid,
    ec.event_id,
    ec.provider,
    ec.channel,
    ec.level,
    ec.level_name,
    ec.task,
    ec.opcode,
    ec.keywords,
    ec.version,
    ed.id AS description_id,
    ed.title,
    ed.description,
    ed.fix,
    ed.source_url,
    ed.source_type,
    ed.lang
FROM event_codes ec
LEFT JOIN event_descriptions ed ON ec.id = ed.code_id;

-- =====================================================
-- 7. Additional performance optimizations
-- =====================================================

-- Add comments for documentation
COMMENT ON SCHEMA crawl4ai IS 'Crawl4AI database schema for web crawling and Windows event data';
COMMENT ON TABLE crawl_metadata IS 'Stores metadata about crawled web pages';
COMMENT ON TABLE event_codes IS 'Stores Windows Event codes information';
COMMENT ON TABLE event_descriptions IS 'Stores human-readable descriptions for event codes';
COMMENT ON TABLE event_logs IS 'Stores actual Windows event log entries';
COMMENT ON TABLE event_references IS 'Stores external references for event descriptions';

-- Add column comments for clarity
COMMENT ON COLUMN crawl_metadata.source_url IS 'The URL that was crawled';
COMMENT ON COLUMN crawl_metadata.http_headers IS 'HTTP response headers in JSON format';
COMMENT ON COLUMN crawl_metadata.raw_html IS 'Raw HTML content from the crawled page';
COMMENT ON COLUMN crawl_metadata.text_content IS 'Extracted text content';
COMMENT ON COLUMN crawl_metadata.lang IS 'Language code (ISO 639-1)';

COMMENT ON COLUMN event_codes.event_id IS 'Windows Event ID number';
COMMENT ON COLUMN event_codes.provider IS 'Event provider (e.g., Microsoft-Windows-Security-Auditing)';
COMMENT ON COLUMN event_codes.channel IS 'Event channel (e.g., Security)';
COMMENT ON COLUMN event_codes.level IS 'Event level number';
COMMENT ON COLUMN event_codes.level_name IS 'Event level name (e.g., Failure Audit)';

COMMENT ON COLUMN event_descriptions.code_id IS 'Foreign key to event_codes table';
COMMENT ON COLUMN event_descriptions.title IS 'Human-readable title for the event';
COMMENT ON COLUMN event_descriptions.description IS 'Detailed description of the event';
COMMENT ON COLUMN event_descriptions.fix IS 'Recommended fix or resolution steps';
COMMENT ON COLUMN event_descriptions.source_type IS 'Type of source (e.g., official_doc)';

COMMENT ON COLUMN event_logs.record_id IS 'Event log record ID';
COMMENT ON COLUMN event_logs.computer IS 'Computer name where event occurred';
COMMENT ON COLUMN event_logs.user_sid IS 'User Security Identifier';
COMMENT ON COLUMN event_logs.event_data IS 'Event-specific data in JSON format';

COMMENT ON VIEW event_codes_with_descriptions IS 'Consolidated view of event codes with their descriptions';

-- =====================================================
-- 8. Insert initial data (if needed)
-- =====================================================

-- You can add any initial seed data here
-- For example, common event codes or reference data

-- =====================================================
-- Migration completed successfully
-- ===================================================== 
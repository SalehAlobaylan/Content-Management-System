-- Lumen Platform Schema Migration
-- This creates all tables needed for the Lumen social media backend

-- ===========================================
-- Extensions
-- ===========================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

-- ===========================================
-- Content Items (Core content for all feeds)
-- ===========================================
CREATE TABLE IF NOT EXISTS content_items (
    id SERIAL PRIMARY KEY,
    public_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Classification
    type VARCHAR(20) NOT NULL CHECK (type IN ('ARTICLE', 'VIDEO', 'TWEET', 'COMMENT', 'PODCAST')),
    source VARCHAR(20) NOT NULL CHECK (source IN ('RSS', 'PODCAST', 'YOUTUBE', 'UPLOAD', 'MANUAL')),
    status VARCHAR(20) DEFAULT 'READY' CHECK (status IN ('PENDING', 'PROCESSING', 'READY', 'FAILED', 'ARCHIVED')),
    
    -- Content
    title TEXT,
    body_text TEXT,
    excerpt TEXT,
    
    -- Media
    media_url TEXT,
    thumbnail_url TEXT,
    original_url TEXT,
    duration_sec INTEGER,
    
    -- Attribution
    author VARCHAR(255),
    source_name VARCHAR(255),
    source_feed_url TEXT,
    
    -- Tags & AI
    topic_tags TEXT[],
    embedding vector(384),  -- pgvector: 384-dimensional for all-MiniLM-L6-v2
    metadata JSONB,
    
    -- Engagement counters
    like_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    share_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    
    -- Timestamps
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for content_items
CREATE INDEX IF NOT EXISTS idx_content_items_type ON content_items(type);
CREATE INDEX IF NOT EXISTS idx_content_items_status ON content_items(status);
CREATE INDEX IF NOT EXISTS idx_content_items_published ON content_items(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_content_items_public_id ON content_items(public_id);

-- Vector index for similarity search (requires pgvector)
-- Note: ivfflat index requires at least 100 rows, use HNSW for smaller datasets
-- CREATE INDEX IF NOT EXISTS idx_content_items_embedding ON content_items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ===========================================
-- Transcripts (for audio/video content)
-- ===========================================
CREATE TABLE IF NOT EXISTS transcripts (
    id SERIAL PRIMARY KEY,
    public_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Association
    content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE CASCADE,
    
    -- Content
    full_text TEXT NOT NULL,
    summary TEXT,
    word_timestamps JSONB,
    language VARCHAR(10),
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transcripts_content_item ON transcripts(content_item_id);

-- ===========================================
-- User Interactions
-- ===========================================
CREATE TABLE IF NOT EXISTS user_interactions (
    id SERIAL PRIMARY KEY,
    public_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- User identification
    user_id UUID,
    session_id VARCHAR(255),
    
    -- Content reference
    content_item_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE CASCADE,
    
    -- Interaction details
    type VARCHAR(50) NOT NULL CHECK (type IN ('like', 'bookmark', 'share', 'view', 'complete')),
    metadata JSONB,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicate like/bookmark per user per content
    UNIQUE (user_id, content_item_id, type)
);

CREATE INDEX IF NOT EXISTS idx_user_interactions_user ON user_interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_session ON user_interactions(session_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_content ON user_interactions(content_item_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_type ON user_interactions(type);

-- ===========================================
-- Content Sources (for aggregator configuration)
-- ===========================================
CREATE TABLE IF NOT EXISTS content_sources (
    id SERIAL PRIMARY KEY,
    public_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Source identification
    name VARCHAR(255) NOT NULL,
    type VARCHAR(20) NOT NULL CHECK (type IN ('RSS', 'PODCAST', 'YOUTUBE', 'UPLOAD', 'MANUAL')),
    
    -- Configuration
    feed_url TEXT,
    api_config JSONB,
    
    -- Status
    is_active BOOLEAN DEFAULT true,
    fetch_interval_minutes INTEGER DEFAULT 60,
    last_fetched_at TIMESTAMPTZ,
    
    -- Metadata
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_content_sources_type ON content_sources(type);
CREATE INDEX IF NOT EXISTS idx_content_sources_active ON content_sources(is_active);

-- ===========================================
-- Helper function for updated_at trigger
-- ===========================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Auto-update updated_at on content_items
DROP TRIGGER IF EXISTS update_content_items_updated_at ON content_items;
CREATE TRIGGER update_content_items_updated_at
    BEFORE UPDATE ON content_items
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Auto-update updated_at on content_sources
DROP TRIGGER IF EXISTS update_content_sources_updated_at ON content_sources;
CREATE TRIGGER update_content_sources_updated_at
    BEFORE UPDATE ON content_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

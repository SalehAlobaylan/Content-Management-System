# Technical Specification: Lumen

**Version:** 1.0
**Scope:** MVP
**Stack:** Node.js (Aggregator), Golang (CMS/API), PostgreSQL (pgvector), FFmpeg, S3.

---

## 1. System Architecture

### 1.1 High-Level Flow
1.  **Ingestion (Node.js):** Fetches data from RSS, APIs, and User Uploads.
2.  **Processing (Node.js):**
    *   **Media Factory:** Converts Audio (MP3) -> Video (MP4) using FFmpeg.
    *   **AI Engine:** Generates vector embeddings for titles/body text.
3.  **Storage:**
    *   **PostgreSQL:** Stores metadata and Vectors.
    *   **S3:** Stores physical MP4 files and images.
4.  **Serving (Golang):** Serves the API and handles Vector Similarity search.

---

## 2. Database Schema (PostgreSQL)

We use a unified `content_items` table for all types to allow cross-feed querying.

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE content_type AS ENUM ('ARTICLE', 'VIDEO', 'TWEET', 'COMMENT');
CREATE TYPE source_type AS ENUM ('RSS', 'UPLOAD', 'API', 'PODCAST');

CREATE TABLE content_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Content Details
    type content_type NOT NULL,
    source source_type NOT NULL,
    title TEXT,                  -- Headline or Video Title
    body_text TEXT,             -- Article content / Transcript / Tweet text
    
    -- Media (Crucial for For You feed)
    media_url TEXT,             -- S3 URL to MP4 (Video) or Image (Article)
    original_url TEXT,          -- Link to source
    
    -- AI & Ranking
    embedding vector(1536),     -- For Semantic Search (Related items)
    
    -- Metadata (Flexible)
    metadata JSONB,             -- {"author": "...", "duration": 300, "read_time": 5}
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    published_at TIMESTAMP
);

CREATE TABLE user_interests (
    user_id UUID PRIMARY KEY,
    interest_vector vector(1536), -- Moving average of user's liked content
    updated_at TIMESTAMP
);

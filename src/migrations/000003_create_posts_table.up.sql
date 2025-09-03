-- Create posts and post_media matching models.Post and many2many relation
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    public_id UUID DEFAULT gen_random_uuid() UNIQUE,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    author VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
-- Junction table for many-to-many between posts and media
CREATE TABLE IF NOT EXISTS post_media (
    post_id INTEGER NOT NULL,
    media_id INTEGER NOT NULL,
    PRIMARY KEY (post_id, media_id),
    CONSTRAINT fk_post_media_post FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    CONSTRAINT fk_post_media_media FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
);
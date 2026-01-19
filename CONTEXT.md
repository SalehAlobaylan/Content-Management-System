# Content Management System - Project Context

> **Purpose:** This document provides comprehensive context for AI agents and developers to understand the CMS system architecture, data models, API capabilities, and integration patterns.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Tech Stack](#tech-stack)
3. [Project Structure](#project-structure)
4. [Data Models](#data-models)
5. [API Reference](#api-reference)
6. [Query System](#query-system)
7. [Response Format](#response-format)
8. [Client Integration Guide](#client-integration-guide)
9. [Environment Setup](#environment-setup)
10. [Future Roadmap (v1.2.0)](#future-roadmap-v120)

---

## Project Overview

A RESTful Content Management System API built with Go, providing comprehensive CRUD operations for managing **Pages**, **Posts**, and **Media** content. The system features advanced query capabilities including filtering, sorting, searching, and pagination.

### Key Features

- **RESTful API** with versioned endpoints (`/api/v1`)
- **Advanced Query System** with filtering, sorting, search, and pagination
- **Many-to-Many Relationships** between Posts and Media
- **UUID-based Public IDs** for secure resource identification
- **Swagger/OpenAPI Documentation** with interactive UI
- **Docker Support** for containerized deployment
- **CORS Enabled** for cross-origin requests

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Language** | Go 1.24+ |
| **Framework** | Gin (HTTP web framework) |
| **Database** | PostgreSQL |
| **ORM** | GORM |
| **UUID** | google/uuid |
| **Container** | Docker & Docker Compose |
| **Documentation** | OpenAPI 3.0 / Swagger UI |

### Dependencies

```go
github.com/gin-gonic/gin       // HTTP framework
github.com/gin-contrib/cors    // CORS middleware
gorm.io/gorm                   // ORM
gorm.io/driver/postgres        // PostgreSQL driver
github.com/google/uuid         // UUID generation
github.com/joho/godotenv       // Environment variables
```

---

## Project Structure

```
Content-Management-System/
├── src/
│   ├── main.go                    # Application entry point
│   ├── controllers/               # Request handlers
│   │   ├── pageController.go
│   │   ├── postController.go
│   │   └── mediaController.go
│   ├── models/                    # Data models (GORM)
│   │   ├── page.go
│   │   ├── post.go
│   │   └── media.go
│   ├── routes/                    # Route definitions
│   │   ├── pageRoutes.go
│   │   ├── postRoutes.go
│   │   └── mediaRoutes.go
│   ├── utils/                     # Utilities
│   │   ├── database.go            # DB connection
│   │   ├── queryParser.go         # Query parameter parsing
│   │   ├── queryBuilder.go        # GORM query builder
│   │   └── response.go            # Response structures
│   ├── migrations/                # SQL migrations
│   └── tests/                     # Unit & integration tests
├── docs/                          # API documentation
│   ├── openapi.yaml               # OpenAPI specification
│   └── index.html                 # Swagger UI
├── cmd/docserver/                 # Documentation server
├── docker-compose.yaml            # Docker orchestration
├── Dockerfile                     # Container definition
└── go.mod                         # Go modules
```

---

## Data Models

### Page

Static content pages for the CMS.

```json
{
  "id": "uuid",           // Public UUID identifier
  "title": "string",      // Required, max 255 chars
  "content": "string",    // Required, text content
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

**Go Model:**
```go
type Page struct {
    ID        uint      `gorm:"primaryKey" json:"-"`
    PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
    Title     string    `gorm:"size:255;not null" json:"title" binding:"required"`
    Content   string    `gorm:"type:text;not null" json:"content" binding:"required"`
    CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
    UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}
```

---

### Post

Blog posts with author attribution and media attachments.

```json
{
  "id": "uuid",           // Public UUID identifier
  "title": "string",      // Required, max 255 chars
  "content": "string",    // Required, text content
  "author": "string",     // Required, max 255 chars
  "media": [],            // Array of associated Media objects
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

**Go Model:**
```go
type Post struct {
    ID        uint      `gorm:"primaryKey" json:"-"`
    PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
    Title     string    `gorm:"size:255;not null" json:"title" binding:"required"`
    Content   string    `gorm:"type:text;not null" json:"content" binding:"required"`
    Author    string    `gorm:"size:255;not null" json:"author" binding:"required"`
    CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
    UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
    Media     []Media   `gorm:"many2many:post_media" json:"media,omitempty"`
}
```

---

### Media

Media assets (images, videos, files) that can be attached to posts.

```json
{
  "id": "uuid",           // Public UUID identifier
  "url": "string",        // Required, URL to media resource
  "type": "string",       // Optional, media type (image, video, etc.)
  "posts": [],            // Array of associated Post objects
  "created_at": "datetime",
  "updated_at": "datetime"
}
```

**Go Model:**
```go
type Media struct {
    ID        uint      `gorm:"primaryKey" json:"-"`
    PublicID  uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();uniqueIndex" json:"id"`
    URL       string    `gorm:"size:255;not null" json:"url" binding:"required"`
    Type      string    `gorm:"size:50" json:"type"`
    CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
    UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
    Post      []Post    `gorm:"many2many:post_media" json:"posts,omitempty"`
}
```

---

### Relationships

```
┌─────────┐       many-to-many       ┌─────────┐
│  Post   │◄────────────────────────►│  Media  │
└─────────┘       (post_media)       └─────────┘
```

- A **Post** can have multiple **Media** items
- A **Media** item can belong to multiple **Posts**
- Junction table: `post_media`

---

## API Reference

**Base URL:** `http://localhost:8080`  
**API Version:** `/api/v1`

### Root Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Welcome message with available endpoints |
| GET | `/health` | Health check |
| GET | `/docs` | Swagger UI documentation |

---

### Pages API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/pages` | Create a new page |
| GET | `/api/v1/pages` | List all pages (with query params) |
| GET | `/api/v1/pages/:id` | Get a page by ID |
| PUT | `/api/v1/pages/:id` | Update a page |
| DELETE | `/api/v1/pages/:id` | Delete a page |

**Create/Update Request Body:**
```json
{
  "title": "Page Title",
  "content": "Page content goes here..."
}
```

**Sortable Fields:** `created_at`, `updated_at`, `title`  
**Filterable Fields:** `title`, `created_at`, `updated_at`, `id`  
**Searchable Fields:** `title`, `content`

---

### Posts API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/posts` | Create a new post |
| GET | `/api/v1/posts` | List all posts (with query params) |
| GET | `/api/v1/posts/:id` | Get a post by ID |
| PUT | `/api/v1/posts/:id` | Update a post |
| DELETE | `/api/v1/posts/:id` | Delete a post |

**Create/Update Request Body:**
```json
{
  "title": "Post Title",
  "content": "Post content goes here...",
  "author": "Author Name",
  "media": [
    { "id": "uuid-of-existing-media" }
  ]
}
```

**Sortable Fields:** `created_at`, `updated_at`, `title`, `author`  
**Filterable Fields:** `title`, `author`, `content`, `created_at`, `updated_at`, `id`  
**Searchable Fields:** `title`, `content`, `author`

---

### Media API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/media` | Create a new media item |
| GET | `/api/v1/media` | List all media (with query params) |
| GET | `/api/v1/media/:id` | Get a media item by ID |
| DELETE | `/api/v1/media/:id` | Delete a media item |

**Create Request Body:**
```json
{
  "url": "https://example.com/image.jpg",
  "type": "image"
}
```

**Sortable Fields:** `created_at`, `updated_at`, `type`  
**Filterable Fields:** `type`, `url`, `id`, `created_at`, `updated_at`  
**Searchable Fields:** `url`, `type`

---

## Query System

The API supports advanced querying through URL parameters.

### Pagination

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `page` | int | 1 | - | Page number (1-indexed) |
| `limit` | int | 20 | 100 | Items per page |
| `offset` | int | 0 | - | Skip N items (alternative to page) |

**Examples:**
```
GET /api/v1/posts?page=2&limit=10
GET /api/v1/posts?offset=20&limit=10
```

---

### Sorting

| Parameter | Description |
|-----------|-------------|
| `sort` | Comma-separated field names |
| `order` | Comma-separated directions (`asc`, `desc`) |

**Examples:**
```
GET /api/v1/posts?sort=created_at&order=desc
GET /api/v1/posts?sort=author,created_at&order=asc,desc
```

**Default Sort:** `created_at DESC`

---

### Filtering

Filter syntax: `field[operator]=value`

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equals | `title[eq]=Hello` |
| `ne` | Not equals | `author[ne]=Admin` |
| `gt` | Greater than | `created_at[gt]=2024-01-01` |
| `gte` | Greater than or equal | `created_at[gte]=2024-01-01` |
| `lt` | Less than | `created_at[lt]=2024-12-31` |
| `lte` | Less than or equal | `created_at[lte]=2024-12-31` |
| `in` | In list | `type[in]=image,video` |
| `nin` | Not in list | `type[nin]=audio` |
| `contains` | Contains substring | `title[contains]=news` |
| `starts` | Starts with | `title[starts]=Breaking` |
| `ends` | Ends with | `url[ends]=.jpg` |
| `null` | Is null | `type[null]=true` |
| `notnull` | Is not null | `type[notnull]=true` |

**Examples:**
```
GET /api/v1/posts?author[eq]=John
GET /api/v1/posts?created_at[gte]=2024-01-01&created_at[lte]=2024-12-31
GET /api/v1/media?type[in]=image,video
GET /api/v1/posts?title[contains]=tutorial
```

---

### Full-Text Search

| Parameter | Description |
|-----------|-------------|
| `search` | Search term |
| `search_fields` | Comma-separated fields to search |

**Examples:**
```
GET /api/v1/posts?search=golang
GET /api/v1/posts?search=tutorial&search_fields=title,content
```

---

### Combined Query Example

```
GET /api/v1/posts?page=1&limit=10&sort=created_at&order=desc&author[eq]=John&search=golang
```

---

## Response Format

### Success Response

```json
{
  "code": 200,
  "message": "Posts fetched successfully",
  "data": [ /* array or object */ ],
  "meta": {
    "page": 1,
    "limit": 20,
    "offset": 0,
    "total": 100,
    "total_pages": 5,
    "has_next": true,
    "has_prev": false
  },
  "links": {
    "self": "/api/v1/posts?page=1",
    "next": "/api/v1/posts?page=2",
    "prev": null,
    "first": "/api/v1/posts?page=1",
    "last": "/api/v1/posts?page=5"
  }
}
```

### Error Response

```json
{
  "code": 400,
  "message": "Invalid request body: Key: 'Post.Title' Error:Field validation for 'Title' failed",
  "data": null
}
```

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success (GET, PUT, DELETE) |
| 201 | Created (POST) |
| 400 | Bad Request (validation error) |
| 404 | Not Found |
| 500 | Internal Server Error |

---

## Client Integration Guide

### JavaScript/TypeScript Example

```typescript
const API_BASE = 'http://localhost:8080/api/v1';

// Fetch posts with pagination and filtering
async function getPosts(options?: {
  page?: number;
  limit?: number;
  author?: string;
  search?: string;
}) {
  const params = new URLSearchParams();
  if (options?.page) params.set('page', String(options.page));
  if (options?.limit) params.set('limit', String(options.limit));
  if (options?.author) params.set('author[eq]', options.author);
  if (options?.search) params.set('search', options.search);
  
  const response = await fetch(`${API_BASE}/posts?${params}`);
  return response.json();
}

// Create a new post
async function createPost(post: {
  title: string;
  content: string;
  author: string;
  media?: { id: string }[];
}) {
  const response = await fetch(`${API_BASE}/posts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(post),
  });
  return response.json();
}

// Update a post
async function updatePost(id: string, post: {
  title: string;
  content: string;
  author: string;
}) {
  const response = await fetch(`${API_BASE}/posts/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(post),
  });
  return response.json();
}

// Delete a post
async function deletePost(id: string) {
  const response = await fetch(`${API_BASE}/posts/${id}`, {
    method: 'DELETE',
  });
  return response.json();
}
```

### React Hook Example

```typescript
import { useState, useEffect } from 'react';

interface Post {
  id: string;
  title: string;
  content: string;
  author: string;
  media: Media[];
  created_at: string;
  updated_at: string;
}

interface Media {
  id: string;
  url: string;
  type: string;
}

interface PaginationMeta {
  page: number;
  limit: number;
  total: number;
  total_pages: number;
  has_next: boolean;
  has_prev: boolean;
}

function usePosts(page = 1, limit = 20) {
  const [posts, setPosts] = useState<Post[]>([]);
  const [meta, setMeta] = useState<PaginationMeta | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetch(`http://localhost:8080/api/v1/posts?page=${page}&limit=${limit}`)
      .then(res => res.json())
      .then(data => {
        setPosts(data.data);
        setMeta(data.meta);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [page, limit]);

  return { posts, meta, loading, error };
}
```

### cURL Examples

```bash
# Get all posts
curl http://localhost:8080/api/v1/posts

# Get posts with pagination
curl "http://localhost:8080/api/v1/posts?page=1&limit=10"

# Search posts
curl "http://localhost:8080/api/v1/posts?search=golang"

# Filter by author
curl "http://localhost:8080/api/v1/posts?author[eq]=John"

# Create a post
curl -X POST http://localhost:8080/api/v1/posts \
  -H "Content-Type: application/json" \
  -d '{"title":"My Post","content":"Hello World","author":"John"}'

# Create a post with media
curl -X POST http://localhost:8080/api/v1/posts \
  -H "Content-Type: application/json" \
  -d '{
    "title":"Post with Image",
    "content":"Check out this image",
    "author":"John",
    "media":[{"id":"existing-media-uuid"}]
  }'

# Update a post
curl -X PUT http://localhost:8080/api/v1/posts/{uuid} \
  -H "Content-Type: application/json" \
  -d '{"title":"Updated Title","content":"Updated content","author":"John"}'

# Delete a post
curl -X DELETE http://localhost:8080/api/v1/posts/{uuid}
```

---

## Environment Setup

### Environment Variables

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=yourpassword
DB_NAME=cms_database
DB_SSLMODE=disable

# Environment (development, production)
ENV=development
```

### Running Locally

```bash
# Install dependencies
go mod download

# Run the application
go run src/main.go

# Run tests
go test -v ./src/tests/integration
go test -v ./src/tests/unit
```

### Running with Docker

```bash
# Build and run with Docker Compose
docker-compose up --build

# Or build manually
docker build -t cms-api .
docker run -p 8080:8080 cms-api
```

### API Documentation Server

```bash
# Start Swagger UI server
go run ./cmd/docserver

# Access at http://localhost:8090
```

---

## Future Roadmap (v1.2.0)

Planned features for version 1.2.0:

### 1. JWT Authentication with Supabase Auth
- OAuth support (Google, GitHub, etc.)
- User sessions and profile management
- Role-based access control

### 2. Redis Integration
- Response caching
- Session storage
- Rate limiting state

### 3. Rate Limiting
- Request throttling per IP/user
- Configurable limits per endpoint

### 4. GraphQL API
- Flexible data querying
- Reduced over-fetching
- Real-time subscriptions

### 5. Enhanced User Management
- User association with content
- Audit trails
- Content ownership

---

## Additional Resources

- **OpenAPI Spec:** [docs/openapi.yaml](docs/openapi.yaml)
- **Swagger UI:** `http://localhost:8090` (run `go run ./cmd/docserver`)
- **Go Documentation:** Run `cd src && go doc -all`

---

## Notes for AI Agents

When building client integrations:

1. **All IDs are UUIDs** - Use string type for IDs in client code
2. **Pagination is 1-indexed** - First page is `page=1`
3. **Content-Type must be `application/json`** for POST/PUT requests
4. **CORS is enabled** - Cross-origin requests are allowed
5. **Filter operators use bracket notation** - e.g., `field[operator]=value`
6. **Media association requires existing media UUIDs** - Create media first, then reference in posts
7. **Responses always include `code` and `message`** - Check these for error handling
8. **List endpoints include pagination metadata** - Use `meta.has_next` for infinite scroll

---

*Last Updated: January 2026*  
*Current Version: 1.0.0*


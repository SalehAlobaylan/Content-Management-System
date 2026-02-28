# Content-Management-System

## Tech Stack

<p align="left">
  <img src="https://skillicons.dev/icons?i=go" alt="Go" height="50" />
  <img src="https://skillicons.dev/icons?i=postgres" alt="PostgreSQL" height="50" />
  <img src="https://skillicons.dev/icons?i=docker" alt="Docker" height="50" />
</p>


# Install dependencies and run

    go mod download
    go run src/main.go
 
 - Go doc (terminal):
 ```
   - View all exported APIs: `cd src; go doc -all`
   - View a package: `go doc ./src/models`
   - View a symbol: `go doc ./src/models Post`
 
Server runs on `http://localhost:8080`
 ```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```
# Database
DATABASE_URL=postgres://user:password@localhost:5432/dbname?sslmode=disable

# Auth (required for Platform Console)
JWT_SECRET=your_secure_secret_here
JWT_EXPIRATION_HOURS=24

# Admin User (dev only)
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=ChangeMe123!
ADMIN_ROLE=admin
```

## API Endpoints

### Public Endpoints

- `GET /` - Welcome message and endpoint list
- `GET /health` - Health check
- `GET /docs` - OpenAPI documentation

### Platform Endpoints (`/api/v1`)

- `GET /api/v1/feed/foryou` - For You feed
- `GET /api/v1/feed/news` - News feed
- `GET /api/v1/feed/rss.xml` - RSS 2.0 output feed (`type`, `topic`, `limit`)
- `GET /api/v1/content/:id` - Content item details
- `GET /api/v1/interactions` - User interactions
- `GET /api/v1/interactions/bookmarks` - Bookmarked content
- `GET /api/v1/posts` - Posts CRUD
- `GET /api/v1/media` - Media CRUD
- `GET /api/v1/pages` - Pages CRUD

### Admin Endpoints

- `POST /admin/login` - Admin login (issues JWT)
- `GET /admin/me` - Get current admin user (requires JWT)
- `GET /admin/users` - List admin users
- `POST /admin/users` - Create admin user
- `GET /admin/users/:id` - Get admin user
- `PUT /admin/users/:id` - Update admin user
- `DELETE /admin/users/:id` - Delete admin user
- `POST /admin/users/:id/password` - Reset admin password
- `GET /admin/sources` - List content sources
- `POST /admin/sources` - Create content source
- `POST /admin/sources/bulk` - Bulk create content sources (OPML import flow)
- `POST /admin/sources/discover` - Discover feed URLs via Aggregation
- `POST /admin/sources/preview` - Preview source ingestion via Aggregation
- `GET /admin/sources/:id` - Get content source
- `PUT /admin/sources/:id` - Update content source
- `DELETE /admin/sources/:id` - Delete content source
- `POST /admin/sources/:id/run` - Trigger source ingestion
- `GET /admin/content` - List ingested content
- `GET /admin/content/:id` - Get content details
- `PATCH /admin/content/:id/status` - Update content status (moderation/archive)

## Testing

```
go test -v ./src/tests/integration
```

To run only admin auth tests:
```
go test -v ./src/tests/integration -run Admin
```
 
 - Swagger/OpenAPI UI:

   - Spec file: `docs/openapi.yaml`
   - Serve static docs locally: `go run ./cmd/docserver`
   - Open in browser: `http://localhost:8090`
   - Swagger UI: `go run ./cmd/docserver` → `http://localhost:8090`
   - Go docs: `cd src; go doc -all`

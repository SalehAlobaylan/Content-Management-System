# Content-Management-System

The system of record and read API for the Wahb platform. CMS owns content CRUD, the PostgreSQL + pgvector store, feed assembly (For You + News), media atomization workflow state, interactions, syndication output (RSS/Atom/JSON), and the full admin/intelligence/storage surface for Platform-Console.

It does **not** scrape sources, run FFmpeg, run ML models, or orchestrate the ingest pipeline — those belong to Aggregation, Enrichment, and Media. CMS calls Enrichment on demand and receives ingested content from Aggregation via `/internal/*`.

**Port:** 8080 · **Production:** https://cms.salehspace.dev · **Stack:** Go 1.24, Gin, GORM, PostgreSQL 15+ with pgvector

> Full architecture, feature, and data-model reference: [`../docs/content-management-system.md`](../docs/content-management-system.md). Product intent: [`../docs/PRD.md`](../docs/PRD.md).

## Tech Stack

<p align="left">
  <img src="https://skillicons.dev/icons?i=go" alt="Go" height="50" />
  <img src="https://skillicons.dev/icons?i=postgres" alt="PostgreSQL" height="50" />
  <img src="https://skillicons.dev/icons?i=docker" alt="Docker" height="50" />
</p>

- **Language:** Go 1.24 (Gin HTTP, GORM ORM)
- **Database:** PostgreSQL 15+ with the `pgvector` extension
- **Vectors:** dense `vector(1024)` text embeddings (`Qwen/Qwen3-Embedding-0.6B`, set by Enrichment), `vector(512)` CLIP image embeddings (set by Media). A legacy `sparsevec(250002)` column from the BGE-M3 era still exists but is dead/unused — semantic similarity is dense cosine only.
- **Auth:** JWT HS256 — **issued by IAM, only validated here** (shared secret). Service-to-service `/internal` calls use a static bearer token.

## Quick Start

```bash
# Install deps and run (dev)
go mod download
go run src/main.go          # serves on http://localhost:8080

# Build
go build ./...

# Docker
docker build -t wahb-cms .
```

In `development` the server can auto-migrate the schema on boot with GORM `AutoMigrate`. `./start.sh` defaults CMS to `AUTO_MIGRATE=false` so the full local stack does not spend minutes introspecting a remote Supabase database. GORM AutoMigrate also does not execute SQL files from `migrations/`.

Apply canonical CMS SQL migrations explicitly:

```bash
go run ./cmd/migrate --status
go run ./cmd/migrate 20260627000000_media_atomization.sql 20260627010000_media_atomization_operations.sql
```

Use `go run ./cmd/migrate --all` only when the target database has a reliable `cms_schema_migrations` ledger or is a fresh database. Production never auto-migrates.

### Go API docs (terminal)

```bash
cd src
go doc -all              # all exported APIs
go doc ./src/models      # a package
go doc ./src/models ContentItem
```

## Configuration

Copy `.env.example` to `.env` and fill in the values. `DATABASE_URL` is the only supported DB config method.

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `DATABASE_URL` | **yes** | — | PostgreSQL DSN (`postgres://…?sslmode=disable`). Use the connection **pooler** (port 6543) on Supabase. |
| `JWT_SECRET` | **yes** | — | Shared HS256 secret — must match IAM. Boot fails if unset. |
| `PORT` | no | 8080 | HTTP port |
| `ENV` | no | development | `development`/`production` (gates auto-migrate) |
| `PUBLIC_BASE_URL` | no | request host | Absolute base for syndication (RSS/Atom/JSON) links |
| `JWT_EXPIRATION_HOURS` | no | 24 | Token lifetime (dev admin seed) |
| `JWT_ISSUER` | no | cms-service | Issuer claim |
| `JWT_AUDIENCE` | no | platform-console | Audience claim |
| `JWT_ALLOWED_ISSUERS` | no | `cms-service,iam-authorization-service` | Accepted token issuers. Empty-issuer tokens are rejected (`iss` must be listed) |
| `JWT_ALLOWED_AUDIENCES` | no | — (disabled) | Optional `aud` allowlist (comma-separated); when unset, audience checks are skipped |
| `JWT_REQUIRE_TENANT_ID` | no | false | Enforce tenant claim |
| `DEFAULT_TENANT_ID` | no | default | Fallback tenant |
| `ADMIN_EMAIL` / `ADMIN_PASSWORD` / `ADMIN_ROLE` | no | — | Seed a dev admin user |
| `CMS_SERVICE_TOKEN` | **yes** | — | Bearer token Aggregation/Media/Enrichment use for `/internal/*` |
| `ENRICHMENT_BASE_URL` | no | http://localhost:5050 | On-demand embed/translate/rerank/news-slide |
| `ENRICHMENT_SERVICE_TOKEN` | no | falls back to `CMS_SERVICE_TOKEN` | Auth for Enrichment calls |
| `REDIS_URL` | no | redis://localhost:6379 | Declared; caching is future-use |
| `AUTO_MIGRATE` | no | (unset = migrate in dev) | Set `false` to skip GORM AutoMigrate on boot. *(Not in `.env.example`.)* |

## Authentication

CMS **does not log anyone in** — IAM issues JWTs (HS256). Platform-Console and Wahb-Platform attach `Authorization: Bearer <token>`; CMS validates the signature and issuer (`JWT_ALLOWED_ISSUERS`; empty issuers rejected) and optionally the audience (`JWT_ALLOWED_AUDIENCES`). There is no `/admin/login` route on CMS.

- **Admin routes** (`/admin/*`) — authenticate with a valid IAM JWT (`AdminAuthMiddleware`), then **authorize per route via per-permission RBAC** (`RequireAdminPermission(resource, action)` in `src/utils/admin_authz_middleware.go`). Authorization reads the token's flattened `permissions` claim — the `admin` role bypasses, `resource:*`/`*:*` wildcards are honored, and a plain `user` token gets **403**. `manager`/`editor`/`agent` get exactly their seeded scope. `POST /admin/restart` is `admin`-role-only (`RequireAdminRole`). Mapping: sources/discovery→`source:*`, content/topics/enrichment/quality/transcription/studio/flags/analytics→`content:*`, feeds/intelligence-modes/ranking/circulation→`feed:*`, storage→`aggregation:*`, audit→`iam:*`.
- **User routes** (`/api/v1/content/mine`, `/content/submit`, `/content/:id/request-restore`, transcribe, interactions) — require a user JWT (`UserAuthMiddleware`); some accept an optional session via `OptionalUserAuthMiddleware`.
- **Internal routes** (`/internal/*`) — static service token (`CMS_SERVICE_TOKEN`), not user JWTs.

## API Surface

### Public — Platform feeds & content (`/api/v1`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/feed/foryou` | For You feed (VIDEO + PODCAST feed units with playback metadata, optional duration preference, cursor-paginated) |
| GET | `/feed/news` | News feed — story-slides (1 featured + up to 3 related) |
| GET | `/feed/rss.xml` · `/feed/atom.xml` · `/feed/feed.json` | Syndication output (`type`, `topic`, `limit`) |
| GET | `/feed/saved/:slug` | A saved named feed |
| GET | `/content/:id` | Single content item (optional session for interaction flags) |
| GET | `/content/:id/comments` | Comments for an item |
| GET | `/content/mine` · POST `/content/submit` | User-generated content (user JWT) |
| POST | `/content/:id/transcribe` | Request transcription (user JWT) |
| GET | `/transcripts/:id` | Fetch a transcript |
| POST/GET/DELETE | `/interactions`, `/interactions/bookmarks`, `/interactions/history`, `/interactions/:id` | Like / bookmark / share / view / complete + history |
| GET/POST/PUT/DELETE | `/posts`, `/media`, `/pages` | Legacy CMS content CRUD (admin-gated writes) |

### Admin (`/admin/*`, IAM JWT) — for Platform-Console

Grouped capabilities (see the full route list in [`../docs/content-management-system.md`](../docs/content-management-system.md)):

- **Sources & discovery** — source CRUD, bulk/OPML import, `discover`/`preview`/`:id/run`; Feeds-Finding discovery profiles, suggestions (approve/reject/bulk), config, sweep-now, graph build + authorities.
- **Content moderation** — list/filter, status updates, bulk delete/status/tags/topic, stats, status-counts, topics.
- **Topics** — rename, delete, merge, reclassify, recluster, label-batch.
- **Intelligence** — ranking config + modes, content flags (boost/suppress/pin/exclude), embeddings explorer (clusters/similar/stats), feed analytics (score-distribution, velocity, trending, source-performance, signal-health), feed preview (foryou/news with score breakdown), news-snapshot refresh.
- **Media Studio, transcription, atomization** — per-item transcript/chapter editor, transcription config + jobs/batches, Media Atomization overview/pipeline/parents/chapters/runs/review/repair, quality.
- **Enrichment** — stats, missing, trigger (single/batch/all), bulk-status, health.
- **Storage** — stats, candidates, purge, restore, policy + overrides, sweep runs/preview, reconcile, operations.
- **Quality** — profiles CRUD, resolve, probe-item.
- **Audit & ops** — audit log read/write, `restart`.

### Internal (`/internal/*`, service token) — for Aggregation / Enrichment / Media

Content write-back pipeline (`POST /content-items` → PATCH `…/artifacts` → POST `/transcripts` → PATCH `…/transcript` → PATCH `…/embedding` → PATCH `…/status`), image-embedding + STT hooks, vector retrieval (`/content-items/knn`, `…/embeddings`, `…/missing-embedding`, `batch-text`), discovery/intel candidate exchange, media atomization candidate/input/plan/children/run endpoints, and storage/quality policy endpoints.

## Media Atomization Rules

CMS is the source of truth for atomization policy, workflow state, review state, repair, and For You visibility.

- Atomization candidates must be parent media longer than 2400 seconds (>40m). Parent media at or under 40m normally remains a raw feed unit when otherwise eligible.
- Visible For You media units must be 270-2400 seconds. 4:30-4:59 is valid and belongs to the `5m` bucket; anything below 4:30 must merge or stay hidden/review-only.
- Child chapters are first-class `content_items` linked to their parent. `chapters` remains the editorial/review marker table.
- For You returns playback metadata (`playback_url`, `playback_type`, fallback/renditions), not an MP4-only contract.
- Re-atomization must archive/replace prior child feed units so duplicates cannot remain visible.

## Testing

```bash
go test ./...                                  # all
go test -v ./src/tests/integration             # integration suite
go test -v ./src/tests/integration -run Admin  # admin/auth only
```

## Project Layout

```
src/
  main.go        # route wiring, boot, auto-migrate
  routes/        # route groups (feed, content, interaction, admin, internal, …)
  controllers/   # handlers
  models/        # GORM structs (ContentItem, Topic, Transcript, RankingConfig, …)
  utils/         # DB, JWT/auth middleware, migration helpers
  tests/         # integration tests
migrations/      # tracked SQL schema changes
```

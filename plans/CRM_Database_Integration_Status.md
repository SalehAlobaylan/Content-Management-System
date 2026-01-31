# CRM Service Database Integration - Status Summary

**Date:** January 31, 2026
**Purpose:** Inform CMS team about CRM service database integration with shared `turfa_platform` database

---

## Executive Summary

The CRM Service has been successfully integrated with the shared `turfa_platform` database. This document outlines what was done, the current configuration, and what the CMS team needs to coordinate on.

---

## 1) Integration Completed

### 1.1 Configuration Updates

The following configuration files have been updated to use the shared database:

**`.env.example`** - Updated with:
- `DATABASE_URL` pointing to `turfa_platform` database (not `crm_db`)
- `JWT_SECRET` notes that it must match CMS service
- Comments clarifying shared nature of database and JWT

**`docker-compose.yml`** - Updated with:
- PostgreSQL service named `turfa-platform-postgres`
- Database name changed from `crm_db` to `turfa_platform`
- Network renamed to `turfa-platform-network`
- Migration runner updated to use `turfa_platform` database

### 1.2 Migration Tools Added

**New files created:**
- `Makefile` - Build and migration commands
- `scripts/migrate.sh` - Executable migration script with colored output
- `README.md` - Comprehensive documentation including database integration guide

### 1.3 Code Changes

**`cmd/server/main.go`** - Modified:
- Removed GORM AutoMigrate call (tables created via golang-migrate)
- Kept pipeline stages seeding (idempotent, safe to run)

**Why:** AutoMigrate was causing conflicts with existing tables created via SQL migrations. The service now relies solely on golang-migrate for schema management.

---

## 2) Database Schema Status

### 2.1 CRM Tables

All CRM tables have been created in the shared `turfa_platform` database:

| Table | Primary Key | Soft Delete | Purpose |
|-------|-------------|--------------|----------|
| `customers` | SERIAL | Yes | Primary identity for clients/leads |
| `contacts` | SERIAL | Yes | Individual people nested under customers |
| `pipeline_stages` | SERIAL | Yes | Configurable sales funnel stages |
| `deals` | SERIAL | Yes | Sales opportunities |
| `activities` | SERIAL | Yes | Tasks, calls, meetings |
| `notes` | SERIAL | Yes | Internal comments and logs |
| `tags` | SERIAL | Yes | Metadata labels for categorization |
| `customer_tags` | (composite) | No | Junction table (many-to-many) |
| `audit_logs` | SERIAL | No | Immutable change log |

### 2.2 Conflict Verification

✅ **Zero table name conflicts** between CMS and CRM schemas

**CMS Tables:**
- `blogs`, `categories`, `content_items`, `content_sources`, `media`, `pages`, `posts`, `transcripts`, `user_interactions`, `visitors`

**CRM Tables:**
- `customers`, `contacts`, `pipeline_stages`, `deals`, `activities`, `notes`, `tags`, `customer_tags`, `audit_logs`

All table names are unique across services.

---

## 3) Authentication & Authorization

### 3.1 Shared JWT Model

The CRM Service implements **verifier-only** JWT authentication:

- **CMS is the issuer** - creates and signs JWT tokens
- **CRM is verifier only** - validates tokens and enforces RBAC
- **Shared JWT_SECRET** - Both services must use identical secret

### 3.2 Required JWT Claims

For CRM to accept CMS-issued tokens, they must include:

```json
{
  "sub": "user-id-or-email",
  "user_id": 123,
  "role": "admin|manager|agent",
  "email": "user@example.com",
  "name": "User Name",
  "exp": 1738330800
}
```

**Required fields:**
- `role` - Must be one of: `admin`, `manager`, `agent`
- `exp` - Expiration timestamp
- Either `sub` or `user_id` - User identification

### 3.3 CMS Service Requirements

The CMS service MUST:

1. **Issue JWTs** with the signing method `HS256` (HMAC)
2. **Use the same JWT_SECRET** as CRM service (from environment variable)
3. **Set issuer claim** to `cms` (`iss: "cms"`)
4. **Include required claims** in all tokens

### 3.4 Authorization Header Format

All requests to CRM admin endpoints must include:

```
Authorization: Bearer <jwt-token>
```

---

## 4) Service Status

### 4.1 Current State

✅ **CRM Service is operational**
- Running on port 3000
- Connected to shared `turfa_platform` database
- All endpoints registered and functional
- Pipeline stages seeded (idempotent)

### 4.2 Available Endpoints

**Public (no auth):**
- `GET /health` - Health check
- `GET /ready` - Readiness probe
- `GET /metrics` - Prometheus metrics

**Admin (JWT required):**
- `GET /admin/me` - Current user info
- `GET /admin/me/activities` - User's activities
- `GET /admin/customers` - List customers
- `POST /admin/customers` - Create customer
- `GET /admin/customers/:id` - Get customer details
- `PUT/PATCH/DELETE /admin/customers/:id` - Update/delete customer
- `/admin/contacts/*` - Contact CRUD
- `/admin/deals/*` - Deal CRUD
- `/admin/activities/*` - Activity CRUD
- `/admin/tags/*` - Tag CRUD
- `GET /admin/reports/overview` - Overview report

---

## 5) Environment Configuration

### 5.1 Required Environment Variables

CRM Service requires:

```env
DATABASE_URL=postgres://user:password@host:5432/turfa_platform?sslmode=mode
JWT_SECRET=<same-as-cms>
JWT_ISSUER=cms
CORS_ALLOWED_ORIGINS=<console-origins>
```

### 5.2 CORS Configuration

CRM allows cross-origin requests from:
- `http://localhost:3000`
- `http://localhost:3001`
- Production Console URL (to be configured)

---

## 6) Migration Workflow

### 6.1 Running Migrations

For local development:

```bash
# Option 1: Using migration script
./scripts/migrate.sh up

# Option 2: Using Makefile
make migrate-up

# Option 3: Direct CLI
migrate -path ./migrations -database "${DATABASE_URL}" up
```

For Docker:

```bash
docker-compose --profile migrate up migrate
```

### 6.2 Migration Files

Located in `migrations/` directory:
- `000001_init_schema.up.sql` - Creates all CRM tables and indexes
- `000001_init_schema.down.sql` - Rolls back all changes

### 6.3 Future Migrations

When adding new database changes:
1. Create new migration: `./scripts/migrate.sh create <name>`
2. Apply up migration in development
3. Test with `./scripts/migrate.sh down` to verify rollback
4. Commit both `.up.sql` and `.down.sql` files

---

## 7) CMS Team Action Items

### 7.1 Required Coordination

1. **Verify CMS DATABASE_URL** points to `turfa_platform` database
   ```env
   DATABASE_URL=postgres://.../turfa_platform?sslmode=...
   ```

2. **Ensure JWT_SECRET matches** between CMS and CRM services
   ```env
   JWT_SECRET=<same-secret-in-both-services>
   ```

3. **Configure CORS** in CMS to allow CRM origins (if any direct calls)

### 7.2 JWT Token Issuance

CMS must issue tokens with:

```go
// Example Go code for token generation
token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
    "sub": user.Email,
    "user_id": user.ID,
    "role": user.Role,  // admin, manager, or agent
    "email": user.Email,
    "name": user.Name,
    "exp": time.Now().Add(24 * time.Hour).Unix(),
    "iss": "cms",
})

tokenString, err := token.SignedString([]byte(jwtSecret))
```

### 7.3 Testing Integration

After CMS is configured:

```bash
# 1. Get JWT from CMS login endpoint
TOKEN=$(curl -X POST https://cms.yourdomain.com/login -d '...' | jq -r '.token')

# 2. Test CRM accepts the token
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/admin/me
```

---

## 8) Production Deployment Considerations

### 8.1 Database Connection

In production, ensure:
- `sslmode=require` or `sslmode=verify-ca`
- Connection pooling is configured
- Database host is accessible from CRM service

### 8.2 Security

- Rotate `JWT_SECRET` periodically (requires coordination)
- Use strong, randomly generated secret (32+ characters)
- Configure `CORS_ALLOWED_ORIGINS` to only trusted domains
- Enable firewall rules to restrict database access

### 8.3 Monitoring

CRM provides:
- `/health` endpoint for liveness checks
- `/ready` endpoint for readiness checks
- `/metrics` endpoint for Prometheus metrics

---

## 9) Troubleshooting

### 9.1 Common Issues

**Problem:** 401 Unauthorized
- **Solution:** Verify `JWT_SECRET` matches between CMS and CRM

**Problem:** 403 Forbidden
- **Solution:** Ensure JWT includes valid `role` claim (admin/manager/agent)

**Problem:** Database connection failed
- **Solution:** Check `DATABASE_URL` format and SSL mode settings

**Problem:** Migration conflicts
- **Solution:** Use `golang-migrate` tool, not GORM AutoMigrate

### 9.2 Contact Points

For issues related to:
- **Database schema:** CMS team (primary owner)
- **JWT authentication:** CMS team (issuer)
- **CRM API endpoints:** CRM team

---

## 10) Summary

✅ **CRM Service Database Integration - COMPLETE**

- Shared `turfa_platform` database configured
- All CRM tables created with no conflicts
- Migration tools and documentation in place
- Service running and operational
- Ready for CMS service coordination

**Next Steps for CMS Team:**
1. Update CMS to use `turfa_platform` database
2. Ensure `JWT_SECRET` matches CRM configuration
3. Issue JWTs with required claims
4. Test authentication flow between CMS and CRM

---

## Appendix: Files Changed

### Modified Files:
- `.env.example`
- `docker-compose.yml`
- `cmd/server/main.go`
- `README.md`

### New Files:
- `Makefile`
- `scripts/migrate.sh`

### Database Files (migrations):
- `migrations/000001_init_schema.up.sql` (existing, verified)
- `migrations/000001_init_schema.down.sql` (existing, verified)

---

**Document Version:** 1.0
**Last Updated:** January 31, 2026

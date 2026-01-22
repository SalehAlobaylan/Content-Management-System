# DigitalOcean Deployment Guide

## The Problem You Encountered

DigitalOcean App Platform was **auto-detecting** your project as a Go application and trying to run:
```bash
go run src/main.go
```

But your Dockerfile uses a **multi-stage build** that creates a compiled binary `./server` in an Alpine Linux container **without Go installed**. That's why it failed with "executable 'go' not found".

## The Solution

Created `.do/app.yaml` which tells DigitalOcean to:
1. ✅ Use your **Dockerfile** instead of buildpack auto-detection
2. ✅ Set up health checks on `/health` endpoint
3. ✅ Configure environment variables properly
4. ✅ Deploy from your main branch

## Deployment Steps

### Option 1: Using the App Spec File (Recommended)

1. **Push the `.do/app.yaml` file to your repository:**
   ```bash
   git add .do/app.yaml
   git commit -m "Add DigitalOcean app spec"
   git push
   ```

2. **Create app from spec in DigitalOcean:**
   - Go to [DigitalOcean App Platform](https://cloud.digitalocean.com/apps)
   - Click "Create App"
   - Choose "GitHub" and select your repository
   - Click "Edit Your App Spec" → "Edit as YAML"
   - Paste the contents of `.do/app.yaml`
   - Click "Save"

3. **Set your DATABASE_URL secret:**
   - In your app settings, go to "Settings" → "App-Level Environment Variables"
   - Add `DATABASE_URL` as an **encrypted** variable
   - Format: `postgres://user:password@host:port/database?sslmode=require`

### Option 2: Manual Configuration

If you prefer the UI:

1. **Create new app** from GitHub repository
2. **Build settings:**
   - Build Command: *(leave empty, Dockerfile handles it)*
   - Run Command: `./server`
   - Dockerfile Path: `Dockerfile`
3. **Environment variables:**
   - `ENV=production`
   - `DATABASE_URL=postgres://...` (encrypted)
   - `PORT=8080`
   - `GIN_MODE=release`
4. **HTTP Settings:**
   - HTTP Port: `8080`
   - Health Check Path: `/health`

## Database Options

### Option A: External PostgreSQL Database

Use your existing database (e.g., Supabase, AWS RDS, managed PostgreSQL):

```bash
DATABASE_URL=postgres://user:pass@your-db-host.com:5432/dbname?sslmode=require
```

### Option B: DigitalOcean Managed Database

Uncomment the `databases` section in `.do/app.yaml`:

```yaml
databases:
  - name: lumen-db
    engine: PG
    version: "15"
    production: true  # Set to true for production!
```

DigitalOcean will automatically inject `${db.DATABASE_URL}` which you can reference in your app.

## Verification

After deployment:

1. Check build logs - should see Dockerfile multi-stage build
2. Visit `https://your-app.ondigitalocean.app/health` - should return `200 OK`
3. Check runtime logs for successful database connection

## Troubleshooting

### "go: command not found" error
- ✅ **Fixed** - Using `.do/app.yaml` forces Dockerfile build

### Database connection failed
- Ensure `DATABASE_URL` is set as an **encrypted** environment variable
- For production, use `sslmode=require` instead of `disable`
- Verify database host is accessible from DigitalOcean

### Port binding issues
- Ensure `PORT=8080` matches Dockerfile `EXPOSE 8080`
- DigitalOcean automatically sets `$PORT` - your app should respect it

## Next Steps

1. ✅ Commit `.do/app.yaml` to your repository
2. ✅ Set `DATABASE_URL` in DigitalOcean app settings
3. ✅ Deploy and verify health check passes
4. 🎉 Your app should be running!

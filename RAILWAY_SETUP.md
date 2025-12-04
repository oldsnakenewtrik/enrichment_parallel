# Railway Deployment Setup Guide

Complete guide for deploying the Email Enrichment Tool to Railway.

---

## Quick Deploy Checklist

- [ ] Push code to GitHub repository
- [ ] Create new Railway project
- [ ] Connect GitHub repo
- [ ] Verify deployment settings
- [ ] Test health endpoint
- [ ] Access your app!

---

## 1. Environment Variables

### Required Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | ✅ Auto-set | Railway automatically injects this. **Do not set manually.** |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| None | - | The Parallel API key is entered via the web UI for security |

> **Note:** This app intentionally does NOT store API keys as environment variables. Users enter their Parallel API key through the web interface for each job, which is more secure for multi-tenant usage.

---

## 2. Build Configuration

Railway uses **Nixpacks** to auto-detect and build Python applications.

### Auto-Detected Settings

- **Runtime:** Python 3.11+
- **Package Manager:** pip
- **Dependencies:** Installed from `requirements.txt`

### Build Command

```bash
# Default (auto-detected) - no custom build command needed
pip install -r requirements.txt
```

### If Custom Build Needed

Set in Railway Dashboard → Settings → Build:

```bash
pip install --upgrade pip && pip install -r requirements.txt
```

---

## 3. Start Command

### Primary (via railway.toml)

The `railway.toml` file configures the start command:

```toml
[deploy]
startCommand = "uvicorn main:app --host 0.0.0.0 --port $PORT"
```

### Backup (via Procfile)

```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

### Manual Override (if needed)

In Railway Dashboard → Settings → Deploy → Start Command:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

---

## 4. Health Check

### Endpoint

```
GET /health
```

### Response

```json
{
  "status": "healthy",
  "jobs_count": 0
}
```

### Railway Configuration (in railway.toml)

```toml
[deploy]
healthcheckPath = "/health"
healthcheckTimeout = 30
```

---

## 5. Railway.toml Configuration

Complete configuration file:

```toml
[build]
builder = "nixpacks"

[deploy]
startCommand = "uvicorn main:app --host 0.0.0.0 --port $PORT"
healthcheckPath = "/health"
healthcheckTimeout = 30
restartPolicyType = "on_failure"
restartPolicyMaxRetries = 3
```

---

## 6. Deployment Methods

### Method A: GitHub Integration (Recommended)

1. Push `railway_app/` contents to a GitHub repository
2. Go to [railway.app](https://railway.app)
3. Click **"New Project"** → **"Deploy from GitHub repo"**
4. Select your repository
5. Railway auto-detects Python and deploys

### Method B: Railway CLI

```bash
# Install CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Navigate to app directory
cd railway_app

# Initialize new project
railway init

# Deploy
railway up

# Get deployment URL
railway open
```

### Method C: Direct Upload

1. Go to [railway.app](https://railway.app)
2. Create new project
3. Choose "Empty project"
4. Add a service from template or upload

---

## 7. Post-Deployment Verification

### Step 1: Check Deployment Logs

In Railway Dashboard:
- Go to your service
- Click "Deployments"
- View build and deploy logs

### Step 2: Test Health Endpoint

```bash
curl https://your-app.railway.app/health
```

Expected response:
```json
{"status": "healthy", "jobs_count": 0}
```

### Step 3: Access Web UI

Open `https://your-app.railway.app` in browser.

---

## 8. Troubleshooting

### Issue: Build Fails

**Check:**
- `requirements.txt` has all dependencies
- Python version compatibility (needs 3.9+)

**Fix:** Add Python version in `runtime.txt`:
```
python-3.11.0
```

### Issue: App Crashes on Start

**Check:**
- PORT is being read from environment: `int(os.environ.get("PORT", 8000))`
- Host is `0.0.0.0`, not `127.0.0.1` or `localhost`

### Issue: Health Check Fails

**Check:**
- `/health` endpoint returns 200 status
- healthcheckTimeout is sufficient (30+ seconds)

### Issue: Timeout on Long Jobs

**Note:** Railway services stay alive as long as there's activity. The app uses:
- Background threading for long-running jobs
- SSE for real-time log streaming
- These keep the connection alive during enrichment

---

## 9. Estimated Costs

### Railway Pricing

Railway offers:
- **Hobby Plan:** $5/month credit (includes 500 hours)
- **Pro Plan:** Usage-based pricing

### App Resource Usage

| Metric | Typical Usage |
|--------|---------------|
| Memory | ~256MB |
| CPU | Minimal (I/O bound) |
| Bandwidth | Low (CSV upload/download) |

### Parallel API Costs

| Processor | Cost per Company |
|-----------|-----------------|
| Base | $0.02 |
| Core | $0.10 |

Example: 1,000 companies with Base processor = $20.00

---

## 10. Files Reference

```
railway_app/
├── main.py           # FastAPI application
├── requirements.txt  # Python dependencies
├── railway.toml      # Railway configuration
├── Procfile          # Backup start command
├── .gitignore        # Git ignore patterns
├── README.md         # Usage documentation
├── RAILWAY_SETUP.md  # This file
├── uploads/          # Uploaded CSV files (created at runtime)
└── results/          # Job results (created at runtime)
```

---

## 11. Quick Commands Reference

```bash
# Deploy
railway up

# View logs
railway logs

# Open deployed app
railway open

# Check status
railway status

# Redeploy
railway up --detach
```

---

## Need Help?

- [Railway Documentation](https://docs.railway.app)
- [FastAPI Documentation](https://fastapi.tiangolo.com)
- [Parallel Task API](https://parallel.ai)
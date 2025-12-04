# Email Enrichment Tool

A Railway-deployable web application for enriching company data with email addresses using the Parallel Task API.

## Features

- **CSV Upload**: Upload your company data as CSV
- **Deep Email Search**: Finds multiple emails per company (primary, secondary, admin, careers)
- **Streaming Logs**: Real-time log streaming in the UI via Server-Sent Events
- **Retry Logic**: Automatic retries with exponential backoff for failed requests
- **Cost Tracking**: Real-time cost tracking per job
- **Long-Running Jobs**: Designed to handle thousands of companies without timeout
- **Download Results**: Export results as JSON or CSV

## Deployment to Railway

### Option 1: Deploy via GitHub

1. Push this folder to a GitHub repository
2. Go to [Railway](https://railway.app)
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Railway will automatically detect and deploy

### Option 2: Deploy via Railway CLI

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Initialize and deploy
cd railway_app
railway init
railway up
```

### Option 3: Deploy via Railway MCP

Use the Railway MCP server to deploy:
1. Create a new project: `railway project_create`
2. Deploy from this directory

## Usage

1. Open the deployed URL in your browser
2. Upload a CSV file with columns:
   - company_name (required)
   - address
   - city
   - state
   - zip
   - type (facility type)
   - number
   - phone
3. Enter your Parallel API key
4. Select processor type:
   - **Base** ($0.02/company): Fast, basic research
   - **Core** ($0.10/company): Deep research, more sources
5. Click "Start Enrichment"
6. Monitor progress via streaming logs
7. Download results when complete

## CSV Format

Your CSV should have at least 4 columns:
```
COMPANY NAME,ADDRESS,CITY,STATE,ZIP,TYPE,NUMBER,PHONE
Shady Lane Retirement,201 Main St,Tampa,FL,33601,Assisted Living,12,(555) 123-4567
```

## Output Fields

The enrichment finds these email types:
- `primary_email`: Main contact/info email
- `secondary_email`: Admissions, sales, or department email
- `admin_email`: Administration/management email
- `careers_email`: HR/careers email
- `website`: Company website URL
- `email_sources`: Where each email was found
- `confidence`: Confidence level (high/medium/low)

## Cost Estimates

| Companies | Base Processor | Core Processor |
|-----------|----------------|----------------|
| 100       | $2.00          | $10.00         |
| 500       | $10.00         | $50.00         |
| 1,000     | $20.00         | $100.00        |
| 1,907     | $38.14         | $190.70        |

## Local Development

```bash
cd railway_app
pip install -r requirements.txt
python main.py
```

Open http://localhost:8000

## Environment Variables

None required - the API key is provided through the UI for security.

## API Endpoints

- `GET /` - Web UI
- `POST /upload` - Upload CSV and start job
- `GET /status/{job_id}` - Get job status
- `GET /logs/{job_id}` - Stream logs (SSE)
- `GET /results/{job_id}.json` - Download JSON results
- `GET /results/{job_id}.csv` - Download CSV results
- `GET /health` - Health check
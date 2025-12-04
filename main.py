"""
Email Enrichment Web Application
- CSV upload
- Streaming logs via SSE
- Background processing with retry logic
- Deep email search (multiple emails per location)
"""
import os
import csv
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
import threading
from queue import Queue
import time

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Parallel API
from parallel import Parallel

app = FastAPI(title="Email Enrichment API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage for jobs and logs
JOBS: Dict[str, Dict[str, Any]] = {}
LOG_QUEUES: Dict[str, Queue] = {}
RESULTS_DIR = Path("results")
UPLOADS_DIR = Path("uploads")
RESULTS_DIR.mkdir(exist_ok=True)
UPLOADS_DIR.mkdir(exist_ok=True)

# Cost tracking
COST_PER_RUN = {
    "base": 0.02,
    "core": 0.10,
}

# Schemas for deep email search
class CompanyInput(BaseModel):
    company_name: str = Field(description="Name of the assisted living facility or company")
    address: str = Field(description="Street address of the location")
    city: str = Field(description="City where the facility is located")
    state: str = Field(description="State abbreviation (e.g., FL)")
    phone: Optional[str] = Field(default=None, description="Phone number if available")

class DeepEmailOutput(BaseModel):
    primary_email: Optional[str] = Field(
        default=None, 
        description="The main contact/info email address found on the company website or business listings"
    )
    secondary_email: Optional[str] = Field(
        default=None,
        description="A secondary email such as admissions, sales, or department-specific email"
    )
    admin_email: Optional[str] = Field(
        default=None,
        description="Administrative or management email if different from primary"
    )
    careers_email: Optional[str] = Field(
        default=None,
        description="HR or careers email address if available"
    )
    website: Optional[str] = Field(
        default=None,
        description="Company website URL"
    )
    email_sources: Optional[str] = Field(
        default=None,
        description="Where each email was found (website, LinkedIn, directories, etc.)"
    )
    confidence: Optional[str] = Field(
        default=None,
        description="Confidence level for the emails found (high/medium/low)"
    )


class JobStatus(BaseModel):
    job_id: str
    status: str  # pending, running, completed, failed
    total_rows: int
    processed_rows: int
    emails_found: int
    estimated_cost: float
    actual_cost: float
    start_time: Optional[str]
    end_time: Optional[str]
    error: Optional[str]


def log_message(job_id: str, message: str, level: str = "INFO"):
    """Add a log message to the job's log queue"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    
    if job_id in LOG_QUEUES:
        LOG_QUEUES[job_id].put(log_entry)
    
    # Also print to console for Railway logs
    print(f"[{job_id[:8]}] {log_entry}")


def process_enrichment_with_retry(
    client: Parallel,
    company: dict,
    task_spec: dict,
    processor: str,
    max_retries: int = 3,
    job_id: str = None
) -> dict:
    """Process a single company with retry logic"""
    
    input_data = {
        "company_name": company['company_name'],
        "address": company.get('address', ''),
        "city": company.get('city', ''),
        "state": company.get('state', ''),
        "phone": company.get('phone')
    }
    
    for attempt in range(max_retries):
        try:
            log_message(job_id, f"Attempt {attempt + 1}/{max_retries} for {company['company_name']}")
            
            # Create task run
            task_run = client.task_run.create(
                input=input_data,
                task_spec=task_spec,
                processor=processor
            )
            
            log_message(job_id, f"Run ID: {task_run.run_id}")
            
            # Poll for result with extended timeout
            start_time = time.time()
            max_wait = 180  # 3 minutes max per company
            
            while time.time() - start_time < max_wait:
                try:
                    result = client.task_run.result(task_run.run_id, api_timeout=30)
                    
                    if result.run.status == "completed":
                        output = result.output.content if hasattr(result.output, 'content') else {}
                        
                        # Count emails found
                        email_fields = ['primary_email', 'secondary_email', 'admin_email', 'careers_email']
                        emails = [output.get(f) for f in email_fields if output.get(f)]
                        
                        log_message(job_id, f"SUCCESS: Found {len(emails)} email(s)")
                        
                        return {
                            'status': 'success',
                            'company_name': company['company_name'],
                            'city': company.get('city'),
                            'state': company.get('state'),
                            'primary_email': output.get('primary_email'),
                            'secondary_email': output.get('secondary_email'),
                            'admin_email': output.get('admin_email'),
                            'careers_email': output.get('careers_email'),
                            'website': output.get('website'),
                            'email_sources': output.get('email_sources'),
                            'confidence': output.get('confidence'),
                            'run_id': task_run.run_id,
                            'emails_found': len(emails),
                            'cost': COST_PER_RUN.get(processor, 0.05)
                        }
                    
                    elif result.run.status == "failed":
                        raise Exception(f"Task failed: {result.run}")
                    
                except Exception as e:
                    if "408" in str(e) or "still active" in str(e).lower():
                        log_message(job_id, f"Still processing... ({int(time.time() - start_time)}s)")
                        time.sleep(10)
                    else:
                        raise
            
            # Timeout
            raise Exception(f"Timeout after {max_wait}s")
            
        except Exception as e:
            log_message(job_id, f"Attempt {attempt + 1} failed: {str(e)}", "WARN")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10  # Exponential backoff
                log_message(job_id, f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log_message(job_id, f"All retries failed for {company['company_name']}", "ERROR")
                return {
                    'status': 'failed',
                    'company_name': company['company_name'],
                    'error': str(e),
                    'emails_found': 0,
                    'cost': COST_PER_RUN.get(processor, 0.05)  # Still charged for attempt
                }
    
    return {'status': 'failed', 'company_name': company['company_name'], 'emails_found': 0, 'cost': 0}


def run_enrichment_job(job_id: str, companies: list, processor: str, api_key: str):
    """Background job to process all companies"""
    
    try:
        JOBS[job_id]['status'] = 'running'
        JOBS[job_id]['start_time'] = datetime.now().isoformat()
        
        log_message(job_id, f"Starting enrichment job with {len(companies)} companies")
        log_message(job_id, f"Processor: {processor}, Est. cost: ${len(companies) * COST_PER_RUN.get(processor, 0.05):.2f}")
        
        client = Parallel(api_key=api_key)
        
        # Build task spec for deep email search
        task_spec = {
            "input_schema": {
                "type": "json",
                "json_schema": CompanyInput.model_json_schema(),
            },
            "output_schema": {
                "type": "json",
                "json_schema": DeepEmailOutput.model_json_schema(),
            },
        }
        
        results = []
        total_emails = 0
        actual_cost = 0.0
        
        for idx, company in enumerate(companies):
            log_message(job_id, f"Processing {idx + 1}/{len(companies)}: {company['company_name']}")
            
            result = process_enrichment_with_retry(
                client=client,
                company=company,
                task_spec=task_spec,
                processor=processor,
                max_retries=3,
                job_id=job_id
            )
            
            results.append(result)
            total_emails += result.get('emails_found', 0)
            actual_cost += result.get('cost', 0)
            
            # Update job status
            JOBS[job_id]['processed_rows'] = idx + 1
            JOBS[job_id]['emails_found'] = total_emails
            JOBS[job_id]['actual_cost'] = actual_cost
            
            # Rate limiting - small delay between requests
            time.sleep(1)
        
        # Save results
        results_file = RESULTS_DIR / f"{job_id}_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Also save as CSV
        csv_file = RESULTS_DIR / f"{job_id}_results.csv"
        if results:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=results[0].keys())
                writer.writeheader()
                writer.writerows(results)
        
        JOBS[job_id]['status'] = 'completed'
        JOBS[job_id]['end_time'] = datetime.now().isoformat()
        
        log_message(job_id, f"JOB COMPLETED!")
        log_message(job_id, f"Total companies: {len(companies)}")
        log_message(job_id, f"Total emails found: {total_emails}")
        log_message(job_id, f"Total cost: ${actual_cost:.2f}")
        log_message(job_id, f"Results saved to: {results_file}")
        
    except Exception as e:
        JOBS[job_id]['status'] = 'failed'
        JOBS[job_id]['error'] = str(e)
        JOBS[job_id]['end_time'] = datetime.now().isoformat()
        log_message(job_id, f"JOB FAILED: {str(e)}", "ERROR")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main UI"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Email Enrichment Tool</title>
    <style>
        * { box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px; 
            margin: 0 auto; 
            padding: 20px;
            background: #f5f5f5;
        }
        h1 { color: #333; }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: 600; }
        input, select { 
            width: 100%; 
            padding: 10px; 
            border: 1px solid #ddd; 
            border-radius: 4px;
            font-size: 14px;
        }
        button {
            background: #007bff;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:hover { background: #0056b3; }
        button:disabled { background: #ccc; cursor: not-allowed; }
        .logs {
            background: #1e1e1e;
            color: #00ff00;
            padding: 15px;
            border-radius: 4px;
            height: 400px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            white-space: pre-wrap;
        }
        .status {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }
        .stat {
            background: #e9ecef;
            padding: 15px;
            border-radius: 4px;
            text-align: center;
        }
        .stat-value { font-size: 24px; font-weight: bold; color: #007bff; }
        .stat-label { font-size: 12px; color: #666; margin-top: 5px; }
        .progress {
            height: 20px;
            background: #e9ecef;
            border-radius: 10px;
            overflow: hidden;
            margin-bottom: 20px;
        }
        .progress-bar {
            height: 100%;
            background: #28a745;
            transition: width 0.3s;
        }
        .cost-estimate {
            background: #fff3cd;
            border: 1px solid #ffc107;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 15px;
        }
        a { color: #007bff; }
    </style>
</head>
<body>
    <h1>Email Enrichment Tool</h1>
    
    <div class="card">
        <h2>Upload CSV</h2>
        <div class="form-group">
            <label>CSV File (with columns: company_name, address, city, state, zip, type, number, phone)</label>
            <input type="file" id="csvFile" accept=".csv">
        </div>
        <div class="form-group">
            <label>Processor</label>
            <select id="processor">
                <option value="base">Base ($0.02/company) - Fast, basic research</option>
                <option value="core">Core ($0.10/company) - Deep research, more sources</option>
            </select>
        </div>
        <div class="form-group">
            <label>Parallel API Key</label>
            <input type="password" id="apiKey" placeholder="Your Parallel API key">
        </div>
        <div class="cost-estimate" id="costEstimate" style="display:none;">
            <strong>Estimated Cost:</strong> <span id="estimatedCost">$0.00</span>
            <br><small>For <span id="rowCount">0</span> companies</small>
        </div>
        <button onclick="startEnrichment()" id="startBtn">Start Enrichment</button>
    </div>
    
    <div class="card" id="statusCard" style="display:none;">
        <h2>Job Status</h2>
        <div class="progress">
            <div class="progress-bar" id="progressBar" style="width: 0%"></div>
        </div>
        <div class="status">
            <div class="stat">
                <div class="stat-value" id="statProcessed">0</div>
                <div class="stat-label">Processed</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statEmails">0</div>
                <div class="stat-label">Emails Found</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statCost">$0.00</div>
                <div class="stat-label">Cost</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statStatus">-</div>
                <div class="stat-label">Status</div>
            </div>
        </div>
        <div id="downloadLinks" style="display:none; margin-bottom: 15px;">
            <a href="#" id="jsonLink" target="_blank">Download JSON Results</a> | 
            <a href="#" id="csvLink" target="_blank">Download CSV Results</a>
        </div>
        <h3>Live Logs</h3>
        <div class="logs" id="logs"></div>
    </div>

    <script>
        let eventSource = null;
        let currentJobId = null;
        
        document.getElementById('csvFile').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                const reader = new FileReader();
                reader.onload = function(e) {
                    const lines = e.target.result.split('\\n').filter(l => l.trim());
                    const rowCount = lines.length;
                    const processor = document.getElementById('processor').value;
                    const cost = processor === 'core' ? 0.10 : 0.02;
                    
                    document.getElementById('rowCount').textContent = rowCount;
                    document.getElementById('estimatedCost').textContent = '$' + (rowCount * cost).toFixed(2);
                    document.getElementById('costEstimate').style.display = 'block';
                };
                reader.readAsText(file);
            }
        });
        
        document.getElementById('processor').addEventListener('change', function() {
            const rowCount = parseInt(document.getElementById('rowCount').textContent) || 0;
            const cost = this.value === 'core' ? 0.10 : 0.02;
            document.getElementById('estimatedCost').textContent = '$' + (rowCount * cost).toFixed(2);
        });

        async function startEnrichment() {
            const fileInput = document.getElementById('csvFile');
            const processor = document.getElementById('processor').value;
            const apiKey = document.getElementById('apiKey').value;
            
            if (!fileInput.files[0]) {
                alert('Please select a CSV file');
                return;
            }
            if (!apiKey) {
                alert('Please enter your Parallel API key');
                return;
            }
            
            const formData = new FormData();
            formData.append('file', fileInput.files[0]);
            formData.append('processor', processor);
            formData.append('api_key', apiKey);
            
            document.getElementById('startBtn').disabled = true;
            document.getElementById('statusCard').style.display = 'block';
            document.getElementById('logs').innerHTML = '';
            
            try {
                const response = await fetch('/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.job_id) {
                    currentJobId = data.job_id;
                    connectToLogs(data.job_id);
                    pollStatus(data.job_id);
                } else {
                    alert('Error: ' + (data.error || 'Unknown error'));
                    document.getElementById('startBtn').disabled = false;
                }
            } catch (e) {
                alert('Error: ' + e.message);
                document.getElementById('startBtn').disabled = false;
            }
        }
        
        function connectToLogs(jobId) {
            if (eventSource) {
                eventSource.close();
            }
            
            eventSource = new EventSource('/logs/' + jobId);
            eventSource.onmessage = function(e) {
                const logsDiv = document.getElementById('logs');
                logsDiv.innerHTML += e.data + '\\n';
                logsDiv.scrollTop = logsDiv.scrollHeight;
            };
            eventSource.onerror = function(e) {
                console.log('SSE error, will retry...');
            };
        }
        
        function pollStatus(jobId) {
            const poll = async () => {
                try {
                    const response = await fetch('/status/' + jobId);
                    const status = await response.json();
                    
                    const progress = status.total_rows > 0 
                        ? (status.processed_rows / status.total_rows * 100) 
                        : 0;
                    
                    document.getElementById('progressBar').style.width = progress + '%';
                    document.getElementById('statProcessed').textContent = 
                        status.processed_rows + '/' + status.total_rows;
                    document.getElementById('statEmails').textContent = status.emails_found;
                    document.getElementById('statCost').textContent = '$' + status.actual_cost.toFixed(2);
                    document.getElementById('statStatus').textContent = status.status.toUpperCase();
                    
                    if (status.status === 'completed' || status.status === 'failed') {
                        document.getElementById('startBtn').disabled = false;
                        if (eventSource) eventSource.close();
                        
                        if (status.status === 'completed') {
                            document.getElementById('downloadLinks').style.display = 'block';
                            document.getElementById('jsonLink').href = '/results/' + jobId + '.json';
                            document.getElementById('csvLink').href = '/results/' + jobId + '.csv';
                        }
                    } else {
                        setTimeout(poll, 2000);
                    }
                } catch (e) {
                    console.error('Poll error:', e);
                    setTimeout(poll, 5000);
                }
            };
            poll();
        }
    </script>
</body>
</html>
    """


@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    processor: str = "base",
    api_key: str = None,
    background_tasks: BackgroundTasks = None
):
    """Upload CSV and start enrichment job"""
    
    if not api_key:
        raise HTTPException(status_code=400, detail="API key required")
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Generate job ID
    job_id = str(uuid.uuid4())
    
    # Save uploaded file
    content = await file.read()
    upload_path = UPLOADS_DIR / f"{job_id}.csv"
    with open(upload_path, 'wb') as f:
        f.write(content)
    
    # Parse CSV
    companies = []
    content_str = content.decode('utf-8')
    reader = csv.reader(content_str.splitlines())
    
    for row in reader:
        if len(row) >= 4:
            companies.append({
                'company_name': row[0],
                'address': row[1] if len(row) > 1 else '',
                'city': row[2] if len(row) > 2 else '',
                'state': row[3] if len(row) > 3 else '',
                'zip_code': row[4] if len(row) > 4 else '',
                'phone': row[7] if len(row) > 7 else None
            })
    
    if not companies:
        raise HTTPException(status_code=400, detail="No valid rows found in CSV")
    
    # Initialize job
    estimated_cost = len(companies) * COST_PER_RUN.get(processor, 0.05)
    
    JOBS[job_id] = {
        'job_id': job_id,
        'status': 'pending',
        'total_rows': len(companies),
        'processed_rows': 0,
        'emails_found': 0,
        'estimated_cost': estimated_cost,
        'actual_cost': 0.0,
        'start_time': None,
        'end_time': None,
        'error': None
    }
    
    # Initialize log queue
    LOG_QUEUES[job_id] = Queue()
    
    # Start background job
    thread = threading.Thread(
        target=run_enrichment_job,
        args=(job_id, companies, processor, api_key)
    )
    thread.daemon = True
    thread.start()
    
    return {"job_id": job_id, "total_rows": len(companies), "estimated_cost": estimated_cost}


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Get job status"""
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")
    return JOBS[job_id]


@app.get("/logs/{job_id}")
async def stream_logs(job_id: str):
    """Stream logs via Server-Sent Events"""
    
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")
    
    async def event_generator():
        while True:
            if job_id in LOG_QUEUES:
                queue = LOG_QUEUES[job_id]
                while not queue.empty():
                    try:
                        message = queue.get_nowait()
                        yield f"data: {message}\n\n"
                    except:
                        break
            
            # Check if job is done
            if JOBS[job_id]['status'] in ['completed', 'failed']:
                yield f"data: [END] Job {JOBS[job_id]['status']}\n\n"
                break
            
            await asyncio.sleep(0.5)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/results/{job_id}.json")
async def download_json_results(job_id: str):
    """Download results as JSON"""
    results_file = RESULTS_DIR / f"{job_id}_results.json"
    if not results_file.exists():
        raise HTTPException(status_code=404, detail="Results not found")
    
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    return JSONResponse(content=results)


@app.get("/results/{job_id}.csv")
async def download_csv_results(job_id: str):
    """Download results as CSV"""
    results_file = RESULTS_DIR / f"{job_id}_results.csv"
    if not results_file.exists():
        raise HTTPException(status_code=404, detail="Results not found")
    
    with open(results_file, 'r') as f:
        content = f.read()
    
    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=enrichment_results_{job_id[:8]}.csv"}
    )


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    return {"status": "healthy", "jobs_count": len(JOBS)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
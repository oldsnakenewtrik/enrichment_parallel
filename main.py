"""
Email Enrichment Web Application - FindAll API Variant
- CSV upload
- Streaming logs via SSE
- Background processing with retry logic
- Uses Parallel FindAll API for discovering emails
- PostgreSQL persistence for Railway
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

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# For direct API calls to Parallel FindAll
import requests

# Database
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from contextlib import contextmanager

app = FastAPI(title="Email Enrichment API", version="1.0.0")

# Database connection
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Get database connection, returns None if no DATABASE_URL"""
    if not DATABASE_URL:
        return None
    try:
        # Railway uses postgres:// but psycopg2 needs postgresql://
        db_url = DATABASE_URL.replace("postgres://", "postgresql://")
        return psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

@contextmanager
def db_cursor():
    """Context manager for database cursor"""
    conn = get_db_connection()
    if conn is None:
        yield None
        return
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_db():
    """Initialize database tables"""
    conn = get_db_connection()
    if conn is None:
        print("No DATABASE_URL configured - using in-memory storage only")
        return
    
    try:
        cur = conn.cursor()
        
        # Create jobs table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id VARCHAR(36) PRIMARY KEY,
                status VARCHAR(20) DEFAULT 'pending',
                total_rows INTEGER DEFAULT 0,
                processed_rows INTEGER DEFAULT 0,
                emails_found INTEGER DEFAULT 0,
                estimated_cost DECIMAL(10,2) DEFAULT 0,
                actual_cost DECIMAL(10,2) DEFAULT 0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error TEXT,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0,
                processor VARCHAR(20) DEFAULT 'base',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create results table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(36) REFERENCES jobs(job_id),
                company_name VARCHAR(500),
                city VARCHAR(200),
                state VARCHAR(50),
                status VARCHAR(20),
                primary_email VARCHAR(500),
                secondary_email VARCHAR(500),
                admin_email VARCHAR(500),
                careers_email VARCHAR(500),
                website VARCHAR(500),
                email_sources TEXT,
                confidence VARCHAR(50),
                run_id VARCHAR(100),
                emails_found INTEGER DEFAULT 0,
                cost DECIMAL(10,4) DEFAULT 0,
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster job lookups
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id)
        """)
        
        conn.commit()
        print("Database tables initialized successfully")
    except Exception as e:
        print(f"Database initialization error: {e}")
        conn.rollback()
    finally:
        conn.close()

# Initialize database on startup
init_db()

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

# FindAll API configuration
FINDALL_API_BASE = "https://api.parallel.ai/v1beta/findall"
FINDALL_BETA_HEADER = "findall-2025-09-15"

# Cost tracking - FindAll uses CPM (cost per 1000 matches)
# For single-company queries, we estimate per-query cost
COST_PER_RUN = {
    "base": 0.06,    # $60 CPM / 1000 = $0.06 per entity
    "core": 0.23,    # $230 CPM / 1000 = $0.23 per entity
    "pro": 1.43,     # $1430 CPM / 1000 = $1.43 per entity
}

# Incremental save settings
SAVE_EVERY_N = 5  # Save results every N companies

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


def save_result_to_db(job_id: str, result: dict):
    """Save a single result to the database"""
    with db_cursor() as cur:
        if cur is None:
            return
        
        cur.execute("""
            INSERT INTO results (
                job_id, company_name, city, state, status,
                primary_email, secondary_email, admin_email, careers_email,
                website, email_sources, confidence, run_id, emails_found, cost, error
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            job_id,
            result.get('company_name'),
            result.get('city'),
            result.get('state'),
            result.get('status'),
            result.get('primary_email'),
            result.get('secondary_email'),
            result.get('admin_email'),
            result.get('careers_email'),
            result.get('website'),
            result.get('email_sources'),
            result.get('confidence'),
            result.get('run_id'),
            result.get('emails_found', 0),
            result.get('cost', 0),
            result.get('error')
        ))


def update_job_status_db(job_id: str, updates: dict):
    """Update job status in database"""
    with db_cursor() as cur:
        if cur is None:
            return
        
        set_clauses = []
        values = []
        for key, value in updates.items():
            set_clauses.append(f"{key} = %s")
            values.append(value)
        values.append(job_id)
        
        cur.execute(f"""
            UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = %s
        """, values)


def create_job_db(job_id: str, job_data: dict):
    """Create a new job in the database"""
    with db_cursor() as cur:
        if cur is None:
            return
        
        cur.execute("""
            INSERT INTO jobs (
                job_id, status, total_rows, processed_rows, emails_found,
                estimated_cost, actual_cost, success_count, fail_count, processor
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            job_id,
            job_data.get('status', 'pending'),
            job_data.get('total_rows', 0),
            job_data.get('processed_rows', 0),
            job_data.get('emails_found', 0),
            job_data.get('estimated_cost', 0),
            job_data.get('actual_cost', 0),
            job_data.get('success_count', 0),
            job_data.get('fail_count', 0),
            job_data.get('processor', 'base')
        ))


def get_job_from_db(job_id: str) -> Optional[dict]:
    """Get job from database"""
    with db_cursor() as cur:
        if cur is None:
            return None
        
        cur.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
        if row:
            return dict(row)
    return None


def get_results_from_db(job_id: str) -> list:
    """Get all results for a job from database"""
    with db_cursor() as cur:
        if cur is None:
            return []
        
        cur.execute("""
            SELECT company_name, city, state, status, primary_email, secondary_email,
                   admin_email, careers_email, website, email_sources, confidence,
                   run_id, emails_found, cost, error
            FROM results WHERE job_id = %s ORDER BY id
        """, (job_id,))
        return [dict(row) for row in cur.fetchall()]


def save_incremental_results(job_id: str, results: list):
    """Save results incrementally to prevent data loss (file-based fallback)"""
    # Save JSON
    json_file = RESULTS_DIR / f"{job_id}_results.json"
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Save CSV
    csv_file = RESULTS_DIR / f"{job_id}_results.csv"
    if results:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['status', 'company_name', 'city', 'state', 'primary_email',
                         'secondary_email', 'admin_email', 'careers_email', 'website',
                         'email_sources', 'confidence', 'run_id', 'emails_found', 'cost', 'error']
            writer = csv.DictWriter(f, fieldnames=[k for k in fieldnames if any(k in r for r in results)])
            writer.writeheader()
            writer.writerows(results)
    
    return json_file, csv_file


def load_existing_results(job_id: str) -> list:
    """Load existing results - try database first, then file"""
    # Try database first
    db_results = get_results_from_db(job_id)
    if db_results:
        return db_results
    
    # Fall back to file
    json_file = RESULTS_DIR / f"{job_id}_results.json"
    if json_file.exists():
        with open(json_file, 'r') as f:
            return json.load(f)
    return []


def get_findall_headers(api_key: str) -> dict:
    """Get headers for FindAll API requests"""
    return {
        "x-api-key": api_key,
        "parallel-beta": FINDALL_BETA_HEADER,
        "Content-Type": "application/json"
    }


def create_findall_run(
    api_key: str,
    company: dict,
    generator: str,
    job_id: str = None,
    progress_prefix: str = ""
) -> Optional[str]:
    """Create a FindAll run and return the findall_id"""

    # Build the objective for finding emails
    company_name = company['company_name']
    city = company.get('city', '')
    state = company.get('state', '')
    address = company.get('address', '')

    location_parts = [p for p in [address, city, state] if p]
    location_str = ", ".join(location_parts) if location_parts else "USA"

    objective = f"FindAll email addresses for {company_name} located at {location_str}"

    # Match conditions for email discovery
    match_conditions = [
        {
            "name": "valid_email_check",
            "description": "Must be a valid email address associated with this specific company or facility."
        },
        {
            "name": "company_association_check",
            "description": f"Email must belong to or be associated with {company_name}."
        }
    ]

    # Optional enrichment fields to capture
    enrichment_schema = {
        "type": "object",
        "properties": {
            "primary_email": {
                "type": "string",
                "description": "The main contact/info email address"
            },
            "secondary_email": {
                "type": "string",
                "description": "A secondary email such as admissions or sales"
            },
            "admin_email": {
                "type": "string",
                "description": "Administrative or management email"
            },
            "careers_email": {
                "type": "string",
                "description": "HR or careers email address"
            },
            "website": {
                "type": "string",
                "description": "Company website URL"
            },
            "email_source": {
                "type": "string",
                "description": "Where the email was found"
            }
        }
    }

    payload = {
        "objective": objective,
        "entity_type": "email_addresses",
        "match_conditions": match_conditions,
        "generator": generator,
        "match_limit": 10,  # Get up to 10 email addresses per company
        "enrichment_schema": enrichment_schema
    }

    headers = get_findall_headers(api_key)

    log_message(job_id, f"{progress_prefix} Creating FindAll run...")
    log_message(job_id, f"{progress_prefix} Objective: {objective}")

    response = requests.post(
        f"{FINDALL_API_BASE}/runs",
        headers=headers,
        json=payload,
        timeout=30
    )

    if response.status_code != 200 and response.status_code != 201:
        raise Exception(f"FindAll API error: {response.status_code} - {response.text}")

    result = response.json()
    findall_id = result.get("findall_id") or result.get("id")

    if not findall_id:
        raise Exception(f"No findall_id in response: {result}")

    log_message(job_id, f"{progress_prefix} FindAll Run ID: {findall_id}")
    return findall_id


def poll_findall_status(api_key: str, findall_id: str) -> dict:
    """Poll for FindAll run status"""
    headers = get_findall_headers(api_key)

    response = requests.get(
        f"{FINDALL_API_BASE}/runs/{findall_id}",
        headers=headers,
        timeout=30
    )

    if response.status_code != 200:
        raise Exception(f"Status poll error: {response.status_code} - {response.text}")

    return response.json()


def get_findall_results(api_key: str, findall_id: str) -> dict:
    """Get FindAll run results"""
    headers = get_findall_headers(api_key)

    response = requests.get(
        f"{FINDALL_API_BASE}/runs/{findall_id}/result",
        headers=headers,
        timeout=30
    )

    if response.status_code != 200:
        raise Exception(f"Results fetch error: {response.status_code} - {response.text}")

    return response.json()


def extract_emails_from_findall_results(results: dict) -> dict:
    """Extract email addresses from FindAll results"""
    emails = {
        'primary_email': None,
        'secondary_email': None,
        'admin_email': None,
        'careers_email': None,
        'website': None,
        'email_sources': []
    }

    matches = results.get("matches", []) or results.get("results", []) or []

    email_list = []
    for match in matches:
        # Extract email from different possible locations in the result
        email = None
        source = None

        # Try different field names
        if isinstance(match, dict):
            email = (match.get("email") or
                    match.get("value") or
                    match.get("name") or
                    match.get("entity"))

            # Get enrichment data if available
            enrichment = match.get("enrichment", {})
            if enrichment:
                if enrichment.get("primary_email"):
                    emails['primary_email'] = enrichment.get("primary_email")
                if enrichment.get("secondary_email"):
                    emails['secondary_email'] = enrichment.get("secondary_email")
                if enrichment.get("admin_email"):
                    emails['admin_email'] = enrichment.get("admin_email")
                if enrichment.get("careers_email"):
                    emails['careers_email'] = enrichment.get("careers_email")
                if enrichment.get("website"):
                    emails['website'] = enrichment.get("website")
                if enrichment.get("email_source"):
                    emails['email_sources'].append(enrichment.get("email_source"))

            source = match.get("source") or match.get("reasoning") or match.get("citation")

            # Check if it looks like an email
            if email and "@" in str(email):
                email_list.append({"email": email, "source": source})
                if source:
                    emails['email_sources'].append(str(source))
        elif isinstance(match, str) and "@" in match:
            email_list.append({"email": match, "source": None})

    # Assign emails to fields if not already set from enrichment
    for i, email_data in enumerate(email_list):
        email = email_data["email"]
        if i == 0 and not emails['primary_email']:
            emails['primary_email'] = email
        elif i == 1 and not emails['secondary_email']:
            emails['secondary_email'] = email
        elif i == 2 and not emails['admin_email']:
            emails['admin_email'] = email
        elif i == 3 and not emails['careers_email']:
            emails['careers_email'] = email

    # Convert sources list to string
    if emails['email_sources']:
        emails['email_sources'] = "; ".join(set(str(s) for s in emails['email_sources'] if s))
    else:
        emails['email_sources'] = None

    return emails


def process_enrichment_with_findall(
    api_key: str,
    company: dict,
    generator: str,
    max_retries: int = 3,
    job_id: str = None,
    company_index: int = 0,
    total_companies: int = 0
) -> dict:
    """Process a single company using FindAll API with retry logic"""

    progress_prefix = f"[{company_index}/{total_companies}]"

    for attempt in range(max_retries):
        try:
            log_message(job_id, f"{progress_prefix} Attempt {attempt + 1}/{max_retries} for: {company['company_name']}")
            log_message(job_id, f"{progress_prefix} Location: {company.get('city', 'N/A')}, {company.get('state', 'N/A')}")

            # Create FindAll run
            findall_id = create_findall_run(
                api_key=api_key,
                company=company,
                generator=generator,
                job_id=job_id,
                progress_prefix=progress_prefix
            )

            # Poll for completion with extended timeout
            start_time = time.time()
            max_wait = 300  # 5 minutes max per company (FindAll can take longer)
            poll_count = 0

            while time.time() - start_time < max_wait:
                poll_count += 1
                elapsed = int(time.time() - start_time)

                try:
                    status_result = poll_findall_status(api_key, findall_id)
                    status = status_result.get("status", "unknown")

                    if status == "completed":
                        # Fetch results
                        results = get_findall_results(api_key, findall_id)

                        # Extract emails from results
                        extracted = extract_emails_from_findall_results(results)

                        # Count emails found
                        email_fields = ['primary_email', 'secondary_email', 'admin_email', 'careers_email']
                        emails = [extracted.get(f) for f in email_fields if extracted.get(f)]

                        # Determine status based on whether emails were found
                        if len(emails) > 0:
                            result_status = 'success'
                            log_message(job_id, f"{progress_prefix} ✓ SUCCESS: Found {len(emails)} email(s) in {elapsed}s")
                            for field in email_fields:
                                if extracted.get(field):
                                    log_message(job_id, f"{progress_prefix}   - {field}: {extracted.get(field)}")
                        else:
                            result_status = 'no_email'
                            log_message(job_id, f"{progress_prefix} ⚠ NO EMAIL: Search completed but no emails found in {elapsed}s")

                        if extracted.get('website'):
                            log_message(job_id, f"{progress_prefix}   - website: {extracted.get('website')}")

                        return {
                            'status': result_status,
                            'company_name': company['company_name'],
                            'city': company.get('city'),
                            'state': company.get('state'),
                            'primary_email': extracted.get('primary_email'),
                            'secondary_email': extracted.get('secondary_email'),
                            'admin_email': extracted.get('admin_email'),
                            'careers_email': extracted.get('careers_email'),
                            'website': extracted.get('website'),
                            'email_sources': extracted.get('email_sources'),
                            'confidence': 'findall',
                            'run_id': findall_id,
                            'emails_found': len(emails),
                            'cost': COST_PER_RUN.get(generator, 0.10)
                        }

                    elif status == "failed":
                        error_msg = status_result.get("error", "Unknown error")
                        raise Exception(f"FindAll run failed: {error_msg}")

                    elif status in ["running", "pending", "processing"]:
                        if poll_count % 6 == 0:  # Log every 30 seconds
                            matches_so_far = status_result.get("matches_found", 0)
                            log_message(job_id, f"{progress_prefix} ⏳ Still searching... ({elapsed}s elapsed, {matches_so_far} candidates found)")
                        time.sleep(5)
                    else:
                        log_message(job_id, f"{progress_prefix} Unknown status: {status}")
                        time.sleep(5)

                except requests.exceptions.Timeout:
                    log_message(job_id, f"{progress_prefix} ⏳ Poll timeout, retrying... ({elapsed}s elapsed)")
                    time.sleep(5)
                except requests.exceptions.RequestException as e:
                    log_message(job_id, f"{progress_prefix} ⚠ Network error: {str(e)}", "WARN")
                    time.sleep(10)

            # Timeout
            raise Exception(f"Timeout after {max_wait}s waiting for FindAll results")

        except Exception as e:
            log_message(job_id, f"{progress_prefix} ⚠ Attempt {attempt + 1} failed: {str(e)}", "WARN")

            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 15  # Longer backoff for FindAll
                log_message(job_id, f"{progress_prefix} Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log_message(job_id, f"{progress_prefix} ✗ FAILED: All {max_retries} retries exhausted for {company['company_name']}", "ERROR")
                return {
                    'status': 'failed',
                    'company_name': company['company_name'],
                    'city': company.get('city'),
                    'state': company.get('state'),
                    'error': str(e),
                    'emails_found': 0,
                    'cost': COST_PER_RUN.get(generator, 0.10)  # Still charged for attempt
                }

    return {'status': 'failed', 'company_name': company['company_name'], 'emails_found': 0, 'cost': 0}


def run_enrichment_job(job_id: str, companies: list, processor: str, api_key: str, start_index: int = 0):
    """Background job to process all companies with incremental saves"""
    
    try:
        JOBS[job_id]['status'] = 'running'
        JOBS[job_id]['start_time'] = datetime.now().isoformat()
        
        total = len(companies)
        log_message(job_id, "=" * 60)
        log_message(job_id, f"🚀 STARTING ENRICHMENT JOB")
        log_message(job_id, f"=" * 60)
        log_message(job_id, f"📊 Total companies: {total}")
        log_message(job_id, f"⚙️  Processor: {processor}")
        log_message(job_id, f"💰 Estimated cost: ${total * COST_PER_RUN.get(processor, 0.05):.2f}")
        log_message(job_id, f"💾 Auto-save: Every {SAVE_EVERY_N} companies")
        
        if start_index > 0:
            log_message(job_id, f"🔄 RESUMING from company #{start_index + 1}")
        
        log_message(job_id, f"🔍 Using FindAll API for email discovery")
        log_message(job_id, "=" * 60)

        # Load existing results if resuming
        results = load_existing_results(job_id)
        total_emails = sum(r.get('emails_found', 0) for r in results)
        actual_cost = sum(r.get('cost', 0) for r in results)
        
        # Track success/failure/no_email counts
        success_count = len([r for r in results if r.get('status') == 'success'])
        no_email_count = len([r for r in results if r.get('status') == 'no_email'])
        fail_count = len([r for r in results if r.get('status') == 'failed'])
        
        # Process remaining companies
        for idx in range(start_index, total):
            company = companies[idx]
            company_num = idx + 1
            
            log_message(job_id, "")
            log_message(job_id, f"{'─' * 50}")
            log_message(job_id, f"📍 COMPANY {company_num}/{total}: {company['company_name']}")
            log_message(job_id, f"{'─' * 50}")
            
            result = process_enrichment_with_findall(
                api_key=api_key,
                company=company,
                generator=processor,  # FindAll uses 'generator' instead of 'processor'
                max_retries=3,
                job_id=job_id,
                company_index=company_num,
                total_companies=total
            )
            
            results.append(result)
            total_emails += result.get('emails_found', 0)
            actual_cost += result.get('cost', 0)
            
            # Update counts based on status
            if result.get('status') == 'success':
                success_count += 1
            elif result.get('status') == 'no_email':
                no_email_count += 1
            else:
                fail_count += 1
            
            # SAVE TO DATABASE IMMEDIATELY (every result)
            save_result_to_db(job_id, result)
            
            # Update job status in memory
            JOBS[job_id]['processed_rows'] = company_num
            JOBS[job_id]['emails_found'] = total_emails
            JOBS[job_id]['actual_cost'] = actual_cost
            JOBS[job_id]['success_count'] = success_count
            JOBS[job_id]['no_email_count'] = no_email_count
            JOBS[job_id]['fail_count'] = fail_count
            
            # Update job status in database
            update_job_status_db(job_id, {
                'processed_rows': company_num,
                'emails_found': total_emails,
                'actual_cost': actual_cost,
                'success_count': success_count,
                'fail_count': fail_count
            })
            
            # INCREMENTAL SAVE to files - Save every N companies
            if company_num % SAVE_EVERY_N == 0:
                save_incremental_results(job_id, results)
                log_message(job_id, f"💾 AUTO-SAVED: {company_num}/{total} companies processed")
                log_message(job_id, f"   ✓ With Email: {success_count} | ⚠ No Email: {no_email_count} | ✗ Failed: {fail_count} | 📧 Total: {total_emails}")
                log_message(job_id, f"   📊 Database: Results persisted to PostgreSQL")
            
            # Progress summary every 10 companies
            if company_num % 10 == 0:
                elapsed = (datetime.now() - datetime.fromisoformat(JOBS[job_id]['start_time'])).total_seconds()
                rate = company_num / elapsed if elapsed > 0 else 0
                remaining = total - company_num
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                log_message(job_id, "")
                log_message(job_id, f"📈 PROGRESS SUMMARY ({company_num}/{total} - {company_num*100//total}%)")
                log_message(job_id, f"   ⏱️  Elapsed: {int(elapsed//60)}m {int(elapsed%60)}s")
                log_message(job_id, f"   🚀 Rate: {rate:.1f} companies/min")
                log_message(job_id, f"   ⏳ ETA: ~{int(eta_minutes)}m remaining")
                log_message(job_id, f"   💰 Cost so far: ${actual_cost:.2f}")
            
            # Rate limiting - small delay between requests
            time.sleep(1)
        
        # Final save
        json_file, csv_file = save_incremental_results(job_id, results)
        
        JOBS[job_id]['status'] = 'completed'
        JOBS[job_id]['end_time'] = datetime.now().isoformat()
        
        # Update final status in database
        update_job_status_db(job_id, {
            'status': 'completed',
            'end_time': datetime.now()
        })
        
        elapsed = (datetime.fromisoformat(JOBS[job_id]['end_time']) -
                   datetime.fromisoformat(JOBS[job_id]['start_time'])).total_seconds()
        
        log_message(job_id, "")
        log_message(job_id, "=" * 60)
        log_message(job_id, f"🎉 JOB COMPLETED!")
        log_message(job_id, "=" * 60)
        log_message(job_id, f"📊 Total companies: {total}")
        log_message(job_id, f"✅ Found emails: {success_count}")
        log_message(job_id, f"⚠️  No emails: {no_email_count}")
        log_message(job_id, f"❌ Failed: {fail_count}")
        log_message(job_id, f"📧 Total emails found: {total_emails}")
        log_message(job_id, f"💰 Total cost: ${actual_cost:.2f}")
        log_message(job_id, f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        log_message(job_id, f"💾 Results saved: {json_file}")
        log_message(job_id, "=" * 60)
        
    except Exception as e:
        # Save whatever we have before marking as failed
        if 'results' in dir() and results:
            save_incremental_results(job_id, results)
            log_message(job_id, f"💾 EMERGENCY SAVE: Saved {len(results)} results before failure")
        
        JOBS[job_id]['status'] = 'failed'
        JOBS[job_id]['error'] = str(e)
        JOBS[job_id]['end_time'] = datetime.now().isoformat()
        
        # Update failure status in database
        update_job_status_db(job_id, {
            'status': 'failed',
            'error': str(e),
            'end_time': datetime.now()
        })
        
        log_message(job_id, f"❌ JOB FAILED: {str(e)}", "ERROR")
        log_message(job_id, f"💡 TIP: Your partial results have been saved to PostgreSQL. Download them from the results link.")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main UI"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Email Enrichment Tool (FindAll API)</title>
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
    <h1>Email Enrichment Tool (FindAll API)</h1>
    <p style="color: #666; margin-top: -10px;">Powered by Parallel FindAll - Entity Discovery at Web Scale</p>

    <div class="card">
        <h2>Upload CSV</h2>
        <div class="form-group">
            <label>CSV File (with columns: company_name, address, city, state, zip, type, number, phone)</label>
            <input type="file" id="csvFile" accept=".csv">
        </div>
        <div class="form-group">
            <label>FindAll Generator</label>
            <select id="processor">
                <option value="base">Base (~$0.06/company) - 30% recall, fastest</option>
                <option value="core" selected>Core (~$0.23/company) - 53% recall, balanced</option>
                <option value="pro">Pro (~$1.43/company) - 61% recall, highest accuracy</option>
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
                <div class="stat-value" id="statSuccess" style="color: #28a745;">0</div>
                <div class="stat-label">✓ Success</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statFailed" style="color: #dc3545;">0</div>
                <div class="stat-label">✗ Failed</div>
            </div>
        </div>
        <div class="status" style="margin-top: -5px;">
            <div class="stat">
                <div class="stat-value" id="statCost">$0.00</div>
                <div class="stat-label">Cost</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statStatus">-</div>
                <div class="stat-label">Status</div>
            </div>
            <div class="stat" style="grid-column: span 2;">
                <div class="stat-value" id="statETA">-</div>
                <div class="stat-label">Estimated Time</div>
            </div>
        </div>
        <div id="downloadLinks" style="margin-bottom: 15px; padding: 10px; background: #e8f5e9; border-radius: 4px;">
            <strong>📥 Download Results:</strong>
            <a href="#" id="jsonLink" target="_blank">JSON</a> |
            <a href="#" id="csvLink" target="_blank">CSV</a>
            <span id="partialWarning" style="color: #ff9800; margin-left: 10px;"></span>
        </div>
        <h3>Live Logs</h3>
        <div class="logs" id="logs"></div>
    </div>

    <script>
        let eventSource = null;
        let currentJobId = null;
        
        // FindAll pricing per entity (estimated from CPM)
        const FINDALL_COSTS = { base: 0.06, core: 0.23, pro: 1.43 };

        function getCost(generator) {
            return FINDALL_COSTS[generator] || 0.23;
        }

        document.getElementById('csvFile').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                const reader = new FileReader();
                reader.onload = function(e) {
                    const lines = e.target.result.split('\\n').filter(l => l.trim());
                    const rowCount = lines.length;
                    const generator = document.getElementById('processor').value;
                    const cost = getCost(generator);

                    document.getElementById('rowCount').textContent = rowCount;
                    document.getElementById('estimatedCost').textContent = '$' + (rowCount * cost).toFixed(2);
                    document.getElementById('costEstimate').style.display = 'block';
                };
                reader.readAsText(file);
            }
        });

        document.getElementById('processor').addEventListener('change', function() {
            const rowCount = parseInt(document.getElementById('rowCount').textContent) || 0;
            const cost = getCost(this.value);
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
        
        let pollStartTime = null;
        
        function pollStatus(jobId) {
            if (!pollStartTime) pollStartTime = Date.now();
            
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
                    document.getElementById('statSuccess').textContent = status.success_count || 0;
                    document.getElementById('statFailed').textContent = status.fail_count || 0;
                    document.getElementById('statCost').textContent = '$' + status.actual_cost.toFixed(2);
                    document.getElementById('statStatus').textContent = status.status.toUpperCase();
                    
                    // Calculate ETA
                    if (status.processed_rows > 0 && status.status === 'running') {
                        const elapsed = (Date.now() - pollStartTime) / 1000;
                        const rate = status.processed_rows / elapsed;
                        const remaining = status.total_rows - status.processed_rows;
                        const etaSeconds = remaining / rate;
                        const etaMin = Math.floor(etaSeconds / 60);
                        const etaSec = Math.floor(etaSeconds % 60);
                        document.getElementById('statETA').textContent = etaMin + 'm ' + etaSec + 's';
                    } else if (status.status === 'completed') {
                        document.getElementById('statETA').textContent = 'Done!';
                    } else if (status.status === 'failed') {
                        document.getElementById('statETA').textContent = 'Failed';
                    }
                    
                    // Show download links after first save (5 companies) or on completion/failure
                    if (status.processed_rows >= 5 || status.status === 'completed' || status.status === 'failed') {
                        document.getElementById('downloadLinks').style.display = 'block';
                        document.getElementById('jsonLink').href = '/results/' + jobId + '.json';
                        document.getElementById('csvLink').href = '/results/' + jobId + '.csv';
                        
                        if (status.status === 'running') {
                            document.getElementById('partialWarning').textContent = '⚠️ Partial results (job still running)';
                            document.getElementById('downloadLinks').style.background = '#fff3cd';
                        } else if (status.status === 'failed') {
                            document.getElementById('partialWarning').textContent = '⚠️ Partial results (job failed)';
                            document.getElementById('downloadLinks').style.background = '#ffebee';
                        } else {
                            document.getElementById('partialWarning').textContent = '✓ Complete results';
                            document.getElementById('downloadLinks').style.background = '#e8f5e9';
                        }
                    }
                    
                    if (status.status === 'completed' || status.status === 'failed') {
                        document.getElementById('startBtn').disabled = false;
                        if (eventSource) eventSource.close();
                        pollStartTime = null;
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
    processor: str = Form("base"),
    api_key: str = Form(...)
):
    """Upload CSV and start enrichment job"""
    
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
    
    job_data = {
        'job_id': job_id,
        'status': 'pending',
        'total_rows': len(companies),
        'processed_rows': 0,
        'emails_found': 0,
        'estimated_cost': estimated_cost,
        'actual_cost': 0.0,
        'start_time': None,
        'end_time': None,
        'error': None,
        'success_count': 0,
        'no_email_count': 0,
        'fail_count': 0,
        'processor': processor
    }
    
    JOBS[job_id] = job_data
    
    # Save job to database
    create_job_db(job_id, job_data)
    
    # Initialize log queue
    LOG_QUEUES[job_id] = Queue()
    
    # Start background job
    thread = threading.Thread(
        target=run_enrichment_job,
        args=(job_id, companies, processor, api_key, 0)
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
    """Download results as JSON - from database or file"""
    # Try database first
    results = get_results_from_db(job_id)
    
    # Fall back to file
    if not results:
        results_file = RESULTS_DIR / f"{job_id}_results.json"
        if not results_file.exists():
            raise HTTPException(status_code=404, detail="Results not found yet. Processing may still be in progress.")
        
        with open(results_file, 'r') as f:
            results = json.load(f)
    
    # Get job info from memory or database
    job_info = JOBS.get(job_id) or get_job_from_db(job_id) or {}
    
    response = {
        "job_status": job_info.get('status', 'unknown'),
        "processed": job_info.get('processed_rows', len(results)),
        "total": job_info.get('total_rows', len(results)),
        "emails_found": job_info.get('emails_found', 0),
        "source": "postgresql" if get_results_from_db(job_id) else "file",
        "results": results
    }
    
    return JSONResponse(content=response)


@app.get("/results/{job_id}.csv")
async def download_csv_results(job_id: str):
    """Download results as CSV - from database or file"""
    # Try database first
    results = get_results_from_db(job_id)
    
    if results:
        # Generate CSV from database results
        import io
        output = io.StringIO()
        fieldnames = ['status', 'company_name', 'city', 'state', 'primary_email',
                     'secondary_email', 'admin_email', 'careers_email', 'website',
                     'email_sources', 'confidence', 'run_id', 'emails_found', 'cost', 'error']
        writer = csv.DictWriter(output, fieldnames=[k for k in fieldnames if any(k in r for r in results)])
        writer.writeheader()
        writer.writerows(results)
        content = output.getvalue()
    else:
        # Fall back to file
        results_file = RESULTS_DIR / f"{job_id}_results.csv"
        if not results_file.exists():
            raise HTTPException(status_code=404, detail="Results not found yet. Processing may still be in progress.")
        
        with open(results_file, 'r') as f:
            content = f.read()
    
    job_info = JOBS.get(job_id) or get_job_from_db(job_id) or {}
    status_suffix = "_partial" if job_info.get('status') == 'running' else ""
    
    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=enrichment_results_{job_id[:8]}{status_suffix}.csv"}
    )


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    return {"status": "healthy", "jobs_count": len(JOBS)}


@app.get("/favicon.ico")
async def favicon():
    """Return empty favicon to prevent 404"""
    return JSONResponse(content={}, status_code=204)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
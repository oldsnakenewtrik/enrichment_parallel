"""
FindAll Discovery & Enrichment Tool
- Discover entities using natural language objectives
- Automatically enrich discovered entities with emails, phones, websites
- Uses Parallel FindAll API for web-scale entity discovery
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

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# For direct API calls to Parallel FindAll
import requests

# Database
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

app = FastAPI(title="FindAll Discovery API", version="2.0.0")

# Database connection
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    """Get database connection, returns None if no DATABASE_URL"""
    if not DATABASE_URL:
        return None
    try:
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
    """Initialize database tables for FindAll results"""
    conn = get_db_connection()
    if conn is None:
        print("No DATABASE_URL configured - using in-memory storage only")
        return

    try:
        cur = conn.cursor()

        # Create jobs table for FindAll runs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS findall_jobs (
                job_id VARCHAR(36) PRIMARY KEY,
                findall_id VARCHAR(100),
                objective TEXT,
                entity_type VARCHAR(100),
                generator VARCHAR(20) DEFAULT 'core',
                match_limit INTEGER DEFAULT 100,
                status VARCHAR(20) DEFAULT 'pending',
                entities_found INTEGER DEFAULT 0,
                entities_with_email INTEGER DEFAULT 0,
                entities_with_phone INTEGER DEFAULT 0,
                estimated_cost DECIMAL(10,2) DEFAULT 0,
                actual_cost DECIMAL(10,2) DEFAULT 0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create results table for discovered entities
        cur.execute("""
            CREATE TABLE IF NOT EXISTS findall_results (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(36) REFERENCES findall_jobs(job_id),
                entity_name VARCHAR(500),
                entity_type VARCHAR(100),
                address VARCHAR(500),
                city VARCHAR(200),
                state VARCHAR(100),
                country VARCHAR(100),
                primary_email VARCHAR(500),
                secondary_email VARCHAR(500),
                phone VARCHAR(100),
                secondary_phone VARCHAR(100),
                website VARCHAR(500),
                description TEXT,
                source_url TEXT,
                confidence VARCHAR(50),
                match_reasoning TEXT,
                raw_enrichment JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_findall_results_job_id ON findall_results(job_id)
        """)

        conn.commit()
        print("FindAll database tables initialized successfully")
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
RESULTS_DIR.mkdir(exist_ok=True)

# FindAll API configuration
FINDALL_API_BASE = "https://api.parallel.ai/v1beta/findall"
FINDALL_BETA_HEADER = "findall-2025-09-15"

# Cost tracking - FindAll CPM (cost per 1000 matches)
COST_PER_MATCH = {
    "base": 0.06,    # $60 CPM
    "core": 0.23,    # $230 CPM
    "pro": 1.43,     # $1430 CPM
}


def log_message(job_id: str, message: str, level: str = "INFO"):
    """Add a log message to the job's log queue"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"

    if job_id in LOG_QUEUES:
        LOG_QUEUES[job_id].put(log_entry)

    print(f"[{job_id[:8]}] {log_entry}")


def get_findall_headers(api_key: str) -> dict:
    """Get headers for FindAll API requests"""
    return {
        "x-api-key": api_key,
        "parallel-beta": FINDALL_BETA_HEADER,
        "Content-Type": "application/json"
    }


def create_findall_discovery_run(
    api_key: str,
    objective: str,
    entity_type: str,
    generator: str,
    match_limit: int,
    job_id: str = None
) -> str:
    """Create a FindAll discovery run with enrichment"""

    # Match conditions based on entity type
    match_conditions = [
        {
            "name": "entity_exists_check",
            "description": f"Must be a real, currently operating {entity_type}."
        },
        {
            "name": "relevance_check",
            "description": f"Must match the search criteria specified in the objective."
        }
    ]

    # Enrichment schema to get contact information for each discovered entity
    enrichment_schema = {
        "type": "object",
        "properties": {
            "entity_name": {
                "type": "string",
                "description": "Full official name of the business/organization"
            },
            "address": {
                "type": "string",
                "description": "Full street address"
            },
            "city": {
                "type": "string",
                "description": "City name"
            },
            "state": {
                "type": "string",
                "description": "State or province"
            },
            "country": {
                "type": "string",
                "description": "Country name"
            },
            "primary_email": {
                "type": "string",
                "description": "Main contact email address (info@, contact@, etc.)"
            },
            "secondary_email": {
                "type": "string",
                "description": "Secondary email (admissions@, sales@, support@, etc.)"
            },
            "phone": {
                "type": "string",
                "description": "Primary phone number"
            },
            "secondary_phone": {
                "type": "string",
                "description": "Secondary or alternate phone number"
            },
            "website": {
                "type": "string",
                "description": "Official website URL"
            },
            "description": {
                "type": "string",
                "description": "Brief description of the business/organization"
            }
        }
    }

    payload = {
        "objective": objective,
        "entity_type": entity_type,
        "match_conditions": match_conditions,
        "generator": generator,
        "match_limit": match_limit,
        "enrichment_schema": enrichment_schema
    }

    headers = get_findall_headers(api_key)

    log_message(job_id, f"Creating FindAll discovery run...")
    log_message(job_id, f"Objective: {objective}")
    log_message(job_id, f"Entity type: {entity_type}")
    log_message(job_id, f"Generator: {generator}")
    log_message(job_id, f"Match limit: {match_limit}")

    response = requests.post(
        f"{FINDALL_API_BASE}/runs",
        headers=headers,
        json=payload,
        timeout=60
    )

    if response.status_code not in [200, 201]:
        raise Exception(f"FindAll API error: {response.status_code} - {response.text}")

    result = response.json()
    findall_id = result.get("findall_id") or result.get("id") or result.get("run_id")

    if not findall_id:
        raise Exception(f"No findall_id in response: {result}")

    log_message(job_id, f"FindAll Run ID: {findall_id}")
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
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"Results fetch error: {response.status_code} - {response.text}")

    return response.json()


def extract_entities_from_results(results: dict) -> List[dict]:
    """Extract and normalize entities from FindAll results"""
    entities = []

    matches = results.get("matches", []) or results.get("results", []) or []

    for match in matches:
        if not isinstance(match, dict):
            continue

        entity = {
            'entity_name': None,
            'address': None,
            'city': None,
            'state': None,
            'country': None,
            'primary_email': None,
            'secondary_email': None,
            'phone': None,
            'secondary_phone': None,
            'website': None,
            'description': None,
            'source_url': None,
            'confidence': None,
            'match_reasoning': None,
            'raw_enrichment': {}
        }

        # Get the entity name from various possible fields
        entity['entity_name'] = (
            match.get("name") or
            match.get("entity") or
            match.get("entity_name") or
            match.get("value") or
            match.get("title")
        )

        # Get match metadata
        entity['confidence'] = match.get("confidence") or match.get("score")
        entity['match_reasoning'] = match.get("reasoning") or match.get("explanation")
        entity['source_url'] = match.get("source") or match.get("url") or match.get("citation")

        # Get enrichment data
        enrichment = match.get("enrichment", {}) or {}
        entity['raw_enrichment'] = enrichment

        # Extract enriched fields
        if enrichment:
            entity['entity_name'] = enrichment.get("entity_name") or entity['entity_name']
            entity['address'] = enrichment.get("address")
            entity['city'] = enrichment.get("city")
            entity['state'] = enrichment.get("state")
            entity['country'] = enrichment.get("country")
            entity['primary_email'] = enrichment.get("primary_email")
            entity['secondary_email'] = enrichment.get("secondary_email")
            entity['phone'] = enrichment.get("phone")
            entity['secondary_phone'] = enrichment.get("secondary_phone")
            entity['website'] = enrichment.get("website")
            entity['description'] = enrichment.get("description")

        if entity['entity_name']:
            entities.append(entity)

    return entities


def save_entity_to_db(job_id: str, entity: dict):
    """Save a single entity to the database"""
    with db_cursor() as cur:
        if cur is None:
            return

        cur.execute("""
            INSERT INTO findall_results (
                job_id, entity_name, entity_type, address, city, state, country,
                primary_email, secondary_email, phone, secondary_phone,
                website, description, source_url, confidence, match_reasoning, raw_enrichment
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            job_id,
            entity.get('entity_name'),
            entity.get('entity_type'),
            entity.get('address'),
            entity.get('city'),
            entity.get('state'),
            entity.get('country'),
            entity.get('primary_email'),
            entity.get('secondary_email'),
            entity.get('phone'),
            entity.get('secondary_phone'),
            entity.get('website'),
            entity.get('description'),
            entity.get('source_url'),
            entity.get('confidence'),
            entity.get('match_reasoning'),
            json.dumps(entity.get('raw_enrichment', {}))
        ))


def create_job_db(job_id: str, job_data: dict):
    """Create a new FindAll job in the database"""
    with db_cursor() as cur:
        if cur is None:
            return

        cur.execute("""
            INSERT INTO findall_jobs (
                job_id, objective, entity_type, generator, match_limit,
                status, estimated_cost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            job_id,
            job_data.get('objective'),
            job_data.get('entity_type'),
            job_data.get('generator'),
            job_data.get('match_limit'),
            job_data.get('status', 'pending'),
            job_data.get('estimated_cost', 0)
        ))


def update_job_db(job_id: str, updates: dict):
    """Update job in database"""
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
            UPDATE findall_jobs SET {', '.join(set_clauses)} WHERE job_id = %s
        """, values)


def get_results_from_db(job_id: str) -> list:
    """Get all results for a job from database"""
    with db_cursor() as cur:
        if cur is None:
            return []

        cur.execute("""
            SELECT entity_name, address, city, state, country,
                   primary_email, secondary_email, phone, secondary_phone,
                   website, description, source_url, confidence, match_reasoning
            FROM findall_results WHERE job_id = %s ORDER BY id
        """, (job_id,))
        return [dict(row) for row in cur.fetchall()]


def save_results_to_file(job_id: str, entities: list, job_data: dict):
    """Save results to JSON and CSV files"""
    # Save JSON
    json_file = RESULTS_DIR / f"{job_id}_results.json"
    with open(json_file, 'w') as f:
        json.dump({
            "job_id": job_id,
            "objective": job_data.get('objective'),
            "entity_type": job_data.get('entity_type'),
            "generator": job_data.get('generator'),
            "total_entities": len(entities),
            "entities": entities
        }, f, indent=2)

    # Save CSV
    csv_file = RESULTS_DIR / f"{job_id}_results.csv"
    if entities:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['entity_name', 'address', 'city', 'state', 'country',
                         'primary_email', 'secondary_email', 'phone', 'secondary_phone',
                         'website', 'description', 'source_url']
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(entities)

    return json_file, csv_file


def run_findall_job(job_id: str, objective: str, entity_type: str,
                    generator: str, match_limit: int, api_key: str):
    """Background job to run FindAll discovery"""

    try:
        JOBS[job_id]['status'] = 'running'
        JOBS[job_id]['start_time'] = datetime.now().isoformat()

        log_message(job_id, "=" * 60)
        log_message(job_id, f"🚀 STARTING FINDALL DISCOVERY")
        log_message(job_id, "=" * 60)
        log_message(job_id, f"🎯 Objective: {objective}")
        log_message(job_id, f"📦 Entity type: {entity_type}")
        log_message(job_id, f"⚙️  Generator: {generator}")
        log_message(job_id, f"🔢 Match limit: {match_limit}")
        log_message(job_id, f"💰 Estimated max cost: ${match_limit * COST_PER_MATCH.get(generator, 0.23):.2f}")
        log_message(job_id, "=" * 60)

        # Create the FindAll run
        findall_id = create_findall_discovery_run(
            api_key=api_key,
            objective=objective,
            entity_type=entity_type,
            generator=generator,
            match_limit=match_limit,
            job_id=job_id
        )

        JOBS[job_id]['findall_id'] = findall_id
        update_job_db(job_id, {'findall_id': findall_id, 'status': 'running'})

        # Poll for completion
        log_message(job_id, "")
        log_message(job_id, "⏳ Discovering entities... (this may take several minutes)")

        start_time = time.time()
        max_wait = 900  # 15 minutes max for discovery
        poll_count = 0

        while time.time() - start_time < max_wait:
            poll_count += 1
            elapsed = int(time.time() - start_time)

            try:
                status_result = poll_findall_status(api_key, findall_id)
                status = status_result.get("status", "unknown")

                # Log progress periodically
                if poll_count % 6 == 0:  # Every 30 seconds
                    candidates = status_result.get("candidates_found", 0) or status_result.get("matches_found", 0)
                    evaluated = status_result.get("candidates_evaluated", 0)
                    matched = status_result.get("matches", 0) or status_result.get("matched", 0)
                    log_message(job_id, f"⏳ Progress: {elapsed}s elapsed | Candidates: {candidates} | Evaluated: {evaluated} | Matched: {matched}")

                if status == "completed":
                    log_message(job_id, f"✓ Discovery completed in {elapsed}s")
                    break
                elif status == "failed":
                    error_msg = status_result.get("error", "Unknown error")
                    raise Exception(f"FindAll run failed: {error_msg}")
                elif status not in ["running", "pending", "processing"]:
                    log_message(job_id, f"Unknown status: {status}")

                time.sleep(5)

            except requests.exceptions.RequestException as e:
                log_message(job_id, f"⚠ Network error during polling: {str(e)}", "WARN")
                time.sleep(10)

        else:
            raise Exception(f"Timeout after {max_wait}s waiting for FindAll results")

        # Fetch results
        log_message(job_id, "")
        log_message(job_id, "📥 Fetching discovered entities...")

        results = get_findall_results(api_key, findall_id)
        entities = extract_entities_from_results(results)

        log_message(job_id, f"📊 Found {len(entities)} entities")

        # Count entities with contact info
        with_email = sum(1 for e in entities if e.get('primary_email') or e.get('secondary_email'))
        with_phone = sum(1 for e in entities if e.get('phone') or e.get('secondary_phone'))
        with_website = sum(1 for e in entities if e.get('website'))

        log_message(job_id, f"📧 With email: {with_email}")
        log_message(job_id, f"📞 With phone: {with_phone}")
        log_message(job_id, f"🌐 With website: {with_website}")

        # Save to database
        log_message(job_id, "")
        log_message(job_id, "💾 Saving results to database...")

        for i, entity in enumerate(entities):
            entity['entity_type'] = entity_type
            save_entity_to_db(job_id, entity)

            if (i + 1) % 10 == 0:
                log_message(job_id, f"   Saved {i + 1}/{len(entities)} entities")

        # Save to files
        json_file, csv_file = save_results_to_file(job_id, entities, JOBS[job_id])
        log_message(job_id, f"💾 Results saved to files")

        # Calculate actual cost
        actual_cost = len(entities) * COST_PER_MATCH.get(generator, 0.23)

        # Update job status
        JOBS[job_id]['status'] = 'completed'
        JOBS[job_id]['end_time'] = datetime.now().isoformat()
        JOBS[job_id]['entities_found'] = len(entities)
        JOBS[job_id]['entities_with_email'] = with_email
        JOBS[job_id]['entities_with_phone'] = with_phone
        JOBS[job_id]['actual_cost'] = actual_cost

        update_job_db(job_id, {
            'status': 'completed',
            'end_time': datetime.now(),
            'entities_found': len(entities),
            'entities_with_email': with_email,
            'entities_with_phone': with_phone,
            'actual_cost': actual_cost
        })

        # Final summary
        elapsed_total = (datetime.fromisoformat(JOBS[job_id]['end_time']) -
                        datetime.fromisoformat(JOBS[job_id]['start_time'])).total_seconds()

        log_message(job_id, "")
        log_message(job_id, "=" * 60)
        log_message(job_id, f"🎉 FINDALL DISCOVERY COMPLETED!")
        log_message(job_id, "=" * 60)
        log_message(job_id, f"📊 Total entities discovered: {len(entities)}")
        log_message(job_id, f"📧 With email addresses: {with_email}")
        log_message(job_id, f"📞 With phone numbers: {with_phone}")
        log_message(job_id, f"🌐 With websites: {with_website}")
        log_message(job_id, f"💰 Estimated cost: ${actual_cost:.2f}")
        log_message(job_id, f"⏱️  Total time: {int(elapsed_total//60)}m {int(elapsed_total%60)}s")
        log_message(job_id, "=" * 60)

        # Log first few entities as preview
        if entities:
            log_message(job_id, "")
            log_message(job_id, "📋 Preview of discovered entities:")
            for i, entity in enumerate(entities[:5]):
                log_message(job_id, f"   {i+1}. {entity.get('entity_name', 'Unknown')}")
                if entity.get('primary_email'):
                    log_message(job_id, f"      📧 {entity['primary_email']}")
                if entity.get('phone'):
                    log_message(job_id, f"      📞 {entity['phone']}")
                if entity.get('website'):
                    log_message(job_id, f"      🌐 {entity['website']}")
            if len(entities) > 5:
                log_message(job_id, f"   ... and {len(entities) - 5} more")

    except Exception as e:
        JOBS[job_id]['status'] = 'failed'
        JOBS[job_id]['error'] = str(e)
        JOBS[job_id]['end_time'] = datetime.now().isoformat()

        update_job_db(job_id, {
            'status': 'failed',
            'error': str(e),
            'end_time': datetime.now()
        })

        log_message(job_id, f"❌ JOB FAILED: {str(e)}", "ERROR")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main UI"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>FindAll Discovery Tool</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        h1 { color: #333; margin-bottom: 5px; }
        .subtitle { color: #666; margin-top: 0; margin-bottom: 20px; }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: 600; }
        input, select, textarea {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        textarea { resize: vertical; min-height: 80px; }
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
        .cost-estimate {
            background: #e8f5e9;
            border: 1px solid #4caf50;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 15px;
        }
        .example-objectives {
            background: #fff3cd;
            border: 1px solid #ffc107;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 15px;
        }
        .example-objectives ul { margin: 10px 0 0 0; padding-left: 20px; }
        .example-objectives li { margin: 5px 0; cursor: pointer; color: #0056b3; }
        .example-objectives li:hover { text-decoration: underline; }
        a { color: #007bff; }
        .form-row { display: flex; gap: 15px; }
        .form-row .form-group { flex: 1; }
    </style>
</head>
<body>
    <h1>FindAll Discovery Tool</h1>
    <p class="subtitle">Powered by Parallel FindAll - Discover entities and enrich with contact information</p>

    <div class="card">
        <h2>Search Configuration</h2>

        <div class="example-objectives">
            <strong>Example Objectives (click to use):</strong>
            <ul>
                <li onclick="setObjective('FindAll assisted living facilities in Tampa, Florida')">FindAll assisted living facilities in Tampa, Florida</li>
                <li onclick="setObjective('FindAll nursing homes in Los Angeles County, California')">FindAll nursing homes in Los Angeles County, California</li>
                <li onclick="setObjective('FindAll home health care agencies in Miami-Dade County')">FindAll home health care agencies in Miami-Dade County</li>
                <li onclick="setObjective('FindAll memory care facilities in Phoenix, Arizona')">FindAll memory care facilities in Phoenix, Arizona</li>
                <li onclick="setObjective('FindAll rehabilitation centers in Houston, Texas')">FindAll rehabilitation centers in Houston, Texas</li>
            </ul>
        </div>

        <div class="form-group">
            <label>Search Objective</label>
            <textarea id="objective" placeholder="Describe what you want to find. Example: FindAll assisted living facilities in Tampa, Florida"></textarea>
        </div>

        <div class="form-row">
            <div class="form-group">
                <label>Entity Type</label>
                <select id="entityType">
                    <option value="businesses">Businesses / Companies</option>
                    <option value="healthcare_facilities" selected>Healthcare Facilities</option>
                    <option value="senior_care_facilities">Senior Care Facilities</option>
                    <option value="organizations">Organizations</option>
                    <option value="schools">Schools / Educational</option>
                    <option value="restaurants">Restaurants</option>
                    <option value="hotels">Hotels / Lodging</option>
                    <option value="other">Other</option>
                </select>
            </div>
            <div class="form-group">
                <label>Generator</label>
                <select id="generator">
                    <option value="base">Base (~$0.06/entity) - 30% recall, fastest</option>
                    <option value="core" selected>Core (~$0.23/entity) - 53% recall, balanced</option>
                    <option value="pro">Pro (~$1.43/entity) - 61% recall, most thorough</option>
                </select>
            </div>
        </div>

        <div class="form-row">
            <div class="form-group">
                <label>Match Limit (max entities to find)</label>
                <input type="number" id="matchLimit" value="50" min="1" max="1000">
            </div>
            <div class="form-group">
                <label>Parallel API Key</label>
                <input type="password" id="apiKey" placeholder="Your Parallel API key">
            </div>
        </div>

        <div class="cost-estimate" id="costEstimate">
            <strong>Estimated Max Cost:</strong> <span id="estimatedCost">$11.50</span>
            <br><small>Based on <span id="matchLimitDisplay">50</span> entities at Core pricing. Actual cost depends on entities found.</small>
        </div>

        <button onclick="startDiscovery()" id="startBtn">Start Discovery</button>
    </div>

    <div class="card" id="statusCard" style="display:none;">
        <h2>Discovery Status</h2>
        <div class="status">
            <div class="stat">
                <div class="stat-value" id="statEntities">0</div>
                <div class="stat-label">Entities Found</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statEmails">0</div>
                <div class="stat-label">With Email</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statPhones">0</div>
                <div class="stat-label">With Phone</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="statStatus">-</div>
                <div class="stat-label">Status</div>
            </div>
        </div>
        <div id="downloadLinks" style="margin-bottom: 15px; padding: 10px; background: #e8f5e9; border-radius: 4px; display: none;">
            <strong>Download Results:</strong>
            <a href="#" id="jsonLink" target="_blank">JSON</a> |
            <a href="#" id="csvLink" target="_blank">CSV</a>
        </div>
        <h3>Live Logs</h3>
        <div class="logs" id="logs"></div>
    </div>

    <script>
        let eventSource = null;
        let currentJobId = null;

        const COSTS = { base: 0.06, core: 0.23, pro: 1.43 };

        function setObjective(text) {
            document.getElementById('objective').value = text;
        }

        function updateCostEstimate() {
            const matchLimit = parseInt(document.getElementById('matchLimit').value) || 50;
            const generator = document.getElementById('generator').value;
            const cost = matchLimit * (COSTS[generator] || 0.23);
            document.getElementById('estimatedCost').textContent = '$' + cost.toFixed(2);
            document.getElementById('matchLimitDisplay').textContent = matchLimit;
        }

        document.getElementById('matchLimit').addEventListener('input', updateCostEstimate);
        document.getElementById('generator').addEventListener('change', updateCostEstimate);

        async function startDiscovery() {
            const objective = document.getElementById('objective').value.trim();
            const entityType = document.getElementById('entityType').value;
            const generator = document.getElementById('generator').value;
            const matchLimit = parseInt(document.getElementById('matchLimit').value) || 50;
            const apiKey = document.getElementById('apiKey').value;

            if (!objective) {
                alert('Please enter a search objective');
                return;
            }
            if (!apiKey) {
                alert('Please enter your Parallel API key');
                return;
            }

            document.getElementById('startBtn').disabled = true;
            document.getElementById('statusCard').style.display = 'block';
            document.getElementById('logs').innerHTML = '';
            document.getElementById('downloadLinks').style.display = 'none';

            try {
                const formData = new URLSearchParams();
                formData.append('objective', objective);
                formData.append('entity_type', entityType);
                formData.append('generator', generator);
                formData.append('match_limit', matchLimit);
                formData.append('api_key', apiKey);

                const response = await fetch('/discover', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
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
            if (eventSource) eventSource.close();

            eventSource = new EventSource('/logs/' + jobId);
            eventSource.onmessage = function(e) {
                const logsDiv = document.getElementById('logs');
                logsDiv.innerHTML += e.data + '\\n';
                logsDiv.scrollTop = logsDiv.scrollHeight;
            };
        }

        function pollStatus(jobId) {
            const poll = async () => {
                try {
                    const response = await fetch('/status/' + jobId);
                    const status = await response.json();

                    document.getElementById('statEntities').textContent = status.entities_found || 0;
                    document.getElementById('statEmails').textContent = status.entities_with_email || 0;
                    document.getElementById('statPhones').textContent = status.entities_with_phone || 0;
                    document.getElementById('statStatus').textContent = (status.status || '-').toUpperCase();

                    if (status.status === 'completed' || status.status === 'failed') {
                        document.getElementById('startBtn').disabled = false;
                        if (eventSource) eventSource.close();

                        if (status.entities_found > 0) {
                            document.getElementById('downloadLinks').style.display = 'block';
                            document.getElementById('jsonLink').href = '/results/' + jobId + '.json';
                            document.getElementById('csvLink').href = '/results/' + jobId + '.csv';
                        }
                    } else {
                        setTimeout(poll, 3000);
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


@app.post("/discover")
async def start_discovery(
    objective: str = Form(...),
    entity_type: str = Form("businesses"),
    generator: str = Form("core"),
    match_limit: int = Form(50),
    api_key: str = Form(...)
):
    """Start a FindAll discovery job"""

    job_id = str(uuid.uuid4())

    estimated_cost = match_limit * COST_PER_MATCH.get(generator, 0.23)

    job_data = {
        'job_id': job_id,
        'objective': objective,
        'entity_type': entity_type,
        'generator': generator,
        'match_limit': match_limit,
        'status': 'pending',
        'entities_found': 0,
        'entities_with_email': 0,
        'entities_with_phone': 0,
        'estimated_cost': estimated_cost,
        'actual_cost': 0.0,
        'start_time': None,
        'end_time': None,
        'error': None
    }

    JOBS[job_id] = job_data
    create_job_db(job_id, job_data)

    LOG_QUEUES[job_id] = Queue()

    thread = threading.Thread(
        target=run_findall_job,
        args=(job_id, objective, entity_type, generator, match_limit, api_key)
    )
    thread.daemon = True
    thread.start()

    return {"job_id": job_id, "estimated_cost": estimated_cost}


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

            if JOBS[job_id]['status'] in ['completed', 'failed']:
                yield f"data: [END] Job {JOBS[job_id]['status']}\n\n"
                break

            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


@app.get("/results/{job_id}.json")
async def download_json_results(job_id: str):
    """Download results as JSON"""
    results_file = RESULTS_DIR / f"{job_id}_results.json"

    if results_file.exists():
        with open(results_file, 'r') as f:
            return JSONResponse(content=json.load(f))

    # Try database
    results = get_results_from_db(job_id)
    if results:
        return JSONResponse(content={
            "job_id": job_id,
            "entities": results
        })

    raise HTTPException(status_code=404, detail="Results not found")


@app.get("/results/{job_id}.csv")
async def download_csv_results(job_id: str):
    """Download results as CSV"""
    results_file = RESULTS_DIR / f"{job_id}_results.csv"

    if results_file.exists():
        with open(results_file, 'r') as f:
            content = f.read()
        return StreamingResponse(
            iter([content]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=findall_results_{job_id[:8]}.csv"}
        )

    raise HTTPException(status_code=404, detail="Results not found")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "jobs_count": len(JOBS)}


@app.get("/favicon.ico")
async def favicon():
    """Return empty favicon"""
    return JSONResponse(content={}, status_code=204)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

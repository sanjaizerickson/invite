"""
FastAPI Risk Analysis Service - EC2 Deployment
================================================
Exposes SNOMED hierarchy functions as REST endpoints for Lambda.

Lambda calls these endpoints using requests library directly.
All hierarchy/DB operations happen here with caching.

Endpoints:
- POST /scan_conditions - Scan coded conditions for high-risk matches
- POST /check_chronic - Check if condition is chronic
- GET /health - Health check
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any, Set
import time
import sys
import traceback
import uvicorn
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import ONLY the functions needed - no SNOMED API calls
from snomed_hierarchy_functions import (
    scan_coded_conditions_hierarchical,
    is_chronic_condition_snomed,
    get_expanded_snomed_hierarchy,
    setup_database_cache,
    categorize_coded_conditions
)

# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="APS Risk Analysis Service",
    description="SNOMED hierarchy and risk analysis endpoints for Lambda",
    version="1.0.0"
)

# Global state tracking
STARTUP_TIME = None
HIERARCHY_LOADED = False
HIERARCHY_SIZE = 0
REQUEST_COUNT = 0


# ============================================================================
# PYDANTIC MODELS - REQUEST/RESPONSE SCHEMAS
# ============================================================================

class CodedCondition(BaseModel):
    """Single coded condition from medical coding stage"""
    entity_text: str
    snomed_code: Optional[str] = None
    icd10_code: Optional[str] = None
    rxnorm_code: Optional[str] = None
    entity_type: Optional[str] = None
    source_page: Optional[int] = None
    bbox: Optional[List[float]] = None
    confidence_score: Optional[float] = None
    
    class Config:
        extra = "allow"  # Allow additional fields


class ScanConditionsRequest(BaseModel):
    """Request to scan coded conditions for high-risk matches"""
    coded_conditions: List[Dict[str, Any]] = Field(
        ...,
        description="List of coded conditions from medical coding stage"
    )


class ScanConditionsResponse(BaseModel):
    """Response with hierarchical risk findings AND categorized conditions"""
    hierarchical_risks: List[Dict[str, Any]]
    matched_codes: List[str]
    processing_time_ms: float
    total_conditions_scanned: int
    high_risk_count: int
    # Categorized buckets (flat lists)
    chronic_conditions: List[Dict[str, Any]]
    high_risk_conditions: List[Dict[str, Any]]
    other_conditions: List[Dict[str, Any]]
    high_risk_medications: List[Dict[str, Any]]
    other_medications: List[Dict[str, Any]]
    # NEW: Umbrella-grouped buckets (for improved UI)
    chronic_conditions_grouped: Dict[str, Any]
    other_conditions_grouped: Dict[str, Any]
    high_risk_medications_grouped: Dict[str, Any]
    other_medications_grouped: Dict[str, Any]


class CheckCodeRequest(BaseModel):
    """Request to check if a single SNOMED code is high-risk"""
    snomed_code: str = Field(..., description="SNOMED CT code to check")
    condition_text: Optional[str] = Field(None, description="Optional condition text for context")


class CheckChronicRequest(BaseModel):
    """Request to check if condition is chronic"""
    snomed_code: Optional[str] = None
    icd10_code: Optional[str] = None
    condition_text: Optional[str] = None


class CheckChronicResponse(BaseModel):
    """Response for chronic condition check"""
    is_chronic: bool


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    hierarchy_loaded: bool
    hierarchy_size: int
    uptime_seconds: float
    request_count: int
    timestamp: str


# ============================================================================
# STARTUP - LOAD DATABASE AND BUILD HIERARCHY
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """
    Load database and build SNOMED hierarchy on service startup.
    This happens once and keeps everything in memory for fast lookups.
    """
    global STARTUP_TIME, HIERARCHY_LOADED, HIERARCHY_SIZE
    
    STARTUP_TIME = time.time()
    
    print("="*80)
    print("APS RISK ANALYSIS SERVICE - STARTUP")
    print("="*80)
    print(f"Starting at: {datetime.now().isoformat()}")
    print()
    
    try:
        # Setup database cache
        print("[STARTUP] Setting up database cache...")
        db_path = setup_database_cache()
        print(f"[STARTUP] ✓ Database path: {db_path}")
        
        # Force hierarchy build and cache in memory
        print("[STARTUP] Building SNOMED hierarchy (this may take 10-30 seconds)...")
        start = time.time()
        
        hierarchy = get_expanded_snomed_hierarchy()
        
        elapsed = time.time() - start
        print(f"[STARTUP] ✓ Hierarchy built in {elapsed:.1f}s")
        
        # Calculate statistics
        HIERARCHY_SIZE = sum(len(descendants) for descendants in hierarchy.values())
        HIERARCHY_LOADED = True
        
        print(f"[STARTUP] ✓ Hierarchy stats:")
        print(f"           - Root codes: {len(hierarchy)}")
        print(f"           - Total descendants: {HIERARCHY_SIZE}")
        print()
        print("="*80)
        print("SERVICE READY - Accepting requests")
        print("="*80)
        sys.stdout.flush()
        
    except Exception as e:
        print(f"[STARTUP] ✗ FATAL ERROR: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


# ============================================================================
# MIDDLEWARE - REQUEST LOGGING
# ============================================================================

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests with timing"""
    global REQUEST_COUNT
    REQUEST_COUNT += 1
    
    start_time = time.time()
    response = await call_next(request)
    duration = (time.time() - start_time) * 1000
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {request.method} {request.url.path} - {response.status_code} - {duration:.1f}ms")
    sys.stdout.flush()
    
    return response


# ============================================================================
# HEALTH & STATUS ENDPOINTS
# ============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    uptime = time.time() - STARTUP_TIME if STARTUP_TIME else 0
    
    return HealthResponse(
        status="healthy" if HIERARCHY_LOADED else "initializing",
        hierarchy_loaded=HIERARCHY_LOADED,
        hierarchy_size=HIERARCHY_SIZE,
        uptime_seconds=uptime,
        request_count=REQUEST_COUNT,
        timestamp=datetime.now().isoformat()
    )


# ============================================================================
# CORE ENDPOINTS - Called by Lambda
# ============================================================================

@app.post("/scan_conditions", response_model=ScanConditionsResponse)
async def scan_conditions(request: ScanConditionsRequest):
    """
    Scan coded conditions for high-risk matches AND categorize all conditions.
    Returns both hierarchical risks and categorized buckets in one call.
    """
    if not HIERARCHY_LOADED:
        raise HTTPException(status_code=503, detail="Hierarchy not yet loaded")
    
    start_time = time.time()
    
    try:
        # Step 1: Scan for hierarchical high-risk matches
        hierarchical_risks, matched_codes = scan_coded_conditions_hierarchical(
            request.coded_conditions
        )
        
        # Step 2: Categorize all conditions using high-risk codes from step 1
        high_risk_codes = set(matched_codes)
        categorized = categorize_coded_conditions(
            request.coded_conditions,
            high_risk_codes,
            skip_parent_codes=set()  # Don't skip any codes
        )
        
        duration_ms = (time.time() - start_time) * 1000
        
        return ScanConditionsResponse(
            hierarchical_risks=hierarchical_risks,
            matched_codes=list(matched_codes),
            processing_time_ms=duration_ms,
            total_conditions_scanned=len(request.coded_conditions),
            high_risk_count=len(hierarchical_risks),
            # Return categorized buckets (flat lists)
            chronic_conditions=categorized['chronic_conditions'],
            high_risk_conditions=categorized['high_risk_conditions'],
            other_conditions=categorized['other_conditions'],
            high_risk_medications=categorized['high_risk_medications'],
            other_medications=categorized['other_medications'],
            # Return umbrella-grouped buckets (NEW)
            chronic_conditions_grouped=categorized['chronic_conditions_grouped'],
            other_conditions_grouped=categorized['other_conditions_grouped'],
            high_risk_medications_grouped=categorized['high_risk_medications_grouped'],
            other_medications_grouped=categorized['other_medications_grouped']
        )
        
    except Exception as e:
        print(f"[ERROR] scan_conditions failed: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error scanning conditions: {str(e)}")

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all error handler"""
    print(f"[ERROR] Unhandled exception: {str(exc)}")
    traceback.print_exc()
    
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": str(exc),
            "path": str(request.url)
        }
    )


# ============================================================================
# MAIN - RUN SERVER
# ============================================================================



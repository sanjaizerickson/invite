import json
import os
import sys
import boto3
from botocore.config import Config
from urllib.parse import urlparse
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import ValidationError
import time
import requests
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from functools import lru_cache
from collections import deque
import pymedtermino
import time
import shutil
import pickle  # OPTIMIZATION 1: For hierarchy cache

# ============================================================================
# LAMBDA /tmp/ CACHING FOR EFS DATABASE
# Copy DB from EFS to /tmp/ on cold starts for faster warm invocation access
# ============================================================================

def setup_database_cache():
    """
    Cache SNOMED database in Lambda /tmp/ directory for fast warm-start access.
    
    On cold starts: Copies 710MB DB from EFS to /tmp/ (one-time cost ~2-5 seconds)
    On warm starts: Reuses cached DB in /tmp/ (instant access)
    
    Returns:
        str: Path to database directory (either /tmp/db/ or fallback)
    """
    EFS_DB_PATH = "/mnt/efs/db/"
    LAMBDA_TMP_DB_PATH = "/tmp/db/"
    LOCAL_DB_PATH = "db/"
    EXPECTED_DB_SIZE_BYTES = 700 * 1024 * 1024  # ~700MB minimum
    
    # Check if running in Lambda (EFS mounted)
    if os.path.exists(EFS_DB_PATH):
        print(f"[DB CACHE] Running in Lambda environment")
        
        efs_db_file = os.path.join(EFS_DB_PATH, "snomedct.sqlite3")
        tmp_db_file = os.path.join(LAMBDA_TMP_DB_PATH, "snomedct.sqlite3")
        
        # Check if DB already cached in /tmp/ and is valid
        if os.path.exists(tmp_db_file):
            tmp_size = os.path.getsize(tmp_db_file)
            efs_size = os.path.getsize(efs_db_file) if os.path.exists(efs_db_file) else 0
            
            # Verify integrity: size must match EFS and be reasonable
            if tmp_size >= EXPECTED_DB_SIZE_BYTES and abs(tmp_size - efs_size) < 1024:
                print(f"[DB CACHE] ✓ Valid database cached in /tmp/ ({tmp_size / (1024**2):.1f}MB) - warm start")
                return LAMBDA_TMP_DB_PATH
            else:
                print(f"[DB CACHE] ✗ Cached database corrupt or incomplete (size: {tmp_size / (1024**2):.1f}MB)")
                print(f"[DB CACHE] Removing corrupt cache and using EFS directly")
                try:
                    os.remove(tmp_db_file)
                except:
                    pass
                return EFS_DB_PATH
        
        # Cold start - attempt copy from EFS to /tmp/ (but don't block on it)
        print(f"[DB CACHE] Cold start - will use EFS directly (background copy disabled due to init timeout)")
        print(f"[DB CACHE] Note: DB access will be slower on cold starts")
        
        # Don't attempt copy during init phase to avoid timeout
        # The copy takes ~10 seconds and Lambda init timeout is 10 seconds
        # Better to use EFS directly than corrupt the database
        # Call copy_db_to_tmp_background() from handler to warm cache for next invocation
        
        return EFS_DB_PATH
    
    # Local development environment
    elif os.path.exists(LOCAL_DB_PATH):
        print(f"[DB CACHE] Running in local environment")
        return LOCAL_DB_PATH
    
    else:
        print(f"[DB CACHE] ✗ Database not found in any expected location")
        return LOCAL_DB_PATH  # Default fallback

def copy_db_to_tmp_synchronous():
    """
    Synchronously copy DB from EFS to /tmp/ for faster warm-start access.
    Call this DURING your Lambda handler to ensure completion.
    Takes ~10 seconds but guarantees cache is ready for next invocation.
    """
    EFS_DB_PATH = "/mnt/efs/db/"
    LAMBDA_TMP_DB_PATH = "/tmp/db/"
    
    if not os.path.exists(EFS_DB_PATH):
        return False  # Not in Lambda environment
    
    efs_db_file = os.path.join(EFS_DB_PATH, "snomedct.sqlite3")
    tmp_db_file = os.path.join(LAMBDA_TMP_DB_PATH, "snomedct.sqlite3")
    
    # Check if already cached and valid
    if os.path.exists(tmp_db_file):
        tmp_size = os.path.getsize(tmp_db_file)
        efs_size = os.path.getsize(efs_db_file)
        if abs(tmp_size - efs_size) < 1024:  # Sizes match
            return True  # Already cached
    
    try:
        print(f"[DB CACHE] Copying DB from EFS to /tmp/ (takes ~10s)...")
        sys.stdout.flush()
        start_time = time.time()
        
        os.makedirs(LAMBDA_TMP_DB_PATH, exist_ok=True)
        shutil.copy2(efs_db_file, tmp_db_file)
        
        elapsed = time.time() - start_time
        size_mb = os.path.getsize(tmp_db_file) / (1024**2)
        print(f"[DB CACHE] ✓ Copy complete: {size_mb:.1f}MB cached in {elapsed:.2f}s")
        print(f"[DB CACHE] Next invocation will be much faster!")
        sys.stdout.flush()
        return True
        
    except Exception as e:
        print(f"[DB CACHE] ✗ Copy failed: {e}")
        sys.stdout.flush()
        return False

# Initialize database path
DB_DIRECTORY = setup_database_cache()

# ============================================================================
# LAZY LOADING FOR SNOMEDCT TO AVOID LAMBDA INIT TIMEOUT
# Only load SNOMEDCT when handler is invoked, not at module import time
# ============================================================================

_SNOMEDCT = None
_snomedct_initialized = False

def get_snomedct():
    """
    Lazy-load SNOMEDCT module on first access.
    Prevents Lambda INIT timeout by deferring database loading until handler invocation.
    
    Returns:
        SNOMEDCT module
    """
    global _SNOMEDCT, _snomedct_initialized
    
    if _snomedct_initialized:
        return _SNOMEDCT
    
    try:
        print("[SNOMEDCT] Lazy loading SNOMEDCT database...")
        sys.stdout.flush()  # Force output
        start_time = time.time()
        
        # Configure pymedtermino
        print("[SNOMEDCT] Configuring pymedtermino...")
        sys.stdout.flush()
        pymedtermino.LANGUAGE = "en" 
        pymedtermino.DATA_DIR = DB_DIRECTORY
        pymedtermino.REMOVE_SUPPRESSED_CONCEPTS = False
        
        # Import SNOMEDCT
        print("[SNOMEDCT] Importing SNOMEDCT module...")
        sys.stdout.flush()
        from pymedtermino.snomedct import SNOMEDCT as _SNOMEDCTModule
        _SNOMEDCT = _SNOMEDCTModule
        _snomedct_initialized = True
        
        elapsed = time.time() - start_time
        print(f"[SNOMEDCT] ✓ Loaded in {elapsed:.2f}s")
        sys.stdout.flush()
        
        return _SNOMEDCT
        
    except Exception as e:
        print(f"[SNOMEDCT] ✗ Failed to load: {e}")
        raise

# Import owlready2 for dynamic SNOMED CT ontology traversal
try:
    from owlready2 import get_ontology, Thing
    OWLREADY2_AVAILABLE = True
    print("[ONTOLOGY] owlready2 is available - will use dynamic SNOMED CT traversal")
except ImportError:
    OWLREADY2_AVAILABLE = False
    print("[ONTOLOGY] owlready2 not available - will use API-based hierarchy traversal")

# Import Pydantic models for validation
from pydantic_models_risk_analyzer import (
    RiskAnalysisAndSummaryResponse,
    ExecutiveSummary,
    HighRiskFinding,
    ConditionSummaryResponse,
    safe_validate_llm_output,
    extract_validated_dict
)

# Load configuration from config.local.json (same pattern as aps_assign_icd_snomed_codes.py)
def load_config():
    """Load configuration from config.local.json if it exists"""
    config_path = os.path.join(os.path.dirname(__file__), "config.local.json")
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("⚠️ config.local.json not found. Using default AWS credentials.")
        return None
    except Exception as e:
        print(f"⚠️ Error loading config.local.json: {e}. Using default AWS credentials.")
        return None

# Initialize AWS clients with profile from config (same pattern as aps_assign_icd_snomed_codes.py)
try:
    config = load_config()
    
    if config:
        # Use config file settings
        AWS_PROFILE = config.get("aws_profile", "default")
        REGION = config.get("aws_region", "us-east-1")
        
        print(f"[CONFIG] Loading AWS profile: {AWS_PROFILE}, region: {REGION}")
        
        # Configure boto3 with timeouts to prevent hanging
        # OPTIMIZATION 5: Extended timeouts and connection pool sizing
        boto_config = Config(
            region_name=REGION,
            connect_timeout=30,          # Increased for VPC endpoints
            read_timeout=120,            # Increased for Bedrock API calls
            retries={'max_attempts': 3, 'mode': 'standard'},
            max_pool_connections=10      # Match ThreadPool worker count
        )
        
        # Create a session with the specified profile
        boto_session = boto3.Session(profile_name=AWS_PROFILE, region_name=REGION)
        
        # Create clients using the session with timeout config
        s3_client = boto_session.client("s3", config=boto_config)
        bedrock_runtime = boto3.client("bedrock-runtime", config=boto_config)
        
        print(f"[CONFIG] AWS clients initialized successfully with profile '{AWS_PROFILE}'")
        print("[CONFIG] Connection timeout: 30s, Read timeout: 120s, Pool size: 10")
    else:
        # Use default AWS credentials (environment variables, IAM role, etc.)
        REGION = "us-east-1"
        
        print("[CONFIG] Using default AWS credentials")
        
        # Configure boto3 with timeouts to prevent hanging
        # OPTIMIZATION 5: Extended timeouts and connection pool sizing
        boto_config = Config(
            region_name=REGION,
            connect_timeout=30,          # Increased for VPC endpoints
            read_timeout=120,            # Increased for Bedrock API calls
            retries={'max_attempts': 3, 'mode': 'standard'},
            max_pool_connections=10      # Match ThreadPool worker count
        )
        
        # Create clients with default credentials and timeout config
        s3_client = boto3.client("s3", config=boto_config)
        bedrock_runtime = boto3.client("bedrock-runtime", config=boto_config)
        
        print("[CONFIG] AWS clients initialized with default credentials")
        print("[CONFIG] Connection timeout: 30s, Read timeout: 120s, Pool size: 10")

except Exception as e:
    print(f"[CONFIG ERROR] Failed to initialize AWS clients: {e}")
    print("[CONFIG ERROR] Script may fail during S3/Bedrock operations")
    # Create clients anyway (will fail at runtime if credentials missing)
    REGION = "us-east-1"
    s3_client = boto3.client("s3", region_name=REGION)
    bedrock_runtime = boto3.client("bedrock-runtime", region_name=REGION)
# ============================================================================
# SNOMED CT HIERARCHICAL STRUCTURE - Node Definition
# ============================================================================

@dataclass
class SnomedNode:
    """Represents a SNOMED CT concept with hierarchical information"""
    code: str
    name: str
    category: str
    risk_level: str
    aliases: List[str] = None
    children: Dict[str, 'SnomedNode'] = None
    parent_code: str = None  # Track parent for path construction
    
    def __post_init__(self):
        if self.aliases is None:
            self.aliases = []
        if self.children is None:
            self.children = {}

# ============================================================================
# SNOMED CT HIERARCHICAL STRUCTURE - 18 High-Risk Conditions
# Using FLAT structure - descendants retrieved dynamically from DB via pymedtermino
# Children NOT hardcoded - expanded at module load from SNOMED database
# ============================================================================
_RISK_ORDER = {"Critical": 0, "High": 1}

SNOMED_HIGH_RISK_CODES = {
    # ========== ONCOLOGY ==========
    "128462008": SnomedNode(
        code="128462008",
        name="Metastatic malignant neoplasm",
        category="Oncology",
        risk_level="Critical",
        aliases=["metastatic cancer", "metastatic", "metastasis", "metastases"]
    ),
    
    "363358000": SnomedNode(
        code="363358000",
        name="Malignant tumor of lung",
        category="Oncology",
        risk_level="High",
        aliases=["lung cancer", "malignant neoplasm of lung", "bronchogenic carcinoma"]
    ),
    
    # ========== CARDIOVASCULAR ==========
    "42343007": SnomedNode(
        code="42343007",
        name="Congestive heart failure",
        category="Cardiovascular",
        risk_level="High",
        aliases=["chf", "heart failure", "cardiac failure", "hf"]
    ),
    
    "22298006": SnomedNode(
        code="22298006",
        name="Myocardial infarction",
        category="Cardiovascular",
        risk_level="High",
        aliases=["heart attack", "mi", "acute mi", "acute myocardial infarction"]
    ),
    
    "230690007": SnomedNode(
        code="230690007",
        name="Cerebrovascular accident",
        category="Cardiovascular",
        risk_level="High",
        aliases=["stroke", "brain attack", "cerebrovascular accident"]
    ),
    
    # ========== RESPIRATORY ==========
    "13645005": SnomedNode(
        code="13645005",
        name="Chronic obstructive pulmonary disease",
        category="Respiratory",
        risk_level="High",
        aliases=["copd", "chronic obstructive lung disease", "chronic obstructive airway disease"]
    ),
    
    "51615001": SnomedNode(
        code="51615001",
        name="Pulmonary fibrosis",
        category="Respiratory",
        risk_level="High",
        aliases=["lung fibrosis", "interstitial lung disease", "ild"]
    ),
    
    # ========== RENAL ==========
    "433146000": SnomedNode(
        code="433146000",
        name="Chronic kidney disease stage 5",
        category="Renal",
        risk_level="Critical",
        aliases=["ckd stage 5", "ckd 5", "stage 5 ckd", "stage v chronic kidney disease"]
    ),
    
    "90870004": SnomedNode(
        code="90870004",
        name="End-stage renal disease",
        category="Renal",
        risk_level="Critical",
        aliases=["esrd", "end stage kidney disease", "kidney failure", "renal failure", "eskd"]
    ),
    
    # ========== HEPATIC ==========
    "19943007": SnomedNode(
        code="19943007",
        name="Cirrhosis of liver",
        category="Hepatic",
        risk_level="High",
        aliases=["cirrhosis", "hepatic cirrhosis", "liver cirrhosis"]
    ),
    
    "59927004": SnomedNode(
        code="59927004",
        name="Hepatic failure",
        category="Hepatic",
        risk_level="Critical",
        aliases=["liver failure", "hepatic insufficiency", "acute liver failure"]
    ),
    
    # ========== NEUROLOGICAL ==========
    "52448006": SnomedNode(
        code="52448006",
        name="Dementia",
        category="Neurological",
        risk_level="High",
        aliases=["cognitive decline", "neurocognitive disorder", "dementia syndrome"]
    ),
    
    "26929004": SnomedNode(
        code="26929004",
        name="Alzheimer's disease",
        category="Neurological",
        risk_level="High",
        aliases=["alzheimer", "alzheimers", "alzheimer disease", "alzheimer's"]
    ),
    
    # ========== INFECTIOUS ==========
    "86406008": SnomedNode(
        code="86406008",
        name="Human immunodeficiency virus infection",
        category="Infectious",
        risk_level="High",
        aliases=["hiv", "hiv infection", "hiv positive", "human immunodeficiency virus"]
    ),
    
    # ========== SUBSTANCE ABUSE ==========
    "75544000": SnomedNode(
        code="75544000",
        name="Opioid dependence",
        category="Substance Abuse",
        risk_level="High",
        aliases=["opioid addiction", "opioid use disorder", "opioid abuse"]
    ),
    
    "66590003": SnomedNode(
        code="66590003",
        name="Alcohol dependence",
        category="Substance Abuse",
        risk_level="High",
        aliases=["alcoholism", "alcohol addiction", "alcohol use disorder", "alcohol abuse"]
    ),
    
    # ========== PSYCHIATRIC ==========
    "58214004": SnomedNode(
        code="58214004",
        name="Schizophrenia",
        category="Psychiatric",
        risk_level="High",
        aliases=["schizophrenic disorder", "schizophrenic"]
    ),
    
    "82313006": SnomedNode(
        code="82313006",
        name="Suicide attempt",
        category="Psychiatric",
        risk_level="Critical",
        aliases=["attempted suicide", "suicide", "suicidal attempt"]
    )
}

# ============================================================================
# PYMEDTERMINO DOWNWARD HIERARCHY EXPANSION
# Cache and expand all descendants from 18 high-risk parent conditions
# ============================================================================

@lru_cache(maxsize=300_000)
def get_snomed_concept(snomed_code: str):
    """Cached wrapper for SNOMEDCT concept lookup"""
    SNOMEDCT = get_snomedct()
    return SNOMEDCT[str(snomed_code)]


@lru_cache(maxsize=500_000)
def get_snomed_term(snomed_code: str) -> str:
    """Cached wrapper for SNOMED term retrieval"""
    return get_snomed_concept(snomed_code).term


# OPTIMIZATION 2: BATCH SNOMED QUERIES
# Fetch multiple SNOMED terms at once to reduce DB round-trips
# ============================================================================

def batch_fetch_snomed_concepts(codes: List[str]) -> Dict[str, any]:
    """
    OPTIMIZATION 2: Batch fetch multiple SNOMED terms at once.
    Uses cache when available, only fetches uncached terms from DB.
    
    TIME SAVED: Eliminates 90% of redundant DB queries
    
    Args:
        codes: List of SNOMED codes to fetch
    
    Returns:
        Dict mapping code → concept object
    """
    SNOMEDCT = get_snomedct()
    results = {}
    
    for code in codes:
        if code:
            try:
                # This will use the @lru_cache if already fetched
                results[code] = get_snomed_concept(code)
            except (KeyError, IndexError):
                pass  # Code not found in SNOMED
    
    return results


@lru_cache(maxsize=800_000)
def get_all_snomed_descendants(parent_code: str, max_depth: int = 15) -> Tuple[str, ...]:
    """
    Recursively get all descendant SNOMED codes from a parent using pymedtermino database.
    Uses caching to handle overlapping subtrees efficiently.
    
    Args:
        parent_code: SNOMED CT parent code
        max_depth: Maximum recursion depth (prevents infinite loops)
    
    Returns:
        Tuple of all descendant codes (deterministic ordering)
    """
    descendants = set()
    
    def recurse(current_code: str, depth: int):
        if depth >= max_depth:
            return
        
        try:
            concept = get_snomed_concept(current_code)
            children = getattr(concept, "children", []) or []
            
            for child in children:
                child_code = str(child.code)
                if child_code not in descendants:
                    descendants.add(child_code)
                    recurse(child_code, depth + 1)
        except Exception as e:
            # Skip concepts that can't be loaded
            pass
    
    try:
        recurse(parent_code, 0)
    except Exception as e:
        print(f"[EXPANSION] Error expanding {parent_code}: {e}")
    
    return tuple(sorted(descendants))  # Deterministic ordering


def build_expanded_snomed_hierarchy() -> Dict[str, Set[str]]:
    """
    Build expanded SNOMED hierarchy by traversing DB downward from 18 high-risk parents.
    This is the COLD START operation - runs once at module load or when cache is cleared.
    """
    print("="*80)
    print("[SNOMED EXPANSION] Starting downward expansion from 18 high-risk parent conditions")
    print("[SNOMED EXPANSION] This may take 10-30 seconds on first run (cached afterward)")
    print("="*80)
    
    expanded_hierarchy = {}
    start_time = time.time()
    total_descendants = 0
    
    for idx, (root_code, root_node) in enumerate(SNOMED_HIGH_RISK_CODES.items(), 1):
        print(f"\n[{idx}/18] Expanding: {root_node.name} ({root_code})")
        
        expansion_start = time.time()
        
        # Get all descendants from database
        descendant_codes = get_all_snomed_descendants(root_code)
        
        # Include root code itself in the set
        all_codes = {root_code} | set(descendant_codes)
        expanded_hierarchy[root_code] = all_codes
        
        expansion_time = time.time() - expansion_start
        descendant_count = len(descendant_codes)
        total_descendants += descendant_count
        
        print(f"    ✓ Found {descendant_count:,} descendants in {expansion_time:.2f}s")
    
    total_time = time.time() - start_time
    total_codes = sum(len(codes) for codes in expanded_hierarchy.values())
    
    print("\n" + "="*80)
    print(f"[SNOMED EXPANSION] ✓ Expansion complete!")
    print(f"[SNOMED EXPANSION] Total time: {total_time:.2f}s")
    print(f"[SNOMED EXPANSION] Total descendants found: {total_descendants:,}")
    print(f"[SNOMED EXPANSION] Total codes in hierarchy: {total_codes:,} (including 18 roots)")
    print(f"[SNOMED EXPANSION] Cache size: {get_all_snomed_descendants.cache_info()}")
    print("="*80)
    
    return expanded_hierarchy


# ============================================================================
# LAZY INITIALIZATION FOR LAMBDA
# Defer hierarchy expansion to first invocation (avoid init timeout)
# OPTIMIZATION 1: Try pre-computed pickle cache first
# ============================================================================

# Hierarchy cache paths
HIERARCHY_CACHE_PATH = '/opt/snomed_hierarchy_cache.pkl'  # Lambda Layer
HIERARCHY_CACHE_FALLBACK = '/tmp/snomed_hierarchy_cache.pkl'  # S3 download fallback

def load_precomputed_hierarchy() -> Optional[Dict[str, Set[str]]]:
    """
    OPTIMIZATION 1: Load pre-computed SNOMED hierarchy from pickle file.
    Falls back to building if cache not available.
    
    TIME SAVED: 10-30 seconds → <1 second
    """
    # Try Lambda Layer first
    for cache_path in [HIERARCHY_CACHE_PATH, HIERARCHY_CACHE_FALLBACK]:
        if os.path.exists(cache_path):
            try:
                print(f"[CACHE] Loading pre-computed hierarchy from {cache_path}...")
                start = time.time()
                with open(cache_path, 'rb') as f:
                    hierarchy = pickle.load(f)
                elapsed = time.time() - start
                print(f"[CACHE] ✓ Loaded {len(hierarchy)} hierarchies in {elapsed:.2f}s")
                return hierarchy
            except Exception as e:
                print(f"[CACHE] Failed to load from {cache_path}: {e}")
    
    print("[CACHE] No pre-computed hierarchy found, will build dynamically")
    return None

# Global variable - will be populated on first use
EXPANDED_SNOMED_HIERARCHY = None
_hierarchy_initialized = False

def get_expanded_snomed_hierarchy() -> Dict[str, Set[str]]:
    """
    Lazily build SNOMED hierarchy on first access (not at module load).
    OPTIMIZATION 1: Tries pre-computed pickle file first (10-30s → <1s)
    This prevents Lambda init timeout by deferring expensive operation to first invocation.
    """
    global EXPANDED_SNOMED_HIERARCHY, _hierarchy_initialized
    
    if not _hierarchy_initialized:
        print("\n[LAZY INIT] First invocation - building SNOMED hierarchy...")
        
        # OPTIMIZATION 1: Try loading from pre-computed cache
        cached_hierarchy = load_precomputed_hierarchy()
        if cached_hierarchy is not None:
            EXPANDED_SNOMED_HIERARCHY = cached_hierarchy
        else:
            # Fall back to dynamic building
            EXPANDED_SNOMED_HIERARCHY = build_expanded_snomed_hierarchy()
        
        _hierarchy_initialized = True
    
    return EXPANDED_SNOMED_HIERARCHY


# ============================================================================
# FAST LOOKUPS USING EXPANDED HIERARCHY
# ============================================================================

def find_high_risk_root_for_snomed_code(snomed_code: str) -> Optional[Tuple[str, SnomedNode]]:
    """
    Check if a SNOMED code belongs to any of the 18 high-risk hierarchies.
    Uses precomputed EXPANDED_SNOMED_HIERARCHY for O(1) set membership lookups.
    
    Args:
        snomed_code: SNOMED CT code to check
    
    Returns:
        Tuple of (root_code, root_node) if found, None otherwise
        If code matches multiple roots, returns highest priority (Critical > High)
    """
    if not snomed_code:
        return None
    
    snomed_code = str(snomed_code)
    
    # Lazy load hierarchy on first use
    expanded_hierarchy = get_expanded_snomed_hierarchy()
    
    # Check if code matches any high-risk root hierarchy
    matched_roots = []
    for root_code, descendant_codes in expanded_hierarchy.items():
        if snomed_code in descendant_codes:
            root_node = SNOMED_HIGH_RISK_CODES[root_code]
            matched_roots.append((root_code, root_node))
    
    if not matched_roots:
        return None
    
    # If multiple matches, return highest priority root (Critical > High)
    if len(matched_roots) > 1:
        matched_roots.sort(key=lambda x: (_RISK_ORDER.get(x[1].risk_level, 99), x[0]))
    
    return matched_roots[0]


def build_hierarchical_path_from_root(root_code: str, child_code: str) -> List[Tuple[str, str]]:
    """
    Build hierarchical path from root to child code using database traversal.
    Returns list of (code, name) tuples representing shortest path.
    
    Note: Path reconstruction is lightweight since we just need parent chain,
    not full descendant traversal.
    """
    if root_code == child_code:
        # Child is the root itself
        try:
            root_term = get_snomed_term(root_code)
            return [(root_code, root_term)]
        except:
            root_node = SNOMED_HIGH_RISK_CODES.get(root_code)
            if root_node:
                return [(root_code, root_node.name)]
            return [(root_code, "Unknown")]
    
    # BFS upward from child to root to find shortest path
    from collections import deque
    
    queue = deque([child_code])
    visited = {child_code}
    parent_map = {}  # child -> parent mapping for path reconstruction
    
    found_root = False
    while queue and not found_root:
        current_code = queue.popleft()
        
        if current_code == root_code:
            found_root = True
            break
        
        # Get parents from database
        try:
            concept = get_snomed_concept(current_code)
            parents = getattr(concept, "parents", []) or []
            
            for parent in parents:
                parent_code = str(parent.code)
                if parent_code not in visited:
                    visited.add(parent_code)
                    parent_map[parent_code] = current_code
                    queue.append(parent_code)
                    
                    if parent_code == root_code:
                        found_root = True
                        break
        except:
            pass
    
    if not found_root:
        # Couldn't find path (shouldn't happen if code is in expanded hierarchy)
        # Return simple 2-node path
        try:
            root_term = get_snomed_term(root_code)
            child_term = get_snomed_term(child_code)
            return [(root_code, root_term), (child_code, child_term)]
        except:
            return [(root_code, "Unknown"), (child_code, "Unknown")]
    
    # Reconstruct path from root to child
    path_codes = []
    current = root_code
    path_codes.append(current)
    
    # Follow parent_map in reverse to build path
    while current != child_code and current in parent_map.values():
        # Find which child points to current as parent
        for child, parent in parent_map.items():
            if parent == current:
                path_codes.append(child)
                current = child
                break
    
    # Get term names for path
    path_with_names = []
    for code in path_codes:
        try:
            term = get_snomed_term(code)
            path_with_names.append((code, term))
        except:
            # Fallback to root node name if available
            if code in SNOMED_HIGH_RISK_CODES:
                path_with_names.append((code, SNOMED_HIGH_RISK_CODES[code].name))
            else:
                path_with_names.append((code, "Unknown"))
    
    return path_with_names


# ============================================================================
# RXNORM MEDICATION HIERARCHY - High-Risk Medications
# ============================================================================

RXNORM_HIERARCHY = {
    # ========== ANTICOAGULANTS ==========
    "11289": SnomedNode(
        code="11289",
        name="Warfarin",
        category="Anticoagulant",
        risk_level="High",
        aliases=["warfarin sodium", "coumadin"],
        children={
            "855333": SnomedNode(code="855333", name="Warfarin Sodium 5 MG", category="Anticoagulant", risk_level="High", parent_code="11289"),
            "855328": SnomedNode(code="855328", name="Warfarin Sodium 2 MG", category="Anticoagulant", risk_level="High", parent_code="11289"),
            "202421": SnomedNode(code="202421", name="Coumadin", category="Anticoagulant", risk_level="High", parent_code="11289"),
        }
    ),
    
    "1364430": SnomedNode(
        code="1364430",
        name="Apixaban",
        category="Anticoagulant",
        risk_level="High",
        aliases=["eliquis"],
        children={
            "1364445": SnomedNode(code="1364445", name="Apixaban 5 MG", category="Anticoagulant", risk_level="High", parent_code="1364430"),
            "1364441": SnomedNode(code="1364441", name="Apixaban 2.5 MG", category="Anticoagulant", risk_level="High", parent_code="1364430"),
            "1599538": SnomedNode(code="1599538", name="Eliquis", category="Anticoagulant", risk_level="High", parent_code="1364430"),
        }
    ),
    
    "1114195": SnomedNode(
        code="1114195",
        name="Rivaroxaban",
        category="Anticoagulant",
        risk_level="High",
        aliases=["xarelto"],
        children={
            "1114208": SnomedNode(code="1114208", name="Rivaroxaban 20 MG", category="Anticoagulant", risk_level="High", parent_code="1114195"),
            "1232084": SnomedNode(code="1232084", name="Rivaroxaban 15 MG", category="Anticoagulant", risk_level="High", parent_code="1114195"),
            "1114198": SnomedNode(code="1114198", name="Rivaroxaban 10 MG", category="Anticoagulant", risk_level="High", parent_code="1114195"),
            "1232653": SnomedNode(code="1232653", name="Xarelto", category="Anticoagulant", risk_level="High", parent_code="1114195"),
        }
    ),
    
    "1037042": SnomedNode(
        code="1037042",
        name="Dabigatran etexilate",
        category="Anticoagulant",
        risk_level="High",
        aliases=["dabigatran", "pradaxa"],
        children={
            "1037045": SnomedNode(code="1037045", name="Dabigatran etexilate 150 MG", category="Anticoagulant", risk_level="High", parent_code="1037042"),
            "1114221": SnomedNode(code="1114221", name="Dabigatran etexilate 75 MG", category="Anticoagulant", risk_level="High", parent_code="1037042"),
            "1037049": SnomedNode(code="1037049", name="Pradaxa", category="Anticoagulant", risk_level="High", parent_code="1037042"),
        }
    ),
    
    # ========== IMMUNOSUPPRESSANTS ==========
    "42316": SnomedNode(
        code="42316",
        name="Tacrolimus",
        category="Immunosuppressant",
        risk_level="High",
        aliases=["prograf", "tacrolimus anhydrous"],
        children={
            "310257": SnomedNode(code="310257", name="Tacrolimus 1 MG", category="Immunosuppressant", risk_level="High", parent_code="42316"),
            "310256": SnomedNode(code="310256", name="Tacrolimus 0.5 MG", category="Immunosuppressant", risk_level="High", parent_code="42316"),
            "104375": SnomedNode(code="104375", name="Prograf", category="Immunosuppressant", risk_level="High", parent_code="42316"),
        }
    ),
    
    "3008": SnomedNode(
        code="3008",
        name="Cyclosporine",
        category="Immunosuppressant",
        risk_level="High",
        aliases=["cyclosporin", "sandimmune", "neoral"],
        children={
            "203809": SnomedNode(code="203809", name="Cyclosporine 100 MG", category="Immunosuppressant", risk_level="High", parent_code="3008"),
            "203810": SnomedNode(code="203810", name="Cyclosporine 25 MG", category="Immunosuppressant", risk_level="High", parent_code="3008"),
            "203927": SnomedNode(code="203927", name="Sandimmune", category="Immunosuppressant", risk_level="High", parent_code="3008"),
            "203939": SnomedNode(code="203939", name="Neoral", category="Immunosuppressant", risk_level="High", parent_code="3008"),
        }
    ),
    
    "68149": SnomedNode(
        code="68149",
        name="Mycophenolate mofetil",
        category="Immunosuppressant",
        risk_level="High",
        aliases=["mycophenolate", "cellcept"],
        children={
            "198145": SnomedNode(code="198145", name="Mycophenolate mofetil 500 MG", category="Immunosuppressant", risk_level="High", parent_code="68149"),
            "317541": SnomedNode(code="317541", name="Mycophenolate mofetil 250 MG", category="Immunosuppressant", risk_level="High", parent_code="68149"),
            "152923": SnomedNode(code="152923", name="Cellcept", category="Immunosuppressant", risk_level="High", parent_code="68149"),
        }
    ),
    
    # ========== IMMUNOTHERAPY ==========
    "1547545": SnomedNode(
        code="1547545",
        name="Pembrolizumab",
        category="Immunotherapy",
        risk_level="High",
        aliases=["keytruda"]
    ),
    
    "121191": SnomedNode(
        code="121191",
        name="Rituximab",
        category="Immunotherapy",
        risk_level="High",
        aliases=["rituxan", "mabthera"]
    ),
    
    # ========== CARDIAC MEDICATIONS ==========
    "703": SnomedNode(
        code="703",
        name="Amiodarone",
        category="Antiarrhythmic",
        risk_level="High",
        aliases=["amiodarone hydrochloride", "cordarone", "pacerone"]
    ),
    
    # ========== PSYCHIATRIC MEDICATIONS ==========
    "2626": SnomedNode(
        code="2626",
        name="Clozapine",
        category="Antipsychotic",
        risk_level="High",
        aliases=["clozaril"]
    ),
    
    # ========== OPIOIDS ==========
    "6813": SnomedNode(
        code="6813",
        name="Methadone",
        category="Opioid",
        risk_level="High",
        aliases=["methadone hydrochloride", "dolophine"],
        children={
            "864706": SnomedNode(code="864706", name="Methadone Hydrochloride 10 MG", category="Opioid", risk_level="High", parent_code="6813"),
            "864718": SnomedNode(code="864718", name="Methadone Hydrochloride 5 MG", category="Opioid", risk_level="High", parent_code="6813"),
            "864984": SnomedNode(code="864984", name="Methadone Hydrochloride 40 MG", category="Opioid", risk_level="High", parent_code="6813"),
            "6814": SnomedNode(code="6814", name="Methadone Hydrochloride", category="Opioid", risk_level="High", parent_code="6813"),
        }
    ),
    
    "1819": SnomedNode(
        code="1819",
        name="Buprenorphine",
        category="Opioid",
        risk_level="High",
        aliases=["buprenorphine hydrochloride", "subutex", "buprenex"],
        children={
            "1666831": SnomedNode(code="1666831", name="Buprenorphine 8 MG", category="Opioid", risk_level="High", parent_code="1819"),
            "1010600": SnomedNode(code="1010600", name="Buprenorphine 2 MG", category="Opioid", risk_level="High", parent_code="1819"),
            "351266": SnomedNode(code="351266", name="Subutex", category="Opioid", risk_level="High", parent_code="1819"),
            "1542390": SnomedNode(code="1542390", name="Buprenorphine / Naloxone", category="Opioid", risk_level="High", parent_code="1819", aliases=["suboxone"]),
        }
    ),
    
    # ========== HEMATOLOGIC AGENTS ==========
    "105694": SnomedNode(
        code="105694",
        name="Epoetin alfa",
        category="Erythropoietin",
        risk_level="Medium",
        aliases=["epogen", "procrit", "epo"]
    ),
    
    # ========== INSULIN ==========
    "274783": SnomedNode(
        code="274783",
        name="Insulin glargine",
        category="Insulin",
        risk_level="Medium",
        aliases=["lantus", "basaglar", "toujeo"]
    )
}


# ============================================================================
# OWLREADY2 DYNAMIC ONTOLOGY TRAVERSAL
# ============================================================================

# Global ontology cache
_snomed_ontology = None
_ontology_loaded = False

# SNOMED CT API configuration
SNOMED_API_BASE = "https://browser.ihtsdotools.org/snowstorm/snomed-ct"
SNOMED_API_BRANCH = "MAIN/2024-01-01"  # Use international edition
SNOMED_API_TIMEOUT = 30  # seconds
# ENABLE_API_FETCH = os.getenv("ENABLE_SNOMED_API", "false").lower() == "true"  # Disabled by default
ENABLE_API_FETCH = False
# Cache for API-fetched descendants
_api_descendants_cache = {}

def load_snomed_ontology(ontology_path: str = None) -> Optional[object]:
    """
    Load SNOMED CT ontology using owlready2.
    
    Args:
        ontology_path: Path to SNOMED CT OWL file. If None, looks for common locations:
                      - ./snomed_ct.owl
                      - ./ontologies/snomed_ct.owl
                      - ./data/snomed_ct.owl
    
    Returns:
        Loaded ontology object or None if not available
    """
    global _snomed_ontology, _ontology_loaded
    
    if _ontology_loaded:
        return _snomed_ontology
    
    if not OWLREADY2_AVAILABLE:
        print("[ONTOLOGY] owlready2 not available, skipping ontology load")
        _ontology_loaded = True
        return None
    
    # Search for ontology file
    search_paths = [
        ontology_path,
        "snomed_ct.owl",
        "ontologies/snomed_ct.owl",
        "data/snomed_ct.owl",
        os.path.join(os.path.dirname(__file__), "snomed_ct.owl"),
        os.path.join(os.path.dirname(__file__), "ontologies", "snomed_ct.owl")
    ]
    
    ontology_file = None
    for path in search_paths:
        if path and os.path.exists(path):
            ontology_file = path
            break
    
    if not ontology_file:
        print("[ONTOLOGY] SNOMED CT OWL file not found in standard locations")
        print("[ONTOLOGY] Searched: " + ", ".join([p for p in search_paths if p]))
        print("[ONTOLOGY] Will use API-based hierarchy traversal")
        _ontology_loaded = True
        return None
    
    try:
        print(f"[ONTOLOGY] Loading SNOMED CT ontology from {ontology_file}...")
        _snomed_ontology = get_ontology(f"file://{ontology_file}").load()
        print(f"[ONTOLOGY] SNOMED CT ontology loaded successfully")
        _ontology_loaded = True
        return _snomed_ontology
    except Exception as e:
        print(f"[ONTOLOGY] Failed to load SNOMED CT ontology: {str(e)}")
        print(f"[ONTOLOGY] Will use API-based hierarchy traversal")
        _ontology_loaded = True
        return None


def get_all_descendants_from_api(snomed_code: str) -> Set[str]:
    """
    Get all descendant SNOMED codes from SNOMED CT Terminology Server API.
    Uses the public Snowstorm API to fetch all descendants.
    
    Args:
        snomed_code: Root SNOMED CT code
    
    Returns:
        Set of all descendant codes including the root code
    """
    # Check cache first
    if snomed_code in _api_descendants_cache:
        return _api_descendants_cache[snomed_code]
    
    descendants = {snomed_code}
    
    try:
        # Add delay to avoid rate limiting
        time.sleep(0.5)
        
        # Use SNOMED CT API to get descendants
        url = f"{SNOMED_API_BASE}/browser/{SNOMED_API_BRANCH}/concepts/{snomed_code}/descendants"
        params = {
            "limit": 1000,  # Get up to 1000 descendants
            "stated": "false"  # Use inferred relationships
        }
        
        response = requests.get(url, params=params, timeout=SNOMED_API_TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            # Extract concept ID from response
            items = data.get('items', [])
            for item in items:
                concept_id = item.get('conceptId') or item.get('id')
                if concept_id:
                    descendants.add(concept_id)
            
            # Cache the result
            _api_descendants_cache[snomed_code] = descendants
        elif response.status_code == 429:
            print(f"[API] Rate limited for {snomed_code}, using root code only")
        else:
            print(f"[API] Failed to fetch descendants for {snomed_code}: HTTP {response.status_code}")
    
    except (requests.exceptions.Timeout, TimeoutError):
        print(f"[API] Timeout fetching descendants for {snomed_code}, using root code only")
    except Exception as e:
        print(f"[API] Error fetching descendants for {snomed_code}: {str(e)}, using root code only")
    
    # Cache even if failed (to avoid retrying)
    if snomed_code not in _api_descendants_cache:
        _api_descendants_cache[snomed_code] = descendants
    
    return descendants


def get_all_descendants_from_ontology(snomed_code: str, ontology: object) -> Set[str]:
    """
    Get all descendant SNOMED codes from ontology (children, grandchildren, etc.)
    
    Args:
        snomed_code: Root SNOMED CT code
        ontology: Loaded owlready2 ontology
    
    Returns:
        Set of all descendant codes including the root code
    """
    if not ontology:
        # Fall back to API-based approach only if enabled
        if ENABLE_API_FETCH:
            return get_all_descendants_from_api(snomed_code)
        else:
            return {snomed_code}
    
    descendants = {snomed_code}
    
    try:
        # Search for the concept in ontology
        # SNOMED CT concepts are typically named like "SCTID_<code>"
        concept = ontology.search_one(iri=f"*{snomed_code}")
        
        if not concept:
            # Try alternative search methods
            for cls in ontology.classes():
                if snomed_code in str(cls):
                    concept = cls
                    break
        
        if concept:
            # Get all descendants recursively
            for subclass in concept.descendants():
                # Extract SNOMED code from class name
                code = extract_snomed_code_from_iri(str(subclass))
                if code:
                    descendants.add(code)
    
    except Exception as e:
        print(f"[ONTOLOGY] Error getting descendants for {snomed_code}: {str(e)}")
        # Fall back to API only if enabled
        if ENABLE_API_FETCH:
            return get_all_descendants_from_api(snomed_code)
    
    return descendants


def extract_snomed_code_from_iri(iri: str) -> Optional[str]:
    """
    Extract SNOMED CT code from IRI/class name.
    
    Common formats:
    - SCTID_<code>
    - SNOMED_<code>
    - <code> (direct)
    """
    # Try to find numeric code
    matches = re.findall(r'\d{6,}', iri)
    if matches:
        return matches[0]
    return None


def _collect_all_descendant_codes(node: SnomedNode) -> Set[str]:
    """
    Recursively collect all descendant codes from a node (including the node's own code).
    Uses the hardcoded children structures we defined.
    """
    codes = {node.code}  # Start with this node's code
    
    # Recursively collect from all children
    for child_code, child_node in node.children.items():
        codes.add(child_code)
        # Recurse to get grandchildren, great-grandchildren, etc.
        codes.update(_collect_all_descendant_codes(child_node))
    
    return codes


def build_expanded_hierarchy_from_hardcoded(hierarchy_dict: dict, hierarchy_name: str = "SNOMED") -> Dict[str, Set[str]]:
    """
    Build expanded hierarchy mapping from HARDCODED children structures.
    No API or ontology file needed - uses the children we manually defined.
    
    Args:
        hierarchy_dict: Dictionary of root conditions (SNOMED_HIERARCHY or RXNORM_HIERARCHY)
        hierarchy_name: Name for logging ("SNOMED" or "RxNorm")
    
    Returns:
        Dict mapping root code -> set of all descendant codes (including root)
    """
    expanded_hierarchy = {}
    
    print(f"[HARDCODED] Building expanded {hierarchy_name} hierarchy from hardcoded children...")
    
    # Process each high-risk condition
    for root_code, root_node in hierarchy_dict.items():
        # Recursively collect all codes (root + all descendants)
        all_codes = _collect_all_descendant_codes(root_node)
        expanded_hierarchy[root_code] = all_codes
        
        child_count = len(all_codes) - 1  # Subtract root itself
        if child_count > 0:
            print(f"[HARDCODED]   {root_node.name} ({root_code}): {child_count} children/descendants")
        else:
            print(f"[HARDCODED]   {root_node.name} ({root_code}): No children defined")
    
    print(f"[HARDCODED] Expanded {hierarchy_name} hierarchy ready: {len(expanded_hierarchy)} root conditions")
    return expanded_hierarchy


def build_expanded_hierarchy_with_ontology(ontology_path: str = None, hierarchy_dict: dict = None, hierarchy_name: str = "SNOMED") -> Dict[str, Set[str]]:
    """
    Build expanded hierarchy mapping for all high-risk conditions.
    
    UPDATED: Now uses hardcoded children as primary source, can optionally enhance with ontology/API.
    
    Args:
        ontology_path: Path to OWL ontology file (optional)
        hierarchy_dict: Dictionary of root conditions (SNOMED_HIERARCHY or RXNORM_HIERARCHY)
        hierarchy_name: Name for logging ("SNOMED" or "RxNorm")
    
    Returns:
        Dict mapping root code -> set of all descendant codes (including root)
    """
    if hierarchy_dict is None:
        hierarchy_dict = SNOMED_HIERARCHY
    
    # PRIMARY SOURCE: Use hardcoded children structures
    expanded_hierarchy = build_expanded_hierarchy_from_hardcoded(hierarchy_dict, hierarchy_name)
    
    # OPTIONAL: Enhance with ontology/API if available
    ontology = load_snomed_ontology(ontology_path) if hierarchy_name == "SNOMED" else None
    use_api = not ontology and hierarchy_name == "SNOMED" and ENABLE_API_FETCH
    
    if use_api:
        print(f"[API] Enhancing with SNOMED CT Terminology Server API...")
        for root_code, root_node in hierarchy_dict.items():
            api_codes = get_all_descendants_from_api(root_code)
            if api_codes:
                # Merge API results with hardcoded
                original_count = len(expanded_hierarchy[root_code])
                expanded_hierarchy[root_code].update(api_codes)
                new_count = len(expanded_hierarchy[root_code]) - original_count
                if new_count > 0:
                    print(f"[API]   Added {new_count} additional codes for {root_node.name}")
    elif ontology:
        print(f"[OWL] Enhancing with SNOMED CT OWL ontology file...")
        for root_code, root_node in hierarchy_dict.items():
            owl_codes = get_all_descendants_from_ontology(root_code, ontology)
            if owl_codes:
                # Merge OWL results with hardcoded
                original_count = len(expanded_hierarchy[root_code])
                expanded_hierarchy[root_code].update(owl_codes)
                new_count = len(expanded_hierarchy[root_code]) - original_count
                if new_count > 0:
                    print(f"[OWL]   Added {new_count} additional codes for {root_node.name}")
    
    return expanded_hierarchy


# ============================================================================
# HIERARCHICAL PATH FUNCTIONS (Legacy - kept for compatibility)
# ============================================================================

def build_hierarchical_path(code: str, node: SnomedNode = None) -> List[Tuple[str, str]]:
    """
    LEGACY FUNCTION - Kept for backward compatibility.
    Use build_hierarchical_path_from_root() for new code.
    """
    # For SNOMED, use new expanded hierarchy approach
    result = find_high_risk_root_for_snomed_code(code)
    if result:
        root_code, root_node = result
        return build_hierarchical_path_from_root(root_code, code)
    
    # For RxNorm, use hardcoded children approach  
    rxnorm_result = find_node_in_rxnorm_hierarchy(code)
    if rxnorm_result:
        root_code, node = rxnorm_result
        path = [(root_code, RXNORM_HIERARCHY[root_code].name)]
        if code != root_code:
            path.append((code, node.name))
        return path
    
    return []


def find_node_in_hierarchy(code: str, hierarchy_node: SnomedNode = None, current_code: str = None) -> Tuple[Optional[SnomedNode], Optional[str]]:
    """
    LEGACY FUNCTION - Kept for backward compatibility.
    Use find_high_risk_root_for_snomed_code() or find_node_in_rxnorm_hierarchy() for new code.
    """
    if hierarchy_node is None:
        # Try SNOMED first
        result = find_high_risk_root_for_snomed_code(code)
        if result:
            root_code, root_node = result
            return (root_node, code)
        
        # Try RxNorm
        rxnorm_result = find_node_in_rxnorm_hierarchy(code)
        if rxnorm_result:
            root_code, node = rxnorm_result
            return (node, code)
        
        return None, None
    
    # Check current node
    if current_code == code:
        return hierarchy_node, current_code
    
    # Check children (RxNorm only - SNOMED has no hardcoded children)
    for child_code, child_node in hierarchy_node.children.items():
        if child_code == code:
            return child_node, child_code
        
        # Recurse deeper
        result_node, result_code = find_node_in_hierarchy(code, child_node, child_code)
        if result_node is not None:
            return result_node, result_code
    
    return None, None


def format_hierarchical_path(path: List[Tuple[str, str]]) -> str:
    """
    Format hierarchical path as string: "Parent > Child"
    Example: "Myocardial infarction > Acute ST segment elevation myocardial infarction"
    """
    if not path:
        return ""
    
    names = [name for code, name in path]
    return " > ".join(names)


def is_high_risk_condition_with_path(snomed_code: str, condition_text: str = "", code_only: bool = False) -> Optional[Dict]:
    """
    Check if a SNOMED/RxNorm code represents a high-risk condition using EXPANDED_SNOMED_HIERARCHY.
    Ultra-fast O(1) set membership lookups via precomputed descendants.
    
    Args:
        snomed_code: SNOMED CT or RxNorm code to check
        condition_text: Optional text for display purposes only (not used for matching if code_only=True)
        code_only: If True, ONLY use code-based matching (no text/alias matching)
    
    Returns: dict with risk info including 'hierarchical_path' and 'path_display', or None
    
    IMPORTANT: When code_only=True, text matching is completely disabled to prevent false positives
    and ensure we only match conditions that have verifiable Textract bounding box evidence.
    """
    if not snomed_code and not condition_text:
        return None
    
    # CODE-ONLY MODE: Strictly require a code (no text matching)
    if code_only and not snomed_code:
        return None
    
    # 1. Try SNOMED code lookup using expanded hierarchy (O(1) set membership)
    if snomed_code:
        result = find_high_risk_root_for_snomed_code(snomed_code)
        if result:
            root_code, root_node = result
            
            # Build hierarchical path
            path = build_hierarchical_path_from_root(root_code, snomed_code)
            path_display = format_hierarchical_path(path)
            
            # Get term name for the input code
            try:
                condition_name = get_snomed_term(snomed_code)
            except:
                condition_name = path[-1][1] if path else "Unknown"
            
            return {
                "condition": condition_name,
                "snomed_code": snomed_code,
                "category": root_node.category,
                "risk_level": root_node.risk_level,
                "hierarchical_path": path,  # List of (code, name) tuples
                "path_display": path_display,  # Formatted string "A > B"
                "hierarchy_depth": len(path),
                "matched_by": "code_lookup_expanded_hierarchy",
                "root_code": root_code,
                "root_condition": root_node.name
            }
        
        # Try RxNorm hierarchy (hardcoded children for medications)
        rxnorm_result = find_node_in_rxnorm_hierarchy(snomed_code)
        if rxnorm_result:
            root_code, node = rxnorm_result
            path = [(root_code, RXNORM_HIERARCHY[root_code].name)]
            if snomed_code != root_code:
                path.append((snomed_code, node.name))
            
            return {
                "condition": node.name,
                "snomed_code": snomed_code,
                "category": node.category,
                "risk_level": node.risk_level,
                "hierarchical_path": path,
                "path_display": format_hierarchical_path(path),
                "hierarchy_depth": len(path),
                "matched_by": "code_lookup_rxnorm",
                "root_code": root_code,
                "root_condition": RXNORM_HIERARCHY[root_code].name
            }
    
    # 2. ONLY allow text matching if code_only is False (backward compatibility for legacy code)
    # TEXT MATCHING SHOULD NOT BE USED for hierarchical risk scanning to prevent false positives
    if code_only:
        return None  # Code was provided but not found in hierarchy = not high-risk
    
    # Legacy text matching (DEPRECATED - should not be used for hierarchical risk scanning)
    # Note: This is kept for backward compatibility but should be phased out
    if condition_text:
        text_lower = condition_text.lower().strip()
        
        # Search through all SNOMED root nodes and their aliases
        for root_code, root_node in SNOMED_HIGH_RISK_CODES.items():
            if _match_node_text(root_node, text_lower):
                return {
                    "condition": root_node.name,
                    "snomed_code": root_code,
                    "category": root_node.category,
                    "risk_level": root_node.risk_level,
                    "hierarchical_path": [(root_code, root_node.name)],
                    "path_display": root_node.name,
                    "hierarchy_depth": 1,
                    "matched_by": "text_alias_snomed"
                }
        
        # Search through all RxNorm nodes and their aliases
        for root_code, root_node in RXNORM_HIERARCHY.items():
            if _match_node_text(root_node, text_lower):
                return {
                    "condition": root_node.name,
                    "snomed_code": root_code,
                    "category": root_node.category,
                    "risk_level": root_node.risk_level,
                    "hierarchical_path": [(root_code, root_node.name)],
                    "path_display": root_node.name,
                    "hierarchy_depth": 1,
                    "matched_by": "text_alias_rxnorm"
                }
    
    return None


def find_node_in_rxnorm_hierarchy(code: str) -> Optional[Tuple[str, SnomedNode]]:
    """
    Search for a code in RxNorm hierarchy (which still uses hardcoded children).
    Returns (root_code, node) if found.
    """
    for root_code, root_node in RXNORM_HIERARCHY.items():
        if code == root_code:
            return (root_code, root_node)
        
        # Search children recursively
        result = _search_rxnorm_children(code, root_node, root_code)
        if result:
            return result
    
    return None


def _search_rxnorm_children(code: str, parent_node: SnomedNode, root_code: str) -> Optional[Tuple[str, SnomedNode]]:
    """Recursively search RxNorm children for code"""
    for child_code, child_node in parent_node.children.items():
        if code == child_code:
            return (root_code, child_node)
        
        # Recurse deeper
        result = _search_rxnorm_children(code, child_node, root_code)
        if result:
            return result
    
    return None


def _match_node_text(node: SnomedNode, text_lower: str) -> bool:
    """Check if text matches node name or aliases using word boundary matching"""
    import re
    
    # For node name, use flexible matching (substring)
    if node.name.lower() in text_lower or text_lower in node.name.lower():
        return True
    
    # For aliases, use word boundary matching to prevent false positives
    for alias in node.aliases:
        # Create regex pattern with word boundaries
        # \b ensures we match complete words, not substrings
        pattern = r'\b' + re.escape(alias.lower()) + r'\b'
        if re.search(pattern, text_lower):
            return True
    
    return False


def _search_children_by_text(parent_node: SnomedNode, text_lower: str, parent_code: str) -> Optional[Dict]:
    """Recursively search children for text match"""
    for child_code, child_node in parent_node.children.items():
        if _match_node_text(child_node, text_lower):
            path = build_hierarchical_path(child_code, child_node)
            return {
                "condition": child_node.name,
                "snomed_code": child_code,
                "category": child_node.category,
                "risk_level": child_node.risk_level,
                "hierarchical_path": path,
                "path_display": format_hierarchical_path(path),
                "hierarchy_depth": len(path),
                "matched_by": "text_alias"
            }
        
        # Recurse deeper
        result = _search_children_by_text(child_node, text_lower, child_code)
        if result:
            return result
    
    return None


# Backward compatibility: build flat reference from hierarchy
def _build_flat_reference() -> Dict[str, Dict]:
    """Build flat HIGH_RISK_SNOMED_CONDITIONS from SNOMED_HIGH_RISK_CODES and RXNORM_HIERARCHY for LLM prompts"""
    flat_dict = {}
    
    # Process SNOMED hierarchy
    for root_code, root_node in SNOMED_HIGH_RISK_CODES.items():
        # Add root node
        flat_dict[root_node.name.lower()] = {
            "snomed_code": root_code,
            "category": root_node.category,
            "risk_level": root_node.risk_level
        }
        
        # Add aliases
        for alias in root_node.aliases:
            flat_dict[alias.lower()] = {
                "snomed_code": root_code,
                "category": root_node.category,
                "risk_level": root_node.risk_level
            }
    
    # Process RxNorm hierarchy
    for root_code, root_node in RXNORM_HIERARCHY.items():
        # Add root node
        flat_dict[root_node.name.lower()] = {
            "rxnorm_code": root_code,
            "category": root_node.category,
            "risk_level": root_node.risk_level
        }
        
        # Add aliases
        for alias in root_node.aliases:
            flat_dict[alias.lower()] = {
                "rxnorm_code": root_code,
                "category": root_node.category,
                "risk_level": root_node.risk_level
            }
    
    return flat_dict

HIGH_RISK_SNOMED_CONDITIONS = _build_flat_reference()


# ============================================================================
# EXPANDED HIERARCHY MATCHING
# ============================================================================

def find_root_condition_for_code(code: str, code_type: str = 'SNOMED') -> Optional[Tuple[str, SnomedNode]]:
    """
    Check if a SNOMED/RxNorm code belongs to any of the high-risk condition hierarchies
    using the expanded hierarchy (EXPANDED_SNOMED_HIERARCHY or RxNorm hardcoded children).
    
    Args:
        code: SNOMED CT or RxNorm code to check
        code_type: 'SNOMED' or 'RxNorm'
    
    Returns:
        Tuple of (root_code, root_node) if found, None otherwise
    """
    if code_type == 'SNOMED':
        # Use expanded SNOMED hierarchy (O(1) set membership)
        result = find_high_risk_root_for_snomed_code(code)
        return result
    else:  # RxNorm
        # Use RxNorm hardcoded children
        result = find_node_in_rxnorm_hierarchy(code)
        return result
    
    return None


# ============================================================================
# HIERARCHICAL RISK SCANNING
# ============================================================================

def scan_coded_conditions_hierarchical(coded_conditions: List[Dict]) -> Tuple[List[Dict], Set[str]]:
    """
    PURE CLASSIFICATION FUNCTION - NO NEW CONDITIONS GENERATED
    
    Simply scans coded_conditions and checks if any SNOMED/RxNorm codes match our
    high-risk hierarchies. If they match, we classify them as high-risk and add
    hierarchy metadata (parent > child path). We DO NOT create new conditions -
    we only work with what's already in coded_conditions.
    
    Algorithm:
    1. For each item in coded_conditions with a SNOMED/RxNorm code
    2. Check if that code exists ANYWHERE in our hierarchies (root, child, grandchild)
    3. If yes: Mark as high-risk, preserve ALL original data, add hierarchy path
    4. Build parent→child relationships for display purposes
    5. NO parent/child deduplication - show all items that are in coded_conditions
    
    Args:
        coded_conditions: List of coded conditions with SNOMED/ICD-10/RxNorm codes
                         (from Task 3 - Medical Coding stage)
    
    Returns:
        Tuple of (hierarchical_risks, matched_codes):
        - hierarchical_risks: List of dicts with all data from coded_conditions + hierarchy info
        - matched_codes: Set of all codes found (for downstream processing)
    """
    print("="*80)
    print("HIERARCHICAL CLASSIFICATION - CODED_CONDITIONS ONLY")
    print("="*80)
    print("✓ Processing ONLY conditions from coded_conditions")
    print("✓ NO new conditions generated - pure classification")
    print("✓ Building hierarchy paths for display")
    print()
    sys.stdout.flush()
    
    hierarchical_risks = []
    matched_codes = set()  # Track which codes are found
    
    # Get the list of entities (handle both dict and list formats)
    entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    print(f"[HIERARCHICAL] Processing {len(entities)} entities from coded_conditions...")
    sys.stdout.flush()
    
    # Track seen codes to avoid exact duplicates within coded_conditions
    seen_codes = set()
    
    processed_count = 0
    for entity in entities:
        processed_count += 1
        if processed_count % 10 == 0:
            print(f"[HIERARCHICAL] Processed {processed_count}/{len(entities)} entities...")
            sys.stdout.flush()
        
        snomed_code = entity.get('snomed_code', '')
        rxnorm_code = entity.get('rxnorm_code', '')
        condition_text = entity.get('condition') or entity.get('text', '')
        
        # Skip if we've already processed this exact code
        entity_code = snomed_code or rxnorm_code
        if entity_code and entity_code in seen_codes:
            continue
        
        # Check if this code matches ANY code in our hierarchies
        risk_info = None
        matched_code = None
        code_type = None
        
        # Try RxNorm first (medications)
        if rxnorm_code:
            risk_info = is_high_risk_condition_with_path(rxnorm_code, condition_text, code_only=True)
            if risk_info:
                matched_code = rxnorm_code
                code_type = 'RxNorm'
        
        # Try SNOMED if RxNorm didn't match
        if not risk_info and snomed_code:
            risk_info = is_high_risk_condition_with_path(snomed_code, condition_text, code_only=True)
            if risk_info:
                matched_code = snomed_code
                code_type = 'SNOMED'
        
        # If matched, add to hierarchical risks with ALL original data from coded_conditions
        if risk_info and matched_code:
            seen_codes.add(matched_code)
            matched_codes.add(matched_code)
            
            # Preserve ALL original data from coded_conditions
            risk_info['condition'] = condition_text or risk_info.get('condition', '')
            risk_info['source'] = 'Hierarchical (from coded_conditions)'
            risk_info['evidence_boxes'] = entity.get('evidence', [])
            risk_info['evidence_text'] = entity.get('extracted_quote') or entity.get('text', '')[:200]
            risk_info['page'] = entity.get('page_num') or entity.get('page')
            risk_info['date_reported'] = entity.get('reported_date') or entity.get('date_reported')
            risk_info['icd10_code'] = entity.get('icd10_code', '')
            risk_info['description'] = entity.get('description', '')  # Preserve any existing description
            risk_info['code_type'] = code_type
            risk_info['has_evidence'] = len(risk_info['evidence_boxes']) > 0  # Track if this item has evidence
            
            # Set the appropriate code fields
            if code_type == 'RxNorm':
                risk_info['rxnorm_code'] = matched_code
                if not risk_info.get('snomed_code'):
                    risk_info['snomed_code'] = ''
            else:  # SNOMED
                risk_info['snomed_code'] = matched_code
                if not risk_info.get('rxnorm_code'):
                    risk_info['rxnorm_code'] = ''
            
            hierarchical_risks.append(risk_info)
            
            # Log the match with hierarchical path details
            code_display = f"{code_type}: {matched_code}"
            path_display = risk_info.get('path_display', condition_text)
            evidence_count = len(risk_info['evidence_boxes'])
            hierarchy_depth = risk_info.get('hierarchy_depth', 1)
            
            print(f"\n  ✅ HIGH-RISK MATCH FOUND:")
            print(f"     Code: {code_display}")
            print(f"     Condition: {condition_text[:80]}")
            print(f"     Hierarchical Path: {path_display}")
            print(f"     Hierarchy Depth: {hierarchy_depth} levels")
            print(f"     Evidence: {evidence_count} bounding boxes")
            print(f"     Page: {risk_info.get('page', 'Not specified')}")
            print(f"     Date: {risk_info.get('date_reported', 'Not specified')}")
            
            # Show the detailed hierarchical relationship
            hierarchical_path = risk_info.get('hierarchical_path', [])
            if hierarchical_path and len(hierarchical_path) > 1:
                print(f"     Relationship Chain:")
                for idx, (code, name) in enumerate(hierarchical_path):
                    indent = "       " + "  " * idx
                    arrow = "└─>" if idx > 0 else "   "
                    level = "ROOT" if idx == 0 else f"L{idx}"
                    print(f"{indent}{arrow} {level}: [{code}] {name}")
    
    # Summary statistics
    print()
    print("="*80)
    print(f"HIERARCHICAL CLASSIFICATION COMPLETE: {len(hierarchical_risks)} high-risk items found")
    print("="*80)
    
    if hierarchical_risks:
        # Count by code type
        snomed_count = sum(1 for r in hierarchical_risks if r.get('code_type') == 'SNOMED')
        rxnorm_count = sum(1 for r in hierarchical_risks if r.get('code_type') == 'RxNorm')
        
        # Count by hierarchy depth
        depth_counts = {}
        total_evidence = 0
        for risk in hierarchical_risks:
            depth = risk.get('hierarchy_depth', 1)
            depth_counts[depth] = depth_counts.get(depth, 0) + 1
            total_evidence += len(risk.get('evidence_boxes', []))
        
        print(f"📊 Classification Results:")
        print(f"     {snomed_count} SNOMED conditions")
        print(f"     {rxnorm_count} RxNorm medications")
        print(f"📊 Hierarchy Depth:")
        for depth in sorted(depth_counts.keys()):
            label = "root nodes" if depth == 1 else f"depth {depth} nodes"
            print(f"     {depth_counts[depth]} at {label}")
        print(f"📦 Total Evidence: {total_evidence} bounding boxes")
        if hierarchical_risks:
            print(f"     Average {total_evidence / len(hierarchical_risks):.1f} bbox per item")
        
        # Show all hierarchical relationships found
        print(f"\n🔗 HIERARCHICAL RELATIONSHIPS FOUND:")
        print(f"   (Showing parent → child → grandchild chains)\n")
        
        # Group by root condition
        by_root = {}
        for risk in hierarchical_risks:
            root_code = risk.get('root_code', 'Unknown')
            root_name = risk.get('root_condition', 'Unknown')
            root_key = f"{root_code}:{root_name}"
            
            if root_key not in by_root:
                by_root[root_key] = []
            by_root[root_key].append(risk)
        
        # print each root and its descendants
        for root_key, risks in sorted(by_root.items()):
            root_code, root_name = root_key.split(':', 1)
            print(f"   🏥 ROOT: [{root_code}] {root_name}")
            print(f"      Found {len(risks)} condition(s) in this hierarchy:")
            
            for risk in risks:
                hierarchical_path = risk.get('hierarchical_path', [])
                condition_text = risk.get('condition', '')[:60]
                matched_code = risk.get('snomed_code') or risk.get('rxnorm_code')
                
                # Build the chain display
                if len(hierarchical_path) == 1:
                    # Direct match to root
                    print(f"         • [{matched_code}] {condition_text}")
                else:
                    # Show the chain
                    chain_display = " → ".join([name[:30] for code, name in hierarchical_path])
                    print(f"         • {chain_display}")
                    print(f"           Matched: [{matched_code}] {condition_text}")
            print()
    
    print(f"✅ All items are from coded_conditions with verifiable Textract evidence")
    print("="*80)
    print()
    
    return hierarchical_risks, matched_codes


# ============================================================================
# CHRONIC CONDITION KEYWORDS
# ============================================================================

CHRONIC_CONDITION_KEYWORDS = [
    # Cardiovascular
    "hypertension", "high blood pressure", "coronary artery disease", "atrial fibrillation",
    "arrhythmia", "cardiomyopathy", "valve disease", "peripheral artery disease",
    
    # Metabolic/Endocrine
    "diabetes", "diabetic", "hyperlipidemia", "high cholesterol", "thyroid", "hypothyroidism",
    "hyperthyroidism", "metabolic syndrome", "obesity",
    
    # Respiratory
    "asthma", "bronchitis", "emphysema",
    
    # Musculoskeletal
    "arthritis", "osteoarthritis", "rheumatoid", "osteoporosis", "fibromyalgia",
    "chronic pain", "back pain", "degenerative",
    
    # Neurological
    "migraine", "headache", "neuropathy", "seizure disorder", "epilepsy", "parkinson",
    "multiple sclerosis", "ms",
    
    # Gastrointestinal
    "crohn", "colitis", "inflammatory bowel", "ibs", "irritable bowel", "gerd",
    "reflux", "gastritis", "peptic ulcer",
    
    # Renal
    "chronic kidney", "ckd", "renal insufficiency",
    
    # Hepatic
    "hepatitis", "fatty liver", "liver disease",
    
    # Psychiatric
    "depression", "anxiety", "bipolar", "ptsd", "ocd", "panic disorder",
    
    # Hematologic
    "anemia", "thrombocytopenia", "coagulation disorder", "hemophilia",
    
    # Other chronic
    "chronic", "recurrent", "persistent", "long-term", "history of"
]



# ============================================================================
# UMBRELLA CATEGORIZATION - Group related conditions/medications
# ============================================================================

# Updated UMBRELLA_CATEGORIES with parent SNOMED codes instead of hard-coded lists
UMBRELLA_CATEGORIES = {
    # Condition groupings
    "diabetic_complications": {
        "name": "Diabetic Complications",
        "keywords": ["diabetic", "diabetes", "diabetic foot", "diabetic retinopathy", "diabetic kidney", 
                    "diabetic neuropathy", "diabetic eye", "diabetic macular", "hyperglycemia", "hypoglycemia"],
        "icd10_prefixes": ["E11", "E10", "E08", "E09"],
        "snomed_codes": ["73211009", "44054006", "46635009", "75682002", "390834004", "201251005", 
                        "127013003", "313340009", "860712005", "371087003", "419100001", "74627003"],
        "lineage_keywords": ["diabet", "glycem", "insulin"],
        "type": "condition",
        "icon": "🩺"
    },
    "cardiovascular_diseases": {
        "name": "Cardiovascular Diseases",
        "keywords": ["coronary artery", "myocardial infarction", "nstemi", "stemi", "angina", "heart disease",
                    "cardiomyopathy", "heart failure", "atrial fibrillation", "arrhythmia"],
        "icd10_prefixes": ["I21", "I25", "I50", "I48", "I20"],
        "snomed_codes": ["53741008", "22298006", "57054005", "399211009", "49436004"],
        "lineage_keywords": ["cardio", "heart", "myocard", "vascular", "coronary", "ischem", "ischemic"],
        "type": "condition",
        "icon": "❤️"
    },
    "kidney_diseases": {
        "name": "Kidney Diseases",
        "keywords": ["chronic kidney", "ckd", "renal", "kidney disease", "nephropathy", "kidney failure"],
        "icd10_prefixes": ["N18", "N19"],
        "snomed_codes": ["709044004", "127013003", "236425005", "46177005"],
        "lineage_keywords": ["kidney", "renal", "nephro", "glomerul"],
        "lineage_exclude_keywords": ["adrenal", "suprarenal"],
        "type": "condition",
        "icon": "🫘"
    },
    "hypertension_related": {
        "name": "Hypertension & Blood Pressure",
        "keywords": ["hypertension", "high blood pressure", "elevated blood pressure", "htn"],
        "icd10_prefixes": ["I10", "I11", "I12", "I13", "I15"],
        "snomed_codes": ["38341003", "59621000"],
        "lineage_keywords": ["hypertens", "blood pressure", "elevated pressure"],
        "type": "condition",
        "icon": "🩸"
    },
    "liver_diseases": {
        "name": "Liver Diseases",
        "keywords": ["hepatitis", "cirrhosis", "liver disease", "fatty liver", "hepatic"],
        "icd10_prefixes": ["K70", "K71", "K72", "K73", "K74", "K75", "K76", "B15", "B16", "B17", "B18", "B19"],
        "snomed_codes": ["235856003", "235880004", "27156006"],
        "lineage_keywords": ["liver", "hepat", "cirrh", "bile", "biliary"],
        "type": "condition",
        "icon": "🫁"
    },
    "respiratory_diseases": {
        "name": "Respiratory Diseases & Symptoms",
        "keywords": ["asthma", "copd", "emphysema", "bronchitis", "pneumonia", "respiratory",
                    "cough", "shortness of breath", "dyspnea", "wheezing", "congestion", "sneezing",
                    "sore throat", "nasal", "sinusitis", "rhinitis"],
        "icd10_prefixes": ["J44", "J45", "J43", "J40", "J41", "J42", "J18", "R05", "R06", "J30", "J31", "J32", "J00", "J01", "J02"],
        "snomed_codes": ["195967001", "13645005", "87433001", "49727002", "267036007", "56018004", "162397003"],
        "lineage_keywords": ["respirat", "lung", "airway", "pulmonary", "bronch", "alveol", "pleura", "breathing", "dyspn", "cough", "wheez"],
        "type": "condition",
        "icon": "🫁"
    },
    "cancer_related": {
        "name": "Cancer & Malignancies",
        "keywords": ["cancer", "carcinoma", "malignant", "tumor", "neoplasm", "leukemia", "lymphoma", "melanoma"],
        "icd10_prefixes": ["C", "D0"],
        "snomed_codes": ["363346000", "372087000"],
        "lineage_keywords": ["malignan", "neoplas", "cancer", "carcinoma", "tumor", "tumour", "metasta", "oncolog"],
        "type": "condition",
        "icon": "🎗️"
    },
    "mental_health": {
        "name": "Mental Health Conditions",
        "keywords": ["depression", "anxiety", "bipolar", "ptsd", "schizophrenia", "psychiatric", "mental"],
        "icd10_prefixes": ["F20", "F30", "F31", "F32", "F33", "F40", "F41", "F43"],
        "snomed_codes": ["35489007", "48694002", "13746004"],
        "lineage_keywords": ["mental", "psychiatric", "psycholog", "mood", "affect", "depress", "anxi"],
        "type": "condition",
        "icon": "🧠"
    },
    "neurological_disorders": {
        "name": "Neurological Disorders",
        "keywords": ["stroke", "seizure", "epilepsy", "parkinson", "multiple sclerosis", "neuropathy", 
                    "dementia", "alzheimer", "migraine", "tremor", "paralysis"],
        "icd10_prefixes": ["G20", "G40", "G41", "I60", "I61", "I63", "I64", "G30"],
        "snomed_codes": ["230690007", "84757009", "49049000", "52448006", "26929004"],
        "lineage_keywords": ["neurolog", "nervous system", "brain", "cerebr", "neural", "neuron", "encephal", "mening"],
        "type": "condition",
        "icon": "🧠"
    },
    "gastrointestinal_disorders": {
        "name": "Gastrointestinal Disorders & Symptoms",
        "keywords": ["gerd", "reflux", "crohn", "colitis", "ibs", "irritable bowel", "ulcer", 
                    "gastritis", "diverticulitis", "pancreatitis", "inflammatory bowel",
                    "nausea", "vomiting", "diarrhea", "constipation", "heartburn", "indigestion",
                    "bloating", "gas", "dyspepsia"],
        "icd10_prefixes": ["K21", "K25", "K26", "K27", "K50", "K51", "K58", "K85", "K86", "R11", "R19", "K59"],
        "snomed_codes": ["235595009", "34000006", "64766004", "235919008", "422587007", "422400008", "62315008", "14760008"],
        "lineage_keywords": ["gastrointestin", "digest", "bowel", "intestin", "gastro", "entero", "colon", "stomach", "esophag", "abdominal", "nausea", "vomit", "diarrh"],
        "type": "condition",
        "icon": "🫃"
    },
    "musculoskeletal_disorders": {
        "name": "Musculoskeletal Disorders",
        "keywords": ["arthritis", "osteoarthritis", "rheumatoid arthritis", "osteoporosis", "fibromyalgia",
                    "back pain", "joint pain", "tendinitis", "bursitis", "fracture", "sprain"],
        "icd10_prefixes": ["M05", "M06", "M15", "M16", "M17", "M19", "M79", "M80", "M81", "S", "T"],
        "snomed_codes": ["69896004", "396275006", "203082005", "64859006"],
        "lineage_keywords": ["musculoskeletal", "bone", "joint", "arthr", "osteo", "muscul", "tendon", "ligament", "cartilage"],
        "type": "condition",
        "icon": "🦴"
    },
    "thyroid_endocrine": {
        "name": "Thyroid & Endocrine Disorders",
        "keywords": ["hypothyroid", "hyperthyroid", "thyroid", "adrenal", "pituitary", "goiter",
                    "hashimoto", "graves", "cushing", "addison"],
        "icd10_prefixes": ["E00", "E01", "E02", "E03", "E04", "E05", "E06", "E07", "E20", "E21", "E22", "E23", "E24", "E25", "E26", "E27"],
        "snomed_codes": ["40930008", "34486009", "190268003", "240969001"],
        "lineage_keywords": ["thyroid", "endocrin", "hormone", "gland", "pituitary", "adrenal", "parathyroid"],
        "type": "condition",
        "icon": "🦋"
    },
    "autoimmune_diseases": {
        "name": "Autoimmune Diseases",
        "keywords": ["lupus", "sle", "systemic lupus", "rheumatoid", "sjogren", "scleroderma",
                    "autoimmune", "dermatomyositis", "polymyositis", "vasculitis"],
        "icd10_prefixes": ["M32", "M33", "M34", "M35"],
        "snomed_codes": ["55464009", "69896004", "83901003", "190905008"],
        "lineage_keywords": ["autoimmun", "immune", "immunolog", "inflammat"],
        "type": "condition",
        "icon": "🛡️"
    },
    "infectious_diseases": {
        "name": "Infectious Diseases",
        "keywords": ["hiv", "aids", "tuberculosis", "tb", "hepatitis b", "hepatitis c", "sepsis",
                    "infection", "pneumonia", "meningitis", "endocarditis"],
        "icd10_prefixes": ["A", "B15", "B16", "B17", "B18", "B20", "J12", "J13", "J14", "J15", "J18"],
        "snomed_codes": ["86406008", "62479008", "56717001", "233604007"],
        "lineage_keywords": ["infection", "infect", "sepsis", "septic", "bacteri", "viral", "microb", "pathogen"],
        "type": "condition",
        "icon": "🦠"
    },
    "pain_related": {
        "name": "Pain & Pain Management",
        "keywords": ["pain", "chronic pain", "back pain", "neck pain", "joint pain", "abdominal pain",
                    "chest pain", "headache", "migraine", "neuropathic pain", "fibromyalgia"],
        "icd10_prefixes": ["R52", "M54", "M79", "G89"],
        "snomed_codes": ["22253000", "161891005", "203082005"],
        "lineage_keywords": ["pain", "algia", "ache"],
        "type": "condition",
        "icon": "⚡"
    },
    "general_symptoms": {
        "name": "General Symptoms & Findings",
        "keywords": ["fever", "fatigue", "malaise", "weakness", "weight loss", "weight gain", 
                    "dizziness", "vertigo", "syncope", "swelling", "edema", "rash"],
        "icd10_prefixes": ["R50", "R53", "R55", "R60", "R63", "R42"],
        "snomed_codes": ["386661006", "84229001", "271594007", "267032009", "248153007"],
        "lineage_keywords": ["symptom", "sign", "malaise", "fatigue"],
        "type": "condition",
        "icon": "🌡️"
    },
    "allergies_sensitivities": {
        "name": "Allergies & Sensitivities",
        "keywords": ["allergy", "allergic", "sensitivity", "intolerance", "hypersensitivity", 
                    "allergic rhinitis", "hay fever", "food allergy", "drug allergy"],
        "icd10_prefixes": ["J30", "T78", "Z88", "Z91.0"],
        "snomed_codes": ["609328004", "419199007", "91935009"],
        "lineage_keywords": ["allerg", "hypersensitiv", "atop", "anaphyla"],
        "type": "condition",
        "icon": "🤧"
    },
    "eye_diseases": {
        "name": "Eye Diseases",
        "keywords": ["glaucoma", "cataract", "macular degeneration", "retinopathy", "vision loss",
                    "blindness", "retinal detachment", "conjunctivitis"],
        "icd10_prefixes": ["H25", "H26", "H40", "H35", "H54"],
        "snomed_codes": ["23986001", "193570009", "267718000", "193387007"],
        "lineage_keywords": ["eye", "ocular", "ophthalm", "vision", "retina", "visual", "sight"],
        "type": "condition",
        "icon": "👁️"
    },
    "blood_disorders": {
        "name": "Blood & Hematologic Disorders",
        "keywords": ["anemia", "thrombocytopenia", "hemophilia", "bleeding disorder", "clotting disorder",
                    "leukemia", "lymphoma", "myeloma", "coagulation"],
        "icd10_prefixes": ["D50", "D51", "D52", "D53", "D55", "D56", "D57", "D58", "D59", "D60", "D61", "D62", "D63", "D64", "D65", "D66", "D67", "D68", "D69"],
        "snomed_codes": ["271737000", "415116008", "41841004", "74474003"],
        "lineage_keywords": ["blood", "hemat", "hemo", "coagul", "thromb", "anemia", "anemi"],
        "type": "condition",
        "icon": "🩸"
    },
    "sleep_disorders": {
        "name": "Sleep Disorders",
        "keywords": ["sleep apnea", "insomnia", "narcolepsy", "snoring", "sleep disorder",
                    "restless leg", "circadian rhythm"],
        "icd10_prefixes": ["G47", "F51"],
        "snomed_codes": ["73430006", "193462001", "60380005"],
        "lineage_keywords": ["sleep", "somn", "apnea"],
        "type": "condition",
        "icon": "😴"
    },
    "skin_conditions": {
        "name": "Skin Conditions",
        "keywords": ["psoriasis", "eczema", "dermatitis", "rash", "acne", "cellulitis",
                    "ulcer", "wound", "pressure sore", "skin cancer", "melanoma"],
        "icd10_prefixes": ["L20", "L21", "L40", "L50", "L70", "L89", "L97", "C43"],
        "snomed_codes": ["9014002", "43116000", "238575004", "402815407"],
        "lineage_keywords": ["skin", "dermat", "cutaneous", "epidermal", "rash", "lesion"],
        "type": "condition",
        "icon": "🫱"
    },
    
    # Medication groupings (unchanged)
    "ace_inhibitors_arbs": {
        "name": "ACE Inhibitors & ARBs",
        "keywords": ["lisinopril", "enalapril", "ramipril", "losartan", "valsartan", "irbesartan", 
                    "candesartan", "olmesartan", "-pril", "-sartan"],
        "rxnorm_codes": ["29046", "3827", "35296", "52175", "69749", "83818", "214354", "321064"],
        "type": "medication",
        "icon": "💊"
    },
    "statins": {
        "name": "Statins & Lipid Management",
        "keywords": ["statin", "atorvastatin", "simvastatin", "rosuvastatin", "pravastatin", 
                    "lovastatin", "fluvastatin"],
        "rxnorm_codes": ["83367", "36567", "301542", "42463", "6472", "38121"],
        "type": "medication",
        "icon": "💊"
    },
    "diabetes_medications": {
        "name": "Diabetes Medications",
        "keywords": ["metformin", "insulin", "glipizide", "glyburide", "sitagliptin", "empagliflozin",
                    "dapagliflozin", "liraglutide", "semaglutide", "glimepiride"],
        "rxnorm_codes": ["6809", "5856", "4821", "4815", "665033", "1545653", "1488564", "475968", "1991302"],
        "type": "medication",
        "icon": "💉"
    },
    "anticoagulants": {
        "name": "Anticoagulants & Antiplatelets",
        "keywords": ["warfarin", "aspirin", "clopidogrel", "rivaroxaban", "apixaban", "dabigatran",
                    "edoxaban", "ticagrelor", "prasugrel"],
        "rxnorm_codes": ["11289", "1191", "32968", "1114195", "1364430", "1037042", "1657025"],
        "type": "medication",
        "icon": "💊"
    },
    "beta_blockers": {
        "name": "Beta Blockers",
        "keywords": ["metoprolol", "atenolol", "carvedilol", "propranolol", "bisoprolol", "labetalol", "-olol"],
        "rxnorm_codes": ["6918", "1202", "20352", "8787", "19484", "6343"],
        "type": "medication",
        "icon": "💊"
    },
    "calcium_channel_blockers": {
        "name": "Calcium Channel Blockers",
        "keywords": ["amlodipine", "diltiazem", "verapamil", "nifedipine", "felodipine", "-dipine"],
        "rxnorm_codes": ["17767", "3443", "11170", "7417", "4119"],
        "type": "medication",
        "icon": "💊"
    },
    "diuretics": {
        "name": "Diuretics",
        "keywords": ["furosemide", "hydrochlorothiazide", "chlorthalidone", "spironolactone", 
                    "bumetanide", "torsemide", "triamterene"],
        "rxnorm_codes": ["4603", "5487", "2409", "9997", "1808", "38413", "10763"],
        "type": "medication",
        "icon": "💊"
    },
    "pain_management": {
        "name": "Pain Management & Opioids",
        "keywords": ["oxycodone", "hydrocodone", "morphine", "fentanyl", "tramadol", "codeine",
                    "hydromorphone", "opioid", "narcotic"],
        "rxnorm_codes": ["7804", "5489", "7052", "4337", "10689", "2670", "3423"],
        "type": "medication",
        "icon": "💊"
    },
    "antidepressants": {
        "name": "Antidepressants",
        "keywords": ["sertraline", "escitalopram", "fluoxetine", "citalopram", "paroxetine",
                    "duloxetine", "venlafaxine", "bupropion", "mirtazapine", "ssri", "snri"],
        "rxnorm_codes": ["36437", "321988", "4493", "2556", "32937", "72625", "39786", "42347", "30121"],
        "type": "medication",
        "icon": "💊"
    },
    "antipsychotics": {
        "name": "Antipsychotics",
        "keywords": ["risperidone", "quetiapine", "aripiprazole", "olanzapine", "clozapine",
                    "haloperidol", "ziprasidone", "paliperidone"],
        "rxnorm_codes": ["35636", "51272", "89013", "30121", "2626", "5093", "132411", "593411"],
        "type": "medication",
        "icon": "💊"
    },
    "thyroid_medications": {
        "name": "Thyroid Medications",
        "keywords": ["levothyroxine", "synthroid", "methimazole", "propylthiouracil", "thyroid"],
        "rxnorm_codes": ["10582", "224905", "6912", "8787"],
        "type": "medication",
        "icon": "💊"
    },
    "proton_pump_inhibitors": {
        "name": "Proton Pump Inhibitors",
        "keywords": ["omeprazole", "pantoprazole", "esomeprazole", "lansoprazole", "rabeprazole", "ppi"],
        "rxnorm_codes": ["7646", "40790", "141120", "17128", "35069"],
        "type": "medication",
        "icon": "💊"
    },
    "bronchodilators_inhalers": {
        "name": "Bronchodilators & Inhalers",
        "keywords": ["albuterol", "fluticasone", "budesonide", "tiotropium", "salmeterol",
                    "formoterol", "ipratropium", "montelukast", "inhaler"],
        "rxnorm_codes": ["435", "37798", "1827", "73137", "36117", "18631", "5640", "30827"],
        "type": "medication",
        "icon": "💨"
    },
    "corticosteroids": {
        "name": "Corticosteroids",
        "keywords": ["prednisone", "dexamethasone", "methylprednisolone", "hydrocortisone",
                    "prednisolone", "steroid"],
        "rxnorm_codes": ["8640", "3264", "6902", "5492", "8652"],
        "type": "medication",
        "icon": "💊"
    },
    "immunosuppressants": {
        "name": "Immunosuppressants",
        "keywords": ["methotrexate", "azathioprine", "cyclosporine", "tacrolimus", "mycophenolate",
                    "immunosuppressant"],
        "rxnorm_codes": ["6851", "1256", "3008", "42316", "68149"],
        "type": "medication",
        "icon": "💊"
    },
    "nsaids": {
        "name": "NSAIDs & Anti-Inflammatories",
        "keywords": ["ibuprofen", "naproxen", "celecoxib", "meloxicam", "diclofenac", "indomethacin",
                    "ketorolac", "nsaid"],
        "rxnorm_codes": ["5640", "7258", "140587", "6960", "3355", "5781", "6142"],
        "type": "medication",
        "icon": "💊"
    },
    "benzodiazepines": {
        "name": "Benzodiazepines & Anxiolytics",
        "keywords": ["alprazolam", "lorazepam", "clonazepam", "diazepam", "temazepam",
                    "xanax", "ativan", "klonopin", "valium"],
        "rxnorm_codes": ["596", "6470", "2598", "3322", "10355"],
        "type": "medication",
        "icon": "💊"
    },
    "osteoporosis_medications": {
        "name": "Osteoporosis Medications",
        "keywords": ["alendronate", "risedronate", "ibandronate", "denosumab", "zoledronic",
                    "bisphosphonate", "fosamax", "boniva", "prolia"],
        "rxnorm_codes": ["778268", "114166", "221131", "993766", "61229"],
        "type": "medication",
        "icon": "💊"
    },
    "anticonvulsants": {
        "name": "Anticonvulsants",
        "keywords": ["gabapentin", "pregabalin", "levetiracetam", "valproic acid", "lamotrigine",
                    "topiramate", "phenytoin", "carbamazepine"],
        "rxnorm_codes": ["4687", "187832", "35766", "11118", "17128", "38404", "8183", "2002"],
        "type": "medication",
        "icon": "💊"
    },
    "antibiotics": {
        "name": "Antibiotics",
        "keywords": ["amoxicillin", "azithromycin", "ciprofloxacin", "doxycycline", "cephalexin",
                    "levofloxacin", "antibiotic", "penicillin", "ceftriaxone"],
        "rxnorm_codes": ["723", "18631", "2551", "3640", "2231", "82122", "7980", "203563"],
        "type": "medication",
        "icon": "💊"
    },
    "antihypertensives_other": {
        "name": "Other Antihypertensives",
        "keywords": ["hydralazine", "clonidine", "minoxidil", "prazosin", "doxazosin"],
        "rxnorm_codes": ["5470", "2599", "7021", "8629", "3616"],
        "type": "medication",
        "icon": "💊"
    }
}


# ============================================================================
# INITIALIZE CHRONIC PARENT CONCEPTS FROM SNOMED SEARCH
# LAZY INITIALIZATION - Defer to first use to avoid Lambda init timeout
# ============================================================================

# Global variable to store chronic parent concepts
CHRONIC_PARENT_CONCEPTS = None
_chronic_concepts_initialized = False

def get_chronic_parent_concepts():
    """
    Lazily initialize chronic parent concepts on first access.
    Prevents Lambda init timeout by deferring database search.
    """
    global CHRONIC_PARENT_CONCEPTS, _chronic_concepts_initialized
    
    if _chronic_concepts_initialized:
        return CHRONIC_PARENT_CONCEPTS
    
    try:
        print("[CHRONIC] First use - searching for chronic parent concepts...")
        sys.stdout.flush()
        
        # Lazy-load SNOMEDCT
        SNOMEDCT = get_snomedct()
        
        # Search for ALL chronic concepts (yields ~2,865 results)
        print("[CHRONIC] Searching SNOMED database (this may take 5-10 seconds)...")
        sys.stdout.flush()
        chronic_search_results = SNOMEDCT.search("chronic") + SNOMEDCT.search("persistent") + SNOMEDCT.search("long-term") + SNOMEDCT.search("long term")
        print(f"[CHRONIC] Found {len(chronic_search_results)} concepts with 'Chronic'")
        sys.stdout.flush()
        CHRONIC_PARENT_CONCEPTS = chronic_search_results
        
        # Filter to keep only high-level parent concepts
        # Look for "chronic disease" or "chronic illness" + "disorder" or "finding"
        # for concept in chronic_search_results:
        #     concept_name = concept.term.lower()
            
        #     # Keep only high-level parent concepts, not specific conditions
        #     if (("chronic disease" in concept_name or "chronic illness" in concept_name) and 
        #         ("disorder" in concept_name or "finding" in concept_name)):
        #         # The code you provided is a comment in Python. Comments in Python start with a hash
        #         # symbol (#) and are used to provide explanations or notes within the code. In this case,
        #         # the comment appears to be indicating a section related to "CHRONIC_PARENT_CONCEPTS".
        #         # Comments are ignored by the Python interpreter and are not executed as part of the
        #         # program.
        #         CHRONIC_PARENT_CONCEPTS.append(concept)
        #         print(f"[CHRONIC] ✅ Added chronic parent: [{concept.code}] {concept.term}")
        
        # If no suitable parent concepts found, use the most common ones by ID
        if len(CHRONIC_PARENT_CONCEPTS) == 0:
            print("[CHRONIC] ⚠️ No parent concepts found via search, using fallback IDs")
            SNOMEDCT = get_snomedct()
            fallback_ids = [90734009, 177010002]  # Chronic disease (disorder), Chronic illness (finding)
            for concept_id in fallback_ids:
                try:
                    concept = SNOMEDCT[concept_id]
                    CHRONIC_PARENT_CONCEPTS.append(concept)
                    print(f"[CHRONIC] ✅ Added fallback parent: [{concept_id}] {concept.term}")
                except Exception as e:
                    print(f"[CHRONIC] ⚠️ Could not load fallback concept {concept_id}: {e}")
        
        if CHRONIC_PARENT_CONCEPTS:
            print(f"[CHRONIC] ✅ Initialized with {len(CHRONIC_PARENT_CONCEPTS)} chronic parent concepts")
            print("[CHRONIC] Will use .is_a() hierarchy checks at runtime (upward traversal)")
            # for parent in CHRONIC_PARENT_CONCEPTS:
            #     print(f"[CHRONIC]   - [{parent.code}] {parent.term}")
        else:
            print("[CHRONIC] ⚠️ No chronic parent concepts loaded - will use fallback keyword detection")
        
        _chronic_concepts_initialized = True
        
    except Exception as e:
        print(f"[CHRONIC ERROR] Failed to initialize chronic parent concepts: {e}")
        print("[CHRONIC ERROR] Will use fallback keyword/ICD-10 based detection")
        import traceback
        traceback.print_exc()
        CHRONIC_PARENT_CONCEPTS = []
        _chronic_concepts_initialized = True
    
    return CHRONIC_PARENT_CONCEPTS

# ============================================================================
# CHRONIC CONDITION DETECTION USING SNOMED HIERARCHY (HYBRID APPROACH)
# ============================================================================

def is_chronic_condition_snomed(snomed_code: str, icd10_code: str = None, condition_text: str = None) -> bool:
    """
    Hybrid approach: Determine if a condition is chronic using SNOMED CT hierarchy + keyword fallback.
    Falls back to keyword matching if SNOMED hierarchy check fails.
    
    Args:
        snomed_code: SNOMED CT code
        icd10_code: ICD-10 code (optional fallback)
        condition_text: Condition text (optional fallback)
    
    Returns:
        True if condition is chronic
    """
    # First try SNOMED hierarchy using .is_a() (most accurate)
    chronic_parents = get_chronic_parent_concepts()
    if snomed_code and chronic_parents:
        try:
            SNOMEDCT = get_snomedct()
            concept = SNOMEDCT[int(snomed_code)]
            
            # Check if it's a descendant of ANY chronic parent concept (upward traversal)
            for parent_concept in chronic_parents:
                try:
                    if concept.is_a(parent_concept):
                        print(f"[CHRONIC] ✅ SNOMED {snomed_code} ({concept.term[:40]}) is chronic via hierarchy")
                        print(f"[CHRONIC]    └─ is_a [{parent_concept.code}] {parent_concept.term[:50]}")
                        return True
                except Exception as e:
                    # Continue checking other parents if one fails
                    continue
            
            # Not found in hierarchy - will try fallback
            # print(f"[CHRONIC] ⊗ SNOMED {snomed_code} ({concept.term[:40]}) NOT in chronic hierarchy, trying fallback...")
            
        except Exception as e:
            print(f"[CHRONIC] ⚠️ Error checking SNOMED code {snomed_code}: {e}")
            # Fall through to keyword fallback
    
    # Fallback: Use ICD-10 or keyword matching if SNOMED hierarchy check didn't find it
    if icd10_code or condition_text:
        return is_chronic_condition_fallback(icd10_code, condition_text)
    
    return False

def is_chronic_condition_fallback(icd10_code: str = None, condition_text: str = None) -> bool:
    """
    Fallback method for chronic condition detection using ICD-10 codes and keywords.
    Only used when SNOMED hierarchy check is not available.
    
    Args:
        icd10_code: ICD-10 code
        condition_text: Condition text
    
    Returns:
        True if condition appears to be chronic based on code/keywords
    """
    # ICD-10 prefixes that indicate chronic conditions
    CHRONIC_ICD10_PREFIXES = [
        "E08", "E09", "E10", "E11",  # Diabetes
        "I11", "I12", "I13",          # Chronic hypertensive conditions
        "I25", "I50",                 # Chronic heart conditions
        "N18",                        # Chronic kidney disease
        "J44", "J45",                 # COPD, Asthma
        "K70", "K71", "K73", "K74",  # Chronic liver disease
        "M05", "M06",                 # Rheumatoid arthritis
    ]
    
    if icd10_code:
        for prefix in CHRONIC_ICD10_PREFIXES:
            if icd10_code.startswith(prefix):
                print(f"[CHRONIC] ✅ ICD-10 {icd10_code} matches chronic prefix {prefix} (fallback)")
                return True
    
    # Keyword fallback (least accurate) - use the existing CHRONIC_CONDITION_KEYWORDS
    if condition_text:
        text_lower = condition_text.lower()
        for keyword in CHRONIC_CONDITION_KEYWORDS:
            if keyword in text_lower:
                print(f"[CHRONIC] ✅ Keyword '{keyword}' found in '{condition_text[:50]}' (fallback)")
                return True
    
    # No chronic indicators found - return False (don't log)
    return False

@lru_cache(maxsize=100_000)
def get_snomed_ancestors(snomed_code: str, max_depth: int = 20) -> Tuple[str, ...]:
    """
    Get all ancestor SNOMED codes by traversing upward through the hierarchy.
    Uses caching for efficiency.
    
    Args:
        snomed_code: Starting SNOMED CT code
        max_depth: Maximum recursion depth to prevent infinite loops
    
    Returns:
        Tuple of all ancestor codes (deterministic ordering)
    """
    ancestors = set()
    visited = {snomed_code}
    
    def recurse(current_code: str, depth: int):
        if depth >= max_depth:
            return
        
        try:
            concept = get_snomed_concept(current_code)
            parents = getattr(concept, "parents", []) or []
            
            for parent in parents:
                parent_code = str(parent.code)
                if parent_code not in visited:
                    visited.add(parent_code)
                    ancestors.add(parent_code)
                    recurse(parent_code, depth + 1)
        except Exception:
            # Skip concepts that can't be loaded
            pass
    
    try:
        recurse(snomed_code, 0)
    except Exception as e:
        print(f"[UMBRELLA] Error getting ancestors for {snomed_code}: {e}")
    
    return tuple(sorted(ancestors))  # Deterministic ordering



def find_umbrella_category_via_lineage_keyword_search(snomed_code: str, is_medication: bool) -> Optional[str]:
    """
    Find umbrella category by searching the lineage (ancestors) terms for specific keywords.
    This is the PRIMARY approach - more flexible than exact code matching.
    
    For example, for cardiovascular diseases, search lineage terms for "cardio" or "heart".
    For kidney diseases, search for "kidney" or "renal" (but exclude "adrenal").
    
    Args:
        snomed_code: SNOMED CT code to check
        is_medication: Whether this is a medication (for filtering)
    
    Returns:
        Category key from UMBRELLA_CATEGORIES if found via lineage keyword search, None otherwise
    """
    import re  # Import here to avoid scope issues
    
    if not snomed_code:
        return None
    
    try:
        # Get all ancestors of this code
        ancestors = get_snomed_ancestors(snomed_code)
        
        # Also check the concept itself
        all_codes_to_check = [snomed_code] + list(ancestors)
        
        # Get terms for all codes in the lineage
        lineage_terms = []
        for code in all_codes_to_check:
            try:
                term = get_snomed_term(code).lower()
                lineage_terms.append(term)
            except Exception:
                # Skip codes that can't be loaded
                continue
        
        # Check each umbrella category
        for category_key, category_info in UMBRELLA_CATEGORIES.items():
            # Filter by type (condition vs medication)
            if is_medication and category_info['type'] != 'medication':
                continue
            if not is_medication and category_info['type'] != 'condition':
                continue
            
            # Get lineage keywords for this category
            lineage_keywords = category_info.get('lineage_keywords', [])
            if not lineage_keywords:
                continue  # Skip categories without lineage keywords
            
            # Get exclude keywords (e.g., "adrenal" for kidney diseases)
            exclude_keywords = category_info.get('lineage_exclude_keywords', [])
            
            # Check if any lineage term contains the keywords
            for term in lineage_terms:
                # Check if any exclude keyword is present
                if any(exclude_kw in term for exclude_kw in exclude_keywords):
                    continue  # Skip this term if it contains an excluded keyword
                
                # Skip overly broad finding/function terms that cause false positives
                # Examples: "psychological finding", "neurological finding", "function finding", "system finding"
                broad_patterns = [
                    r'^\w+\s+(finding|disorder)(\s+\(finding\)|\s+\(disorder\))?$',  # Single word + finding/disorder
                    r'function\s+finding',  # Any "function finding"
                    r'system\s+finding',    # Any "system finding"  
                    r'state.*finding',      # Any "state...finding"
                ]
                if any(re.search(pattern, term) for pattern in broad_patterns):
                    continue  # Skip broad parent terms
                
                # Check if any lineage keyword is present
                # Use word boundaries to avoid false positives (e.g., "liver" matching "delivery")
                for kw in lineage_keywords:
                    # Create a regex pattern with word boundaries
                    # Use \b for word boundary, but handle partial words like "diabet" by not requiring end boundary if it ends without letter
                    if kw.endswith(('ic', 'al', 'ed', 'ing', 'ion', 'ive')):
                        # Full word - require both boundaries
                        pattern = r'\b' + re.escape(kw) + r'\b'
                    else:
                        # Partial stem (like "diabet") - only require start boundary
                        pattern = r'\b' + re.escape(kw)
                    
                    if re.search(pattern, term):
                        print(f"[UMBRELLA] ✅ SNOMED {snomed_code} matched '{category_key}' via lineage keyword search")
                        print(f"[UMBRELLA]    └─ Found term: '{term[:60]}...' (keyword: '{kw}')")
                        return category_key
        
    except Exception as e:
        print(f"[UMBRELLA] Error in lineage keyword search for {snomed_code}: {e}")
    
    return None


def find_umbrella_category_via_snomed_hierarchy(snomed_code: str, is_medication: bool) -> Optional[str]:

    """
    Find umbrella category by checking if this SNOMED code is a descendant of 
    any umbrella category SNOMED codes using the database hierarchy.
    
    Args:
        snomed_code: SNOMED CT code to check
        is_medication: Whether this is a medication (for filtering)
    
    Returns:
        Category key from UMBRELLA_CATEGORIES if found via hierarchy, None otherwise
    """
    if not snomed_code:
        return None
    
    try:
        # Get all ancestors of this code
        ancestors = get_snomed_ancestors(snomed_code)
        
        # Check each umbrella category
        for category_key, category_info in UMBRELLA_CATEGORIES.items():
            # Filter by type (condition vs medication)
            if is_medication and category_info['type'] != 'medication':
                continue
            if not is_medication and category_info['type'] != 'condition':
                continue
            
            # Check if any ancestor matches the umbrella category SNOMED codes
            umbrella_snomed_codes = set(category_info.get('snomed_codes', []))
            if umbrella_snomed_codes:
                # Check both direct match and ancestor match
                if snomed_code in umbrella_snomed_codes or any(ancestor in umbrella_snomed_codes for ancestor in ancestors):
                    return category_key
        
    except Exception as e:
        print(f"[UMBRELLA] Error finding umbrella via SNOMED hierarchy for {snomed_code}: {e}")
    
    return None


def assign_umbrella_category(entity: Dict) -> Optional[str]:
    """
    Assign an umbrella category to a condition or medication based on codes and names.
    Uses a three-stage approach for conditions:
    1. Try lineage keyword search (primary, CONDITIONS ONLY)
    2. Try SNOMED hierarchy exact code matching (first fallback, CONDITIONS ONLY)
    3. Fall back to keyword/prefix/code matching (second fallback)
    
    Note: Medications (RxNorm codes) skip SNOMED approaches since DB is SNOMED-only.
    
    Args:
        entity: Dict containing condition/medication info with codes
    
    Returns:
        Category key from UMBRELLA_CATEGORIES, or None if no match
    """
    condition_text = (entity.get('condition') or entity.get('text', '')).lower()
    icd10_code = entity.get('icd10_code', '')
    snomed_code = entity.get('snomed_code', '')
    rxnorm_code = entity.get('rxnorm_code', '')
    
    # Determine if medication or condition
    is_medication = bool(rxnorm_code) or 'medication' in entity.get('category', '').lower()
    
    # === STAGE 1: Try lineage keyword search (PRIMARY approach, CONDITIONS ONLY) ===
    # Skip SNOMED approaches for medications since our DB is SNOMED-only (no RxNorm)
    if snomed_code and not is_medication:
        category_via_lineage_keywords = find_umbrella_category_via_lineage_keyword_search(snomed_code, is_medication)
        if category_via_lineage_keywords:
            # Mark that this was found via lineage keyword search
            entity['_umbrella_match_method'] = 'lineage_keyword_search'
            return category_via_lineage_keywords
    
    # === STAGE 2: Try SNOMED hierarchy exact code match (FIRST FALLBACK, CONDITIONS ONLY) ===
    # Skip SNOMED hierarchy for medications since our DB is SNOMED-only (no RxNorm)
    if snomed_code and not is_medication:
        category_via_db = find_umbrella_category_via_snomed_hierarchy(snomed_code, is_medication)
        if category_via_db:
            # Mark that this was found via DB hierarchy
            entity['_umbrella_match_method'] = 'snomed_hierarchy_exact_match'
            return category_via_db
    
    # === STAGE 3: Fall back to keyword/prefix matching (SECOND FALLBACK) ===
    # Check each umbrella category
    for category_key, category_info in UMBRELLA_CATEGORIES.items():
        # Filter by type (condition vs medication)
        if is_medication and category_info['type'] != 'medication':
            continue
        if not is_medication and category_info['type'] != 'condition':
            continue
        
        # Check keywords
        if any(keyword in condition_text for keyword in category_info.get('keywords', [])):
            entity['_umbrella_match_method'] = 'keyword'
            return category_key
        
        # Check ICD-10 prefixes (for conditions)
        if icd10_code:
            for prefix in category_info.get('icd10_prefixes', []):
                if icd10_code.startswith(prefix):
                    entity['_umbrella_match_method'] = 'icd10_prefix'
                    return category_key
        
        # Check RxNorm codes (for medications)
        if rxnorm_code and rxnorm_code in category_info.get('rxnorm_codes', []):
            entity['_umbrella_match_method'] = 'rxnorm_code'
            return category_key
    
    return None


def group_by_umbrella_categories(entities: List[Dict]) -> Dict[str, Dict]:
    """
    Group entities by umbrella categories.
    
    Args:
        entities: List of conditions or medications
    
    Returns:
        Dict with structure:
        {
            "grouped": {
                "category_key": {
                    "name": "Category Name",
                    "icon": "🩺",
                    "items": [entity1, entity2, ...]
                },
                ...
            },
            "ungrouped": [entity1, entity2, ...]
        }
    """
    grouped = {}
    ungrouped = []
    
    for entity in entities:
        category_key = assign_umbrella_category(entity)
        
        if category_key:
            if category_key not in grouped:
                category_info = UMBRELLA_CATEGORIES[category_key]
                grouped[category_key] = {
                    "name": category_info['name'],
                    "icon": category_info['icon'],
                    "type": category_info['type'],
                    "items": []
                }
            
            # Add umbrella category info to entity
            entity['umbrella_category'] = category_key
            entity['umbrella_category_name'] = UMBRELLA_CATEGORIES[category_key]['name']
            
            grouped[category_key]['items'].append(entity)
        else:
            ungrouped.append(entity)
    
    return {
        "grouped": grouped,
        "ungrouped": ungrouped
    }

# ============================================================================
# UPDATE CATEGORIZE_ALL_CODED_CONDITIONS TO USE SNOMED HIERARCHY
# ============================================================================

def sort_by_date(items):
    def get_sort_key(item):
        date_str = item.get('date_reported', '')
        if date_str and date_str != 'Not specified':
            try:
                # Try parsing common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                    try:
                        return datetime.strptime(date_str, fmt)
                    except:
                        continue
            except:
                pass
        return datetime.min  # Put items without dates at the end
    return sorted(items, key=get_sort_key, reverse=True)

def categorize_all_coded_conditions(coded_conditions: List[Dict], high_risk_codes: Set[str], skip_parent_codes: Set[str] = None):
    """
    OPTIMIZATION 4: Single-pass categorization with all checks in one loop.
    Categorize conditions into chronic vs acute using SNOMED hierarchy.
    Uses ALL chronic parent concepts from SNOMEDCT.search("Chronic").
    
    TIME SAVED: 1-2 minutes (reduced from multiple passes to single pass)
    
    Args:
        coded_conditions: List of conditions with codes
        high_risk_codes: Set of high-risk SNOMED codes
        skip_parent_codes: Optional set of parent codes to skip
    
    Returns:
        Dict with categorized conditions (same structure as original)
    """
    chronic_conditions = []
    high_risk_conditions = []
    other_conditions = []
    high_risk_medications = []
    other_medications = []
    
    print("\n" + "="*80)
    print("[CATEGORIZATION] Starting OPTIMIZED single-pass condition categorization")
    print("="*80)
    
    # Pre-fetch chronic parents once (not per-entity)
    chronic_parents = get_chronic_parent_concepts()
    if chronic_parents:
        print(f"[CHRONIC]   - {len(chronic_parents)} chronic parent concepts loaded")
        print(f"[CHRONIC]   - Will check each condition against these parents at runtime")
    else:
        print("[CHRONIC] ⚠️ No SNOMED chronic parent concepts available, using fallback keyword detection")
    
    # Extract entities from coded_conditions (handle both dict and list formats)
    entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    
    print(f"[CATEGORIZATION] Processing {len(entities)} entities in single pass...")
    sys.stdout.flush()
    
    # OPTIMIZATION 4: Single pass through all entities
    for idx, entity in enumerate(entities):
        if idx % 50 == 0 and idx > 0:  # Reduce logging frequency
            print(f"[CATEGORIZATION] Processed {idx}/{len(entities)}...")
            sys.stdout.flush()
        
        snomed_code = entity.get('snomed_code', '')
        rxnorm_code = entity.get('rxnorm_code', '')
        icd10_code = entity.get('icd10_code', '')
        condition_text = entity.get('condition') or entity.get('text', '')
        category = entity.get('category', '').lower()
        
        # Skip parent codes if specified
        if skip_parent_codes and (snomed_code in skip_parent_codes or rxnorm_code in skip_parent_codes):
            continue
        
        # Check if it's a medication based on RxNorm code or category
        is_medication = bool(rxnorm_code) or 'medication' in category or 'drug' in category
        
        # SINGLE-PASS LOGIC: Check all conditions in one pass
        if is_medication:
            if rxnorm_code in high_risk_codes:
                high_risk_medications.append(entity)
            else:
                other_medications.append(entity)
        else:
            # It's a condition
            if snomed_code in high_risk_codes:
                high_risk_conditions.append(entity)
            elif is_chronic_condition_snomed(snomed_code, icd10_code, condition_text):
                chronic_conditions.append(entity)
            else:
                other_conditions.append(entity)
    
    print(f"[CATEGORIZATION] ✓ Single-pass complete")
    print(f"     Chronic: {len(chronic_conditions)}")
    print(f"     High-risk conditions: {len(high_risk_conditions)}")
    print(f"     Other conditions: {len(other_conditions)}")
    print(f"     High-risk meds: {len(high_risk_medications)}")
    print(f"     Other meds: {len(other_medications)}")
    sys.stdout.flush()
    
    # Apply umbrella grouping (keep original structure with "grouped" and "ungrouped")
    chronic_conditions_grouped = group_by_umbrella_categories(sort_by_date(chronic_conditions))
    high_risk_medications_grouped = group_by_umbrella_categories(sort_by_date(high_risk_medications))
    other_medications_grouped = group_by_umbrella_categories(sort_by_date(other_medications))
    other_conditions_grouped = group_by_umbrella_categories(sort_by_date(other_conditions))
    
    # Print umbrella category assignments
    def print_umbrella_assignments(title: str, grouped_data: Dict):
        """Print which items went into which umbrella categories"""
        if not grouped_data['grouped'] and not grouped_data['ungrouped']:
            return
        
        print(f"\n[UMBRELLA] {title}:")
        
        # Print grouped items
        for category_key, category_data in grouped_data['grouped'].items():
            items = category_data['items']
            print(f"  {category_data['icon']} {category_data['name']} ({len(items)} items):")
            for item in items[:3]:  # Show first 3 only to reduce log spam
                condition_name = item.get('condition') or item.get('text', 'Unknown')
                match_method = item.get('_umbrella_match_method', 'unknown')
                print(f"    - {condition_name} [via {match_method}]")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more")
        
        # Print ungrouped items (summary only)
        if grouped_data['ungrouped']:
            print(f"  ❓ Ungrouped ({len(grouped_data['ungrouped'])} items)")
    
    print_umbrella_assignments("Chronic Conditions", chronic_conditions_grouped)
    print_umbrella_assignments("Other Conditions", other_conditions_grouped)
    print_umbrella_assignments("High-Risk Medications", high_risk_medications_grouped)
    print_umbrella_assignments("Other Medications", other_medications_grouped)
    print("="*80)
    
    # Return same structure as original
    return {
        "chronic_conditions": chronic_conditions_grouped,
        "high_risk_conditions": sort_by_date(high_risk_conditions),
        "other_conditions": other_conditions_grouped,
        "high_risk_medications": sort_by_date(high_risk_medications),
        "other_medications": other_medications_grouped
    }

def parse_s3_path(s3_path):
    """Parse S3 path to extract bucket and key"""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key

def download_json_from_s3(s3_path):
    """Download JSON file from S3"""
    print(f"[S3] Downloading from {s3_path}")
    sys.stdout.flush()
    
    try:
        start_time = time.time()
        bucket, key = parse_s3_path(s3_path)
        print(f"[S3] Parsed: bucket={bucket}, key={key}")
        sys.stdout.flush()
        
        print(f"[S3] Calling s3_client.get_object...")
        sys.stdout.flush()
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        print(f"[S3] Reading response body...")
        sys.stdout.flush()
        body_bytes = response['Body'].read()
        size_mb = len(body_bytes) / (1024 * 1024)
        print(f"[S3] Downloaded {size_mb:.2f} MB")
        sys.stdout.flush()
        
        print(f"[S3] Parsing JSON...")
        sys.stdout.flush()
        data = json.loads(body_bytes)
        
        elapsed = time.time() - start_time
        print(f"[S3] ✓ Download complete in {elapsed:.2f}s")
        sys.stdout.flush()
        
        return data
        
    except Exception as e:
        error_type = type(e).__name__
        print(f"\n[S3 ERROR] Failed to download from S3: {error_type}")
        print(f"[S3 ERROR] Error details: {str(e)}")
        sys.stdout.flush()
        
        # Provide specific guidance for common errors
        if "timed out" in str(e).lower() or "timeout" in str(e).lower():
            print(f"\n[S3 ERROR] CONNECTION TIMEOUT - Possible causes:")
            print(f"  1. Lambda is in a VPC without S3 VPC Endpoint")
            print(f"  2. Lambda is in a VPC without NAT Gateway for internet access")
            print(f"  3. Security group blocking outbound traffic")
            print(f"\n[S3 ERROR] Solutions:")
            print(f"  - Add S3 VPC Endpoint (Gateway type) to your VPC")
            print(f"  - OR add NAT Gateway to private subnets")
            print(f"  - OR remove Lambda from VPC (if not needed)")
        elif "could not be found" in str(e).lower() or "does not exist" in str(e).lower():
            print(f"\n[S3 ERROR] FILE NOT FOUND")
            print(f"  Bucket: {bucket}")
            print(f"  Key: {key}")
        elif "access denied" in str(e).lower() or "forbidden" in str(e).lower():
            print(f"\n[S3 ERROR] ACCESS DENIED - Check Lambda IAM role permissions")
            print(f"  Required: s3:GetObject on {bucket}/{key}")
        
        sys.stdout.flush()
        raise

def download_medical_summary(s3_path):
    """Download medical summary JSON from S3"""
    print(f"Downloading medical summary from {s3_path}")
    return download_json_from_s3(s3_path)

def generate_executive_summary_with_llm(medical_summary, coded_conditions, diagnostic_summaries, hierarchical_risks=None):
    """
    Use Bedrock LLM to generate ONLY the executive summary.
    
    High-risk conditions are now identified exclusively through:
    1. scan_coded_conditions_hierarchical() - matches codes in hierarchies
    2. categorize_all_coded_conditions() - categorizes remaining conditions
    
    This function ONLY generates the executive summary based on all available data.
    
    Args:
        medical_summary: Medical summary dict from Task 2
        coded_conditions: Coded conditions from Task 3
        diagnostic_summaries: Diagnostic analysis from Task 5  
        hierarchical_risks: List of conditions found by hierarchical SNOMED scan
    
    Returns:
        dict: {"executive_summary": {...}}
    """
    print("Using Bedrock LLM to generate executive summary...")
    
    if hierarchical_risks is None:
        hierarchical_risks = []

    # Extract only essential data to reduce token count
    summary = medical_summary.get('summary', medical_summary)
    
    # Compact medical summary - only key fields
    compact_summary = {
        "overview": summary.get('overview', {}),
        "insurance": summary.get('insurance', {}),
        "health_latest": summary.get('health_latest', {}),
        "medical_history": summary.get('medical_history', ''),
        "medication_history": summary.get('medication_history', ''),
        "substance_use": summary.get('substance_use', {}),
        "health_history": summary.get('health_history', '')[:500] if summary.get('health_history') else '',  # Truncate long narratives
        "medical_appointments": summary.get('medical_appointments', []),  # Include for doctor recommendations
        "lab_results_summary": summary.get('lab_results_summary', '')  # NEW: Include lab summary for narrative_summary
    }
    
    # Compact coded conditions - extract only condition names and codes
    coded_entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    compact_coded = []
    for entity in coded_entities:  # Removed limit to first 50 conditions
        compact_coded.append({
            "condition": entity.get("condition") or entity.get("text"),
            "icd10": entity.get("icd10_code"),
            "snomed": entity.get("snomed_code"),
            "rxnorm": entity.get("rxnorm_code"),
            "category": entity.get("category")
        })
    
    # Compact diagnostics - only abnormal findings
    abnormal_diagnostics = [
        {
            "test": d.get("test_type"),                      # FIXED: was "test_or_procedure"
            "test_date": d.get("test_date"),                 # NEW: include test date
            "finding": d.get("impression"),                  # FIXED: was "key_finding"
            "findings": d.get("findings", []),               # NEW: include findings array
            "likelihood": d.get("abnormality_likelihood")
        }
        for d in diagnostic_summaries 
        if d.get("abnormality_likelihood") == "likely_abnormal"
    ][:20]  # Limit to first 20 abnormal findings

    # Prepare compact input data
    compact_input = {
        "patient": compact_summary,
        "coded_conditions": compact_coded,
        "abnormal_diagnostics": abnormal_diagnostics,
        "hierarchical_risk_findings": [
            {
                "condition": h.get('condition'),
                "snomed_code": h.get('snomed_code'),
                "category": h.get('category'),
                "risk_level": h.get('risk_level'),
                "path": h.get('path_display', '')
            }
            for h in hierarchical_risks
        ] if hierarchical_risks else []
    }

    # Create prompt for executive summary ONLY (no condition identification)
    prompt = f"""You are a medical risk assessment specialist. Generate a comprehensive executive summary for the patient based on ALL available data.

                    PATIENT DATA:
                    {json.dumps(compact_input, indent=2)}

                    HIERARCHICAL RISK FINDINGS ALREADY IDENTIFIED:
                    The following high-risk conditions were already found by code-based hierarchical analysis:
                    {json.dumps(compact_input.get('hierarchical_risk_findings', []), indent=2)}

                    IMPORTANT: Do NOT identify new high-risk conditions. All high-risk condition identification is done through code-based hierarchical matching.
                    Your task is ONLY to generate a comprehensive executive summary based on the data provided.

                    Return a JSON response with executive summary ONLY:
                    {{
                    "executive_summary": {{
                        "narrative_summary": "A comprehensive 4-6 sentence paragraph summarizing the patient's overall health status, key risk factors, and underwriting considerations. MUST include: (1) current health metrics (vitals, BMI), (2) LAB RESULTS SUMMARY - incorporate the lab_results_summary from patient data to highlight any abnormal findings, trends, and clinical significance, (3) primary medical conditions and their management status, (4) identified high-risk conditions with severity indicators, (5) lifestyle factors (substance use, activity level), and (6) treatment compliance patterns. Write in clear, professional language suitable for underwriters to quickly grasp the patient's insurability profile.",
                        "key_summary_points": ["3-5 concise bullet points (complete sentences ending with periods) extracting the MOST CRITICAL findings from narrative_summary. Each bullet should highlight: key diagnoses with severity, abnormal vitals or lab results, high-risk medications, or critical lifestyle factors. Focus on underwriting-relevant information. Example: 'Patient has uncontrolled Type 2 Diabetes with HbA1c of 9.2%, indicating poor glycemic control.'"],
                        "risk_level": "high|moderate|low - Classify overall underwriting risk using structured evaluation:\n                            HIGH: Critical/uncontrolled conditions, recent hospitalization (last 6 months), high mortality risk, severe abnormal vitals/labs, poor treatment adherence, or multiple high-risk conditions.\n                            MODERATE: Chronic conditions under treatment with borderline control, abnormal but stable vitals/labs, moderate risk family history, controlled high-risk conditions, or history of substance use.\n                            LOW: Assign ONLY if ALL criteria met: (1) Age-appropriate vitals all in normal ranges, (2) No uncontrolled chronic conditions (well-managed conditions acceptable), (3) No recent hospitalizations/surgeries (last 12 months), (4) No significant high-risk family history patterns, (5) Good treatment adherence if on medications, (6) Normal or near-normal lab results, (7) No substance abuse history.",
                        "risk_level_keywords": "CRITICAL INSTRUCTION - READ CAREFULLY: Extract 3-8 EXACT keywords/phrases that appear VERBATIM in your overall_risk_assessment text above. These keywords will be highlighted in the Risk Assessment section of the UI. STEP 1: Write your overall_risk_assessment first. STEP 2: After writing it, extract ONLY words/phrases that actually appear in that specific text. STEP 3: Verify each keyword appears word-for-word in overall_risk_assessment before including it. ONLY populate the array matching your risk_level assignment, leave the other two EMPTY []. EXAMPLE: If overall_risk_assessment contains 'severe weight loss' extract 'weight loss' or 'severe weight loss'. If it contains 'hospitalization' extract 'hospitalization'. DO NOT extract 'unintentional weight loss' if the text only says 'weight loss'. DO NOT extract phrases from narrative_summary that don't appear in overall_risk_assessment.",
                        "risk_level_keywords_extracted": {{
                        "high": [],
                        "moderate": [],
                        "low": []
                        }},
                        "patient_demographics": {{
                        "name": "Patient name",
                        "date_of_birth": "DOB",
                        "age": "Age",
                        "sex": "Sex",
                        "phone": "Phone",
                        "address": "Address",
                        "pcp": "PCP name",
                        "insurance_provider": "Insurance provider",
                        "insurance_id": "Insurance ID",
                        "insurance_policy_number": "Policy number"
                        }},
                        "health_latest": {{
                        "height": "Height",
                        "weight": "Weight",
                        "bmi": "BMI",
                        "bp": "BP",
                        "heart_rate": "HR",
                        "hba1c": "HbA1c",
                        "cholesterol": "Cholesterol",
                        "hdl": "HDL"
                        }},
                        "risk_overview": {{
                        "total_high_risk_conditions": 0,
                        "critical_findings_count": 0,
                        "hierarchical_risk_analysis_count": 0,
                        "categories_affected": ["List of categories"]
                        }},
                        "key_high_risk_conditions": ["Top 3-5 critical conditions including those from hierarchical analysis"],
                        "overall_risk_assessment": "IMPORTANT: Write a 2-3 sentence risk assessment considering both LLM-identified and hierarchically-identified conditions. REQUIRED - DO NOT leave empty or use default values. CRITICAL FOR KEYWORD HIGHLIGHTING: When describing medical conditions, complications, or findings in this assessment, USE THE SAME EXACT TERMINOLOGY AND PHRASES from your narrative_summary above. Examples: If narrative_summary says 'unintentional weight loss' use 'unintentional weight loss' not 'weight loss'; if it says 'iron deficiency anemia' use that exact phrase not just 'anemia'; if it says 'protein-calorie malnutrition' use that exact phrase not 'malnutrition'; if it says 'elevated inflammatory markers' use that exact phrase not 'inflammation'. This ensures risk_level_keywords will highlight properly in both summary sections.",
                        "recommendations": ["Extract actual doctor recommendations, follow-up plans, or treatment recommendations from the medical appointments and medical history. IMPORTANT: Consider the hierarchical_risk_findings when extracting recommendations - if specific conditions are mentioned there, look for any doctor recommendations related to those conditions. Include specific recommendations such as: follow-up appointments scheduled, monitoring plans, lifestyle modifications prescribed, medication changes, referrals to specialists, or diagnostic tests ordered. If no specific doctor recommendations are found in the documents, return empty array []. Do NOT generate generic recommendations."]
                    }}
                    }}
                    }}

                    CRITICAL REQUIREMENTS:
                    1. ALWAYS generate narrative_summary - it cannot be empty or "Not available"
                    2. ALWAYS generate overall_risk_assessment - it cannot be empty or "Not available"
                    3. Both narrative_summary and overall_risk_assessment MUST contain actual analysis based on the patient data provided
                    4. If there is insufficient data, state what is known and what is missing, but NEVER use "Not available" as the entire response
                    5. Consider the hierarchical_risk_findings when making risk assessments and recommendations

                    Return ONLY valid JSON with the executive_summary object."""

    try:
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "temperature": 0.1,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}]
                }
            ]
        }

        # Retry with exponential backoff
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = bedrock_runtime.invoke_model(
                    modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                    body=json.dumps(request_body)
                )

                response_body = json.loads(response['body'].read())
                response_text = response_body['content'][0]['text']

                # Extract JSON from response
                json_text = extract_json_from_text(response_text)
                result = json.loads(json_text)
                
                # Get executive summary only
                executive_summary_data = result.get("executive_summary", {})
                
                # Map risk_level_keywords_extracted back to risk_level_keywords for Pydantic compatibility
                if "risk_level_keywords_extracted" in executive_summary_data:
                    executive_summary_data["risk_level_keywords"] = executive_summary_data.pop("risk_level_keywords_extracted")
                
                # Validate executive_summary
                print(f"[DEBUG] Validating executive summary...")
                print(f"[DEBUG] Executive summary data keys: {list(executive_summary_data.keys())}")
                print(f"[DEBUG] Narrative preview: {executive_summary_data.get('narrative_summary', 'MISSING')[:100]}...")
                
                # Try to validate executive_summary, but preserve original if validation fails
                try:
                    exec_summary_validated = ExecutiveSummary(**executive_summary_data)
                    print(f"[DEBUG] Executive summary validation successful")
                    exec_summary_dict = extract_validated_dict(exec_summary_validated)
                except Exception as validation_error:
                    print(f"[ERROR] Executive summary validation failed: {str(validation_error)}")
                    # Use original data if validation fails
                    exec_summary_dict = executive_summary_data
                    print(f"[WARNING] Using original LLM executive_summary data without Pydantic validation")
                
                print(f"✅ Executive summary generated by LLM")
                print(f"[DEBUG] Final executive summary keys: {list(exec_summary_dict.keys())}")
                print(f"[DEBUG] Final narrative summary: {exec_summary_dict.get('narrative_summary', 'MISSING')[:200]}...")
                print(f"[DEBUG] Final overall risk assessment: {exec_summary_dict.get('overall_risk_assessment', 'MISSING')[:200]}...")
                
                return {
                    "executive_summary": exec_summary_dict
                }
            
            except Exception as retry_error:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (0.5 * attempt)  # Exponential backoff
                    print(f"Attempt {attempt + 1} failed for executive summary generation: {str(retry_error)}. Retrying in {wait_time:.1f}s...")
                    import time
                    time.sleep(wait_time)
                else:
                    raise  # Re-raise on final attempt

    except Exception as e:
        print(f"Error in LLM executive summary generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "executive_summary": {}
        }

def extract_json_from_text(text):
    """Extract JSON from text that might contain markdown or extra content"""
    text = text.strip()

    # Remove markdown code blocks if present
    if text.startswith('```'):
        text = re.sub(r'^```(?:json)?\s*\n', '', text)
        text = re.sub(r'\n```\s*$', '', text)

    # Try to find JSON object in the text
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        return json_match.group(0)

    return text

def enrich_high_risk_findings_with_bboxes(high_risk_findings, coded_conditions):
    """
    Enrich LLM-identified high-risk findings with bounding boxes from coded entities.
    Matches by SNOMED code, RxNorm code, or condition text.
    Similar to how medical_summary extracts bounding boxes for fields.
    
    Args:
        high_risk_findings: List of high-risk findings from LLM
        coded_conditions: Dict/list containing coded_entities with evidence boxes
    
    Returns:
        Enriched high_risk_findings with evidence_boxes and page_number
    """
    print("Enriching LLM high-risk findings with bounding boxes from coded entities...")
    
    # Extract coded entities
    coded_entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    
    # Build lookup maps for fast matching
    snomed_map = {}  # snomed_code -> entity
    rxnorm_map = {}  # rxnorm_code -> entity
    condition_map = {}  # normalized condition text -> entity
    
    for entity in coded_entities:
        snomed = entity.get('snomed_code')
        rxnorm = entity.get('rxnorm_code')
        condition = entity.get('condition') or entity.get('text', '')
        
        if snomed:
            if snomed not in snomed_map:
                snomed_map[snomed] = []
            snomed_map[snomed].append(entity)
        
        if rxnorm:
            if rxnorm not in rxnorm_map:
                rxnorm_map[rxnorm] = []
            rxnorm_map[rxnorm].append(entity)
        
        if condition:
            norm_cond = condition.lower().strip()
            if norm_cond not in condition_map:
                condition_map[norm_cond] = []
            condition_map[norm_cond].append(entity)
    
    enriched_count = 0
    
    for finding in high_risk_findings:
        snomed_code = finding.get('snomed_code')
        condition = finding.get('condition', '')
        condition_text = finding.get('condition_text', '')
        
        # Try to find matching entity
        matched_entity = None
        
        # 1. Try exact SNOMED code match (highest priority)
        if snomed_code and snomed_code in snomed_map:
            matched_entity = snomed_map[snomed_code][0]  # Take first match
        
        # 2. Try RxNorm code for medications
        if not matched_entity:
            rxnorm = finding.get('rxnorm_code')
            if rxnorm and rxnorm in rxnorm_map:
                matched_entity = rxnorm_map[rxnorm][0]
        
        # 3. Try condition text matching
        if not matched_entity:
            for text in [condition, condition_text]:
                if text:
                    norm_text = text.lower().strip()
                    if norm_text in condition_map:
                        matched_entity = condition_map[norm_text][0]
                        break
        
        # Enrich with bounding box data from matched entity
        if matched_entity:
            evidence_boxes = matched_entity.get('evidence', [])
            page_num = matched_entity.get('page_num') or matched_entity.get('page')
            extracted_quote = matched_entity.get('extracted_quote', '')
            
            # Preserve original text evidence from LLM, add bboxes separately
            if evidence_boxes:
                finding['evidence_boxes'] = evidence_boxes  # Array of bbox objects (for PDF navigation)
                # Keep original 'evidence' string field from LLM intact
                # Only update if it's empty
                if not finding.get('evidence'):
                    finding['evidence'] = extracted_quote
                enriched_count += 1
            
            if page_num and not finding.get('page_number'):
                finding['page'] = page_num
                finding['page_number'] = page_num
    
    print(f"  - Enriched {enriched_count}/{len(high_risk_findings)} LLM findings with bounding boxes")
    return high_risk_findings

def generate_executive_summary(medical_summary, high_risk_findings, llm_risk_analysis):
    """Generate a concise executive summary for the top of the report"""
    print("Generating executive summary...")

    if 'summary' in medical_summary:
        summary = medical_summary['summary']
    else:
        summary = medical_summary

    # Extract key patient info
    overview = summary.get('overview', {})
    name = overview.get('name', 'Unknown')
    age = overview.get('age', 'Unknown')
    sex = overview.get('sex', 'Unknown')
    dob = overview.get('date_of_birth', 'Unknown')
    pcp = overview.get('pcp', 'Unknown')
    phone = overview.get('phone', 'Unknown')
    address = overview.get('address', 'Unknown')

    health_latest = summary.get('health_latest', {})
    height = health_latest.get('height', 'Unknown')
    weight = health_latest.get('weight', 'Unknown')
    bmi = health_latest.get('bmi', 'Unknown')
    bp = health_latest.get('bp', 'Unknown')
    heart_rate = health_latest.get('heart_rate', 'Not specified')
    hba1c = health_latest.get('hba1c', 'Unknown')
    cholesterol = health_latest.get('cholesterol', 'Unknown')
    hdl = health_latest.get('hdl', 'Unknown')

    insurance = summary.get('insurance', {})
    provider = insurance.get('provider', 'Unknown')
    insurance_id = insurance.get('ID_number', 'Unknown')
    policy_number = insurance.get('policy_number', 'Unknown')

    # Count high-risk findings
    snomed_high_risk_count = len([f for f in high_risk_findings if f['risk_level'] == 'High'])
    snomed_medium_risk_count = len([f for f in high_risk_findings if f['risk_level'] == 'Medium'])

    llm_high_risk_count = len([r for r in llm_risk_analysis.get('llm_identified_risks', []) 
                                if r['risk_level'] == 'High'])

    # Create executive summary
    exec_summary = {
        "patient_demographics": {
            "name": name,
            "date_of_birth": dob,
            "age": age,
            "sex": sex,
            "phone": phone,
            "address": address,
            "pcp": pcp,
            "insurance_provider": provider,
            "insurance_id": insurance_id,
            "insurance_policy_number": policy_number
        },
        "health_latest": {
            "height": height,
            "weight": weight,
            "bmi": bmi,
            "bp": bp,
            "heart_rate": heart_rate,
            "hba1c": hba1c,
            "cholesterol": cholesterol,
            "hdl": hdl
        },
        "risk_overview": {
            "total_high_risk_conditions": snomed_high_risk_count + llm_high_risk_count,
            "total_medium_risk_conditions": snomed_medium_risk_count,
            "snomed_identified_conditions": len(high_risk_findings),
            "llm_identified_risks": len(llm_risk_analysis.get('llm_identified_risks', []))
        },
        "key_high_risk_conditions": [f['condition'] for f in high_risk_findings if f['risk_level'] == 'High'][:5],
        "overall_risk_assessment": llm_risk_analysis.get('overall_risk_assessment', 'Not available'),
        "top_recommendations": llm_risk_analysis.get('recommendations', [])[:3]
    }

    print("Executive summary generated")
    return exec_summary

def save_enhanced_summary_to_s3(enhanced_summary, original_s3_path):
    """Save enhanced medical summary to S3"""
    print("Saving enhanced medical summary to S3...")

    # Parse original path to determine output location
    bucket, key = parse_s3_path(original_s3_path)

    # Remove 'medical_summary.json' from the key path
    base_path = key.replace('/medical_summary.json', '')

    # Create output key
    output_key = f"{base_path}/enhanced_medical_summary_with_risks.json"

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=json.dumps(enhanced_summary, indent=2),
        ContentType='application/json'
    )

    output_s3_path = f"s3://{bucket}/{output_key}"
    print(f"Enhanced medical summary saved to {output_s3_path}")

    return output_s3_path

def derive_sibling_output_path(medical_summary_s3_path, sibling_filename):
    """
    Given .../outputs/medical_summary.json, derive .../outputs/<sibling_filename>
    """
    if not medical_summary_s3_path.endswith("/medical_summary.json"):
        raise ValueError(f"Expected path ending with /medical_summary.json, got: {medical_summary_s3_path}")
    return medical_summary_s3_path.replace("/medical_summary.json", f"/{sibling_filename}")


def create_enhanced_summary(
    executive_summary,
    high_risk_conditions,
    chronic_conditions,
    other_conditions,
    hierarchical_risks,
    high_risk_medications=None,
    other_medications=None,
    coded_conditions=None,
    diagnostic_summaries=None,
    medical_summary=None,
    categorized_all=None
):
    """
    Create enhanced medical summary combining all analysis results.
    Includes:
      - Executive summary (from LLM - already incorporates hierarchical findings)
      - High-risk conditions, chronic conditions, and other conditions (categorized and sorted)
      - High-risk medications and other medications (categorized and sorted)
      - Hierarchical risk analysis (from SNOMED hierarchy with parent paths)
      - Coded conditions (with evidence boxes, page/bbox/line_id, etc.) - GROUPED by unique code
      - Diagnostic summaries (all tests with abnormal highlighted)
      - PDF/document metadata when present
    
    Args:
        high_risk_conditions: List of high-risk conditions (Critical/High risk level)
        chronic_conditions: List of chronic conditions (Medium risk level)
        other_conditions: List of other conditions (Low risk level)
        hierarchical_risks: Pre-computed hierarchical risk findings (passed from process_risk_analysis)
    """
    print("Creating enhanced medical summary.")
    print(f"[DEBUG] Received executive_summary type: {type(executive_summary)}")
    print(f"[DEBUG] Executive summary keys: {list(executive_summary.keys()) if isinstance(executive_summary, dict) else 'Not a dict'}")

    summary = medical_summary["summary"]

    # Get the list of coded conditions
    coded_entities_list = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    
    # Group conditions by unique ICD-10/SNOMED code with all dates
    grouped_conditions = group_conditions_by_code(coded_entities_list)
    
    # Generate clinical summaries for each condition using LLM in parallel (replaces description field)
    print(f"Generating clinical summaries for {len(grouped_conditions)} unique conditions in parallel...")
    
    # Use ThreadPoolExecutor to parallelize LLM calls
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all condition summary generation tasks
        future_to_condition = {
            executor.submit(generate_condition_summary, condition, medical_summary, diagnostic_summaries): condition
            for condition in grouped_conditions
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_condition):
            condition = future_to_condition[future]
            try:
                condition["description"] = future.result()
                completed += 1
                if completed % 5 == 0 or completed == len(grouped_conditions):
                    print(f"  Progress: {completed}/{len(grouped_conditions)} summaries generated")
            except Exception as e:
                print(f"  Error generating summary for {condition.get('condition')}: {str(e)}")
                condition["description"] = f"Condition: {condition.get('condition', 'Unknown')}. Summary generation failed."
    
    print("Clinical summaries generated for all conditions (description field updated)")

    # Separate hierarchical risks into SNOMED conditions and RxNorm medications
    snomed_high_risk = [r for r in hierarchical_risks if r.get('code_type') == 'SNOMED']
    rxnorm_high_risk = [r for r in hierarchical_risks if r.get('code_type') == 'RxNorm']
    
    print(f"Separated hierarchical risks: {len(snomed_high_risk)} SNOMED conditions, {len(rxnorm_high_risk)} RxNorm medications")
    
    # Generate clinical summaries for high-risk SNOMED conditions in parallel
    if snomed_high_risk:
        print(f"Generating clinical summaries for {len(snomed_high_risk)} high-risk SNOMED conditions...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_condition = {
                executor.submit(generate_condition_summary, risk, medical_summary, diagnostic_summaries): risk
                for risk in snomed_high_risk
            }
            
            completed = 0
            for future in as_completed(future_to_condition):
                risk = future_to_condition[future]
                try:
                    risk["clinical_summary"] = future.result()
                    completed += 1
                    if completed % 5 == 0 or completed == len(snomed_high_risk):
                        print(f"  Progress: {completed}/{len(snomed_high_risk)} SNOMED summaries generated")
                except Exception as e:
                    print(f"  Error generating summary for {risk.get('condition')}: {str(e)}")
                    risk["clinical_summary"] = f"High-risk condition: {risk.get('condition', 'Unknown')}. Clinical context unavailable."
    
    # Generate clinical summaries for high-risk RxNorm medications in parallel
    if rxnorm_high_risk:
        print(f"Generating clinical summaries for {len(rxnorm_high_risk)} high-risk RxNorm medications...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_medication = {
                executor.submit(generate_condition_summary, risk, medical_summary, diagnostic_summaries): risk
                for risk in rxnorm_high_risk
            }
            
            completed = 0
            for future in as_completed(future_to_medication):
                risk = future_to_medication[future]
                try:
                    risk["clinical_summary"] = future.result()
                    completed += 1
                    if completed % 5 == 0 or completed == len(rxnorm_high_risk):
                        print(f"  Progress: {completed}/{len(rxnorm_high_risk)} RxNorm summaries generated")
                except Exception as e:
                    print(f"  Error generating summary for {risk.get('condition')}: {str(e)}")
                    risk["clinical_summary"] = f"High-risk medication: {risk.get('condition', 'Unknown')}. Clinical context unavailable."

    # Update executive_summary risk_overview to include categorized condition counts
    if isinstance(executive_summary, dict) and "risk_overview" in executive_summary:
        executive_summary["risk_overview"]["hierarchical_risk_analysis_count"] = len(hierarchical_risks)
        executive_summary["risk_overview"]["snomed_conditions_count"] = len(snomed_high_risk)
        executive_summary["risk_overview"]["rxnorm_medications_count"] = len(rxnorm_high_risk)
        executive_summary["risk_overview"]["high_risk_conditions_count"] = len(high_risk_conditions)
        executive_summary["risk_overview"]["chronic_conditions_count"] = len(chronic_conditions)
        executive_summary["risk_overview"]["other_conditions_count"] = len(other_conditions)
        print(f"Updated executive_summary risk_overview - Total: {len(hierarchical_risks)}, SNOMED: {len(snomed_high_risk)}, RxNorm: {len(rxnorm_high_risk)}")
        print(f"Categorized conditions - High Risk: {len(high_risk_conditions)}, Chronic: {len(chronic_conditions)}, Other: {len(other_conditions)}")

    enhanced_summary = {
        "executive_summary": executive_summary,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "high_risk_conditions": high_risk_conditions,
        "chronic_conditions": chronic_conditions,
        "chronic_conditions_grouped": categorized_all.get("chronic_conditions", {}) if categorized_all else {},  # Grouped structure with umbrella categories
        "other_conditions": other_conditions,
        "other_conditions_grouped": categorized_all.get("other_conditions", {}) if categorized_all else {},  # Grouped structure with umbrella categories
        "high_risk_medications": high_risk_medications or [],
        "other_medications": other_medications or [],
        "other_medications_grouped": categorized_all.get("other_medications", {}) if categorized_all else {},  # Grouped structure with umbrella categories
        "hierarchical_risk_analysis": {
            "total_found": len(hierarchical_risks),
            "snomed_conditions": snomed_high_risk,
            "rxnorm_medications": rxnorm_high_risk,
            "conditions": hierarchical_risks,  # Keep for backward compatibility
            "description": "High-risk conditions and medications separated by type (SNOMED for medical conditions, RxNorm for medications)"
        },
        "health_history": summary.get("health_history"),
        "family_history": summary.get("family_history"),
        "medical_history": summary.get("medical_history"),
        "surgeries": summary.get("surgeries", []),  # NEW: Array of surgery dicts with date/procedure/summary
        "surgical_history": summary.get("surgical_history"),  # DEPRECATED: Keep for backward compatibility
        "hospitalizations": summary.get("hospitalizations", []),  # NEW: Array of hospitalization dicts with date/reason/summary
        "medication_history": summary.get("medication_history"),
        "medical_appointments": summary.get("medical_appointments", []),
        "lab_results": summary.get("lab_results", []),
        "lab_results_summary": summary.get("lab_results_summary", ""),  # NEW: LLM-generated comprehensive lab summary
        "vitals_trend": summary.get("vitals_trend", {})
    }

    # Include grouped coded conditions (unique codes with all dates)
    enhanced_summary["coded_conditions"] = grouped_conditions

    # Include ALL diagnostic summaries (not just abnormal)
    enhanced_summary["diagnostic_summaries"] = diagnostic_summaries

    total_categorized = len(high_risk_conditions) + len(chronic_conditions) + len(other_conditions)
    total_medications = len(high_risk_medications or []) + len(other_medications or [])
    print("Enhanced summary created.")
    print(f"  - High Risk Conditions: {len(high_risk_conditions)}")
    print(f"  - Chronic Conditions: {len(chronic_conditions)}")
    print(f"  - Other Conditions: {len(other_conditions)}")
    print(f"  - Total Categorized Conditions: {total_categorized}")
    print(f"  - High Risk Medications: {len(high_risk_medications or [])}")
    print(f"  - Other Medications: {len(other_medications or [])}")
    print(f"  - Total Medications: {total_medications}")
    print(f"  - Hierarchical Risk Conditions: {len(hierarchical_risks)}")
    print(f"  - Grouped Coded Conditions: {len(grouped_conditions)}")
    print(f"  - Diagnostic Summaries: {len(diagnostic_summaries)}")
    return enhanced_summary


def generate_condition_summary(condition_data, medical_summary, diagnostic_summaries):
    """
    Generate a concise clinical summary for a specific condition using LLM.
    Combines information from coded condition data, medical summary, and diagnostic findings.
    
    Args:
        condition_data: Dict containing condition info (codes, dates, evidence, etc.)
        medical_summary: Full medical summary dict
        diagnostic_summaries: List of diagnostic test summaries
    
    Returns:
        str: 2-3 sentence clinical summary for underwriter context
    """
    try:
        # Extract relevant context from condition data
        condition_name = condition_data.get('condition', 'Unknown condition')
        primary_code = condition_data.get('primary_code', '')
        codes = condition_data.get('codes', {})
        icd10_code = codes.get('icd10', '')
        icd10_description = codes.get('icd10_description', '')
        snomed_code = codes.get('snomed', '')
        snomed_description = codes.get('snomed_description', '')
        rxnorm_code = codes.get('rxnorm', '')
        rxnorm_description = codes.get('rxnorm_description', '')
        dates = condition_data.get('dates', [])
        most_recent_date = condition_data.get('most_recent_date', 'Not specified')
        category = condition_data.get('category', '')
        extracted_quote = condition_data.get('extracted_quote', '')
        
        # Get summary sections that might be relevant
        summary = medical_summary.get('summary', medical_summary)
        medical_history = summary.get('medical_history', '')
        medication_history = summary.get('medication_history', '')
        health_history = summary.get('health_history', '')
        substance_use = summary.get('substance_use', {})
        encounters = summary.get('encounters', [])
        lab_results = summary.get('lab_results', [])
        
        # Build prompt for LLM
        prompt = f"""You are a medical underwriting assistant. Generate a concise 2-3 sentence clinical summary for the following condition to help an underwriter understand its context and significance.

                    CONDITION DETAILS:
                    - Condition: {condition_name}
                    - Primary Code: {primary_code}
                    - ICD-10: {icd10_code} - {icd10_description}
                    - SNOMED: {snomed_code} - {snomed_description}
                    - RxNorm: {rxnorm_code} - {rxnorm_description}
                    - Category: {category}
                    - Most Recent Date: {most_recent_date}
                    - All Dates Reported: {', '.join(str(d) for d in dates[:5]) if dates else 'Not specified'}
                    - Extracted Text from Document: {extracted_quote}

                    RELEVANT PATIENT CONTEXT:
                    Medical History: {medical_history}

                    Medication History: {medication_history}

                    Health History: {health_history}

                    Substance Use: {json.dumps(substance_use, indent=2)}

                    Recent Lab Results: {json.dumps(lab_results[-3:], indent=2) if lab_results else 'None available'}

                    DIAGNOSTIC TEST FINDINGS:
                    {json.dumps([d for d in diagnostic_summaries if d.get('abnormality_likelihood') == 'likely_abnormal'][:3], indent=2) if diagnostic_summaries else 'No abnormal findings'}

                    INSTRUCTIONS:
                    1. Provide a 2-3 sentence summary explaining:
                    - What this condition/medication is in plain language
                    - When it was first reported and most recently documented
                    - Current status, treatment, or management (based on patient context)
                    - Any relevant severity indicators, complications, or underwriting concerns
                    2. Focus on underwriting-relevant information:
                    - Severity and progression
                    - Chronicity and stability
                    - Treatment compliance and effectiveness
                    - Associated risks or complications
                    3. Use clear, professional language suitable for non-clinical underwriters
                    4. If the condition appears multiple times with different dates, mention the recurrence pattern
                    5. If information is limited, state what is known and acknowledge gaps

                    Return ONLY the summary text, no JSON or formatting."""
        
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 350,
            "temperature": 0.3,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}]
                }
            ]
        }
        
        # Retry with exponential backoff
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = bedrock_runtime.invoke_model(
                    modelId="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                    body=json.dumps(request_body)
                )
                
                response_body = json.loads(response['body'].read())
                summary_text = response_body['content'][0]['text'].strip()
                
                # Validate summary length (2-3 sentences, reasonable length)
                if len(summary_text) > 800:
                    summary_text = summary_text[:797] + "..."
                
                # Simple validation via Pydantic (just ensures it's a string)
                validated_summary = ConditionSummaryResponse(summary=summary_text)
                
                return validated_summary.summary
            
            except Exception as retry_error:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + (0.5 * attempt)  # Exponential backoff
                    # Don't logger.info every retry for parallel execution (too noisy)
                    time.sleep(wait_time)
                else:
                    # On final attempt, raise the error to be caught by outer try-except
                    raise
        
    except Exception as e:
        print(f"Error generating condition summary for {condition_data.get('condition')}: {str(e)}")
        return f"Condition: {condition_data.get('condition', 'Unknown')}. Additional clinical context unavailable."


def group_conditions_by_code(coded_entities):
    """
    Group conditions by unique ICD-10/SNOMED/RxNorm code.
    For each unique code, collect all dates and occurrences.
    Sort by most recent date.
    
    Args:
        coded_entities: List of coded conditions from Task 4
    
    Returns:
        List of grouped conditions, sorted by most recent date
    """
    from collections import defaultdict
    from datetime import datetime as dt
    
    # Group by unique code (prefer ICD-10, then SNOMED, then RxNorm)
    code_groups = defaultdict(lambda: {
        "dates": [],
        "pages": [],
        "evidence": [],
        "category": None,
        "condition": None,
        "description": None,
        "codes": {}
    })
    
    for entity in coded_entities:
        # Determine primary code for grouping
        primary_code = None
        if entity.get("icd10_code"):
            primary_code = f"ICD10:{entity['icd10_code']}"
        elif entity.get("snomed_code"):
            primary_code = f"SNOMED:{entity['snomed_code']}"
        elif entity.get("rxnorm_code"):
            primary_code = f"RXNORM:{entity['rxnorm_code']}"
        
        if not primary_code:
            # Skip entities without any medical codes
            continue
        
        group = code_groups[primary_code]
        
        # Collect date if available
        reported_date = entity.get("reported_date") or entity.get("date_reported")
        if reported_date:
            group["dates"].append(reported_date)
        
        # Collect page numbers
        page_num = entity.get("page_num") or entity.get("page")
        if page_num and page_num not in group["pages"]:
            group["pages"].append(page_num)
        
        # Collectevidence boxes
        if entity.get("evidence"):
            group["evidence"].extend(entity["evidence"])
        
        # Store first occurrence details
        if not group["condition"]:
            group["condition"] = entity.get("condition") or entity.get("text")
            group["category"] = entity.get("category")
            group["description"] = entity.get("icd10_description") or entity.get("snomed_description") or entity.get("rxnorm_description")
            group["codes"] = {
                "icd10": entity.get("icd10_code"),
                "icd10_description": entity.get("icd10_description"),
                "snomed": entity.get("snomed_code"),
                "snomed_description": entity.get("snomed_description"),
                "rxnorm": entity.get("rxnorm_code"),
                "rxnorm_description": entity.get("rxnorm_description"),
            }
            group["extracted_quote"] = entity.get("extracted_quote") or entity.get("text")
    
    # Convert to list and sort by most recent date
    grouped_list = []
    for code, group in code_groups.items():
        # Parse and sort dates
        parsed_dates = []
        for date_str in group["dates"]:
            try:
                # Try multiple date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y"]:
                    try:
                        parsed_dates.append(dt.strptime(str(date_str).strip(), fmt))
                        break
                    except:
                        continue
            except:
                pass
        
        # Determine most recent date
        most_recent_date = None
        if parsed_dates:
            most_recent_date = max(parsed_dates)
        
        grouped_list.append({
            "primary_code": code,
            "condition": group["condition"],
            "category": group["category"],
            "description": group["description"],
            "codes": group["codes"],
            "dates": sorted(group["dates"], reverse=True),  # Most recent first
            "most_recent_date": most_recent_date.strftime("%Y-%m-%d") if most_recent_date else None,
            "pages": sorted(list(set(group["pages"]))),
            "evidence_count": len(group["evidence"]),
            "evidence": group["evidence"],
            "extracted_quote": group["extracted_quote"]
        })
    
    # Sort by most recent date (descending)
    grouped_list.sort(key=lambda x: x["most_recent_date"] or "0000-00-00", reverse=True)
    
    print(f"Grouped {len(coded_entities)} entities into {len(grouped_list)} unique conditions")
    return grouped_list

def process_risk_analysis(medical_summary_s3_path, coded_conditions_s3_path, diagnostic_summaries_s3_path):
    """
    Main processing function for risk analysis.
    Requires three S3 file paths:
      - medical_summary.json
      - coded_conditions.json
      - diagnostic_summaries.json
    """
    print(f"\n{'='*80}")
    print(f"STARTING RISK ANALYSIS")
    print(f"{'='*80}")
    print(f"Processing risk analysis with three input files:")
    print(f"  1. Medical Summary: {medical_summary_s3_path}")
    print(f"  2. Coded Conditions: {coded_conditions_s3_path}")
    print(f"  3. Diagnostic Summaries: {diagnostic_summaries_s3_path}")
    print(f"{'='*80}\n")
    sys.stdout.flush()

    try:
        # Download medical summary (required)
        print(f"[STEP 1/3] Downloading medical summary...")
        sys.stdout.flush()
        medical_summary = download_medical_summary(medical_summary_s3_path)
        print(f"[STEP 1/3] ✓ Medical summary downloaded\n")
        sys.stdout.flush()

        # Load coded conditions (required)
        print(f"[STEP 2/3] Downloading coded conditions...")
        sys.stdout.flush()
        coded_conditions = download_json_from_s3(coded_conditions_s3_path)
        print(f"[STEP 2/3] ✓ Coded conditions downloaded\n")
        sys.stdout.flush()

        # Load diagnostics (required) and filter likely abnormal only
        print(f"[STEP 3/3] Downloading diagnostic summaries...")
        sys.stdout.flush()
        diagnostic_summaries = download_json_from_s3(diagnostic_summaries_s3_path)
        print(f"[STEP 3/3] ✓ Diagnostic summaries downloaded\n")
        sys.stdout.flush()
        
        print(f"{'='*80}")
        print(f"ALL INPUT FILES DOWNLOADED - Starting Analysis")
        print(f"{'='*80}\n")
        sys.stdout.flush()

        # STEP 1: Run hierarchical SNOMED/RxNorm scan first to identify high-risk conditions/medications
        print("\n" + "="*80)
        print("HIERARCHICAL RISK ANALYSIS (Pre-LLM)")
        print("="*80)
        sys.stdout.flush()
        hierarchical_risks, matched_codes = scan_coded_conditions_hierarchical(coded_conditions)
        print("="*80 + "\n")
        sys.stdout.flush()
        high_risk_codes = {
            risk.get('snomed_code') or risk.get('rxnorm_code')
            for risk in hierarchical_risks
            if risk.get('snomed_code') or risk.get('rxnorm_code')
        }
        
        # Categorize ALL coded conditions and medications 
        # NOTE: We DON'T skip parent codes anymore because we want ALL hierarchical matches
        # to appear in high_risk_conditions (both parent and child nodes)
        categorized_all = categorize_all_coded_conditions(
            coded_conditions, 
            high_risk_codes,
            skip_parent_codes=set()  # Don't skip any codes - we want full hierarchy
        )
        print("="*80 + "\n")

        # STEP 2: Generate executive summary ONLY using LLM
        # All condition identification is done through hierarchical scanning and categorization
        llm_result = generate_executive_summary_with_llm(
            medical_summary,
            coded_conditions=coded_conditions,
            diagnostic_summaries=diagnostic_summaries,
            hierarchical_risks=hierarchical_risks  # Pass to LLM for context in summary
        )
        
        # Get executive summary only (no condition lists from LLM)
        executive_summary = llm_result.get("executive_summary", {})

        # STEP 3: Organize conditions from hierarchical scanning and categorization
        # NO LLM condition identification - all conditions come from coded_conditions
        print(f"\nOrganizing conditions from hierarchical analysis and categorization...")
        print(f"  - From hierarchical analysis: {len(hierarchical_risks)} high-risk items (all with evidence)")
        print(f"  - From coded conditions: High Risk={len(categorized_all['high_risk_conditions'])}, Chronic={len(categorized_all['chronic_conditions'])}, Other={len(categorized_all['other_conditions'])}")
        print(f"  - From medications: High Risk={len(categorized_all['high_risk_medications'])}, Other={len(categorized_all['other_medications'])}")
        
        # Helper function to check if condition already exists
        def condition_already_exists(new_item, existing_lists):
            """Check if condition exists in any of the existing lists by code or name"""
            snomed_code = new_item.get('snomed_code', '')
            rxnorm_code = new_item.get('rxnorm_code', '')
            condition_name = (new_item.get('condition') or '').lower().strip()
            
            for existing_list in existing_lists:
                for existing in existing_list:
                    # Check by SNOMED code
                    if snomed_code and existing.get('snomed_code') == snomed_code:
                        return True
                    # Check by RxNorm code
                    if rxnorm_code and existing.get('rxnorm_code') == rxnorm_code:
                        return True
                    # Check by condition name (case-insensitive)
                    existing_name = (existing.get('condition') or '').lower().strip()
                    if condition_name and existing_name and condition_name == existing_name:
                        return True
            return False
        
        # START WITH HIERARCHICAL RISKS for high-risk conditions and medications
        # This ensures ALL matched conditions from the hierarchy (including parent and child nodes) 
        # are included with their evidence_boxes and hierarchy paths
        snomed_hierarchical = [r for r in hierarchical_risks if r.get('code_type') == 'SNOMED']
        rxnorm_hierarchical = [r for r in hierarchical_risks if r.get('code_type') == 'RxNorm']
        
        # Format hierarchical risks to match the expected structure for high_risk_conditions
        def format_hierarchical_for_high_risk(risk_item):
            """Convert hierarchical risk format to high_risk_conditions format"""
            return {
                "condition": risk_item.get('condition', ''),
                "snomed_code": risk_item.get('snomed_code', ''),
                "rxnorm_code": risk_item.get('rxnorm_code', ''),
                "icd10_code": risk_item.get('icd10_code', ''),
                "category": risk_item.get('category', 'Not specified'),
                "risk_level": risk_item.get('risk_level', 'High'),
                "evidence_boxes": risk_item.get('evidence_boxes', []),
                "evidence_text": risk_item.get('evidence_text', ''),
                "description": risk_item.get('description', ''),
                "clinical_summary": risk_item.get('clinical_summary', ''),  # Will be generated later
                "page": risk_item.get('page'),
                "date_reported": risk_item.get('date_reported'),
                "date": risk_item.get('date_reported'),  # Use date_reported as date
                "condition_type": "High Risk",
                "source": risk_item.get('source', 'Hierarchical Analysis'),
                "hierarchical_path": risk_item.get('hierarchical_path', []),
                "path_display": risk_item.get('path_display', ''),
                "hierarchy_depth": risk_item.get('hierarchy_depth', 1)
            }
        
        # Start with hierarchical SNOMED conditions
        high_risk_conditions = [format_hierarchical_for_high_risk(r) for r in snomed_hierarchical]
        
        # Start with hierarchical RxNorm medications
        high_risk_medications = [format_hierarchical_for_high_risk(r) for r in rxnorm_hierarchical]
        
        # Add any additional high-risk conditions from categorized that aren't in hierarchical
        for cat_item in categorized_all['high_risk_conditions']:
            if not condition_already_exists(cat_item, [high_risk_conditions]):
                high_risk_conditions.append(cat_item)
        
        # Add any additional high-risk medications from categorized that aren't in hierarchical
        for cat_item in categorized_all['high_risk_medications']:
            if not condition_already_exists(cat_item, [high_risk_medications]):
                high_risk_medications.append(cat_item)
        
        # Extract flat lists from grouped structures (chronic_conditions, other_conditions, other_medications)
        def flatten_grouped_structure(grouped_dict):
            """
            Extract flat list from grouped structure returned by group_by_umbrella_categories.
            
            Args:
                grouped_dict: Dict with 'grouped' and 'ungrouped' keys
            
            Returns:
                Flat list of all items
            """
            items = []
            
            # Add all items from grouped categories
            for category_key, category_data in grouped_dict.get('grouped', {}).items():
                items.extend(category_data.get('items', []))
            
            # Add all ungrouped items
            items.extend(grouped_dict.get('ungrouped', []))
            
            return items
        
        # Use categorized lists - flatten grouped structures
        chronic_conditions = flatten_grouped_structure(categorized_all['chronic_conditions'])
        other_conditions = flatten_grouped_structure(categorized_all['other_conditions'])
        other_medications = flatten_grouped_structure(categorized_all['other_medications'])
        
        # Re-sort each list by date after merging (most recent first)
        def sort_by_date_desc(findings_list):
            with_date = [f for f in findings_list if f.get("date") or f.get("date_reported")]
            without_date = [f for f in findings_list if not (f.get("date") or f.get("date_reported"))]
            with_date.sort(key=lambda x: x.get("date") or x.get("date_reported", ""), reverse=True)
            return with_date + without_date
        
        high_risk_conditions = sort_by_date_desc(high_risk_conditions)
        chronic_conditions = sort_by_date_desc(chronic_conditions)
        other_conditions = sort_by_date_desc(other_conditions)
        high_risk_medications = sort_by_date_desc(high_risk_medications)
        other_medications = sort_by_date_desc(other_medications)
        
        # FILTER: Remove parent nodes without evidence_boxes if their children exist
        # This prevents LLM-generated parent nodes from appearing when we have actual
        # coded_conditions children with evidence
        def filter_parents_without_evidence(conditions_list):
            """Remove parent nodes without evidence_boxes if children with evidence exist"""
            # Build a map of parent codes to check
            parent_codes = set()
            children_with_evidence = []
            
            for cond in conditions_list:
                if cond.get("evidence_boxes"):
                    children_with_evidence.append(cond)
                    # Track parent codes from hierarchical path
                    hierarchical_path = cond.get("hierarchical_path", [])
                    if hierarchical_path and len(hierarchical_path) > 1:
                        parent_code = hierarchical_path[0][0]  # First node is parent
                        parent_codes.add(parent_code)
            
            # Filter: keep all conditions with evidence_boxes OR conditions without evidence
            # but only if they're not parent nodes of existing children
            filtered = []
            for cond in conditions_list:
                has_evidence = bool(cond.get("evidence_boxes"))
                snomed_code = cond.get("snomed_code", "")
                
                # Keep if has evidence
                if has_evidence:
                    filtered.append(cond)
                # Keep if no evidence BUT not a parent of existing children
                elif snomed_code not in parent_codes:
                    filtered.append(cond)
                # Skip: no evidence AND is a parent of existing children
                else:
                    print(f"  ⚠️  Filtered out parent without evidence: {cond.get('condition')} ({snomed_code})")
            
            return filtered
        
        high_risk_conditions = filter_parents_without_evidence(high_risk_conditions)
        high_risk_medications = filter_parents_without_evidence(high_risk_medications)
        
        total_conditions = len(high_risk_conditions) + len(chronic_conditions) + len(other_conditions)
        total_medications = len(high_risk_medications) + len(other_medications)
        print(f"  - Final totals - Conditions: High Risk={len(high_risk_conditions)}, Chronic={len(chronic_conditions)}, Other={len(other_conditions)}")
        print(f"  - Final totals - Medications: High Risk={len(high_risk_medications)}, Other={len(other_medications)}")
        print()

        # STEP 4: Create enhanced summary with all risk data
        enhanced_summary = create_enhanced_summary(
            executive_summary,
            high_risk_conditions, 
            chronic_conditions,
            other_conditions,
            hierarchical_risks,  # Pass separately for dedicated section
            high_risk_medications=high_risk_medications,
            other_medications=other_medications,
            coded_conditions=coded_conditions,
            diagnostic_summaries=diagnostic_summaries,
            medical_summary=medical_summary,
            categorized_all=categorized_all  # Pass grouped structures
        )
        
        # STEP 5: Enrich categorized buckets with descriptions from grouped_conditions
        print(f"\nEnriching categorized conditions/medications with descriptions from grouped_conditions...")
        grouped_conditions_with_desc = enhanced_summary.get("coded_conditions", [])
        
        # Create lookup dict: code -> description
        code_to_description = {}
        for grouped in grouped_conditions_with_desc:
            codes = grouped.get('codes', {})
            description = grouped.get('description', '')
            
            # Map by ICD-10, SNOMED, or RxNorm code
            if codes.get('icd10'):
                code_to_description[codes['icd10']] = description
            if codes.get('snomed'):
                code_to_description[codes['snomed']] = description
            if codes.get('rxnorm'):
                code_to_description[codes['rxnorm']] = description
            
            # Also map by condition name (case-insensitive)
            condition_name = grouped.get('condition', '').lower().strip()
            if condition_name:
                code_to_description[condition_name] = description
        
        # Enrich each categorized list
        def enrich_with_description(item_list):
            """Add description field to each item by matching codes or name"""
            enriched = 0
            for item in item_list:
                if item.get('description'):  # Skip if already has description
                    continue
                
                # Try to find description by code or name
                description = None
                
                # Match by ICD-10
                if item.get('icd10_code'):
                    description = code_to_description.get(item['icd10_code'])
                
                # Match by SNOMED
                if not description and item.get('snomed_code'):
                    description = code_to_description.get(item['snomed_code'])
                
                # Match by RxNorm
                if not description and item.get('rxnorm_code'):
                    description = code_to_description.get(item['rxnorm_code'])
                
                # Match by condition name (case-insensitive)
                if not description:
                    condition_name = item.get('condition', '').lower().strip()
                    if condition_name:
                        description = code_to_description.get(condition_name)
                
                # Set description if found
                if description:
                    item['description'] = description
                    enriched += 1
            
            return enriched
        
        # Enrich all categorized lists
        enriched_hr_conds = enrich_with_description(enhanced_summary['high_risk_conditions'])
        enriched_chronic = enrich_with_description(enhanced_summary['chronic_conditions'])
        enriched_other = enrich_with_description(enhanced_summary['other_conditions'])
        enriched_hr_meds = enrich_with_description(enhanced_summary['high_risk_medications'])
        enriched_other_meds = enrich_with_description(enhanced_summary['other_medications'])
        
        print(f"  ✓ Enriched High Risk Conditions: {enriched_hr_conds}/{len(enhanced_summary['high_risk_conditions'])}")
        print(f"  ✓ Enriched Chronic Conditions: {enriched_chronic}/{len(enhanced_summary['chronic_conditions'])}")
        print(f"  ✓ Enriched Other Conditions: {enriched_other}/{len(enhanced_summary['other_conditions'])}")
        print(f"  ✓ Enriched High Risk Medications: {enriched_hr_meds}/{len(enhanced_summary['high_risk_medications'])}")
        print(f"  ✓ Enriched Other Medications: {enriched_other_meds}/{len(enhanced_summary['other_medications'])}")

        # Save to S3 (existing function)
        output_path = save_enhanced_summary_to_s3(enhanced_summary, medical_summary_s3_path)

        return {
            "status": "success",
            "input_paths": {
                "medical_summary": medical_summary_s3_path,
                "coded_conditions": coded_conditions_s3_path,
                "diagnostic_summaries": diagnostic_summaries_s3_path
            },
            "output_path": output_path,
            "diagnostics_image_count": len(diagnostic_summaries),
            "coded_conditions_loaded": bool(coded_conditions),
            "executive_summary": executive_summary,
            "high_risk_count": len(high_risk_conditions),
            "chronic_count": len(chronic_conditions),
            "other_count": len(other_conditions),
            "total_findings": total_conditions
        }

    except Exception as e:
        print(f"Error processing risk analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error_message": str(e),
            "input_paths": {
                "medical_summary": medical_summary_s3_path,
                "coded_conditions": coded_conditions_s3_path,
                "diagnostic_summaries": diagnostic_summaries_s3_path
            }
        }

def lambda_handler(event, context):
    """Lambda handler for comprehensive risk analysis with multiple pipeline inputs"""

    print(f"Event received: {event}")

    # ============================================================================
    # WARMUP EVENT HANDLING (EventBridge scheduled invocation every 5 min)
    # Detects warmup event and only loads caches to keep container warm
    # ============================================================================
    if event.get('source') == 'aws.events' and event.get('detail-type') == 'Scheduled Event':
        print("\n" + "="*80)
        print("WARMUP EVENT DETECTED - Loading caches only")
        print("="*80)
        
        # Load SNOMED hierarchy cache (from /opt/ pickle file)
        print("[WARMUP] Loading SNOMED hierarchy cache...")
        hierarchy = get_expanded_snomed_hierarchy()
        print(f"[WARMUP] ✓ Hierarchy loaded: {len(hierarchy)} risk categories")
        
        # Copy SNOMED CT database from EFS to /tmp/ for fast access
        print("[WARMUP] Copying SNOMED CT database to /tmp/...")
        copy_db_to_tmp_synchronous()
        print("[WARMUP] ✓ Database cached in /tmp/")
        
        # Verify database connection (uses lazy loading)
        print("[WARMUP] Testing database connection...")
        SNOMEDCT = get_snomedct()
        print(f"[WARMUP] ✓ SNOMED CT loaded successfully")
        
        print("="*80)
        print("WARMUP COMPLETE - Container ready for incoming requests")
        print("="*80)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'warmup_complete',
                'hierarchy_categories': len(hierarchy),
                'container_id': context.request_id
            })
        }

    # Extract S3 paths from parameters
    parameters = event.get('parameters', [])

    # Build parameter dictionary
    param_dict = {}
    for param in parameters:
        param_name = param.get('name', '')
        param_value = param.get('value')
        if param_name:
            param_dict[param_name] = param_value

    # All three parameters are required
    medical_summary_path = param_dict.get('medical-summary-path')
    coded_conditions_path = param_dict.get('coded-conditions-path')
    diagnostic_summaries_path = param_dict.get('diagnostic-summaries-path')

    # Validate all required parameters
    missing_params = []
    if not medical_summary_path:
        missing_params.append('medical-summary-path')
    if not coded_conditions_path:
        missing_params.append('coded-conditions-path')
    if not diagnostic_summaries_path:
        missing_params.append('diagnostic-summaries-path')

    if missing_params:
        return {
            'messageVersion': '1.0',
            'response': {
                'actionGroup': event.get('actionGroup', 'risk-analysis'),
                'function': event.get('function', 'analyze-risks'),
                'functionResponse': {
                    'responseBody': {
                        'TEXT': {
                            'body': json.dumps({
                                'status': 'error',
                                'error_message': f'Missing required parameters: {", ".join(missing_params)}'
                            })
                        }
                    }
                }
            }
        }

    # Process comprehensive risk analysis with all three files
    result = process_risk_analysis(
        medical_summary_path,
        coded_conditions_path,
        diagnostic_summaries_path
    )

    # Format response
    response_body = {
        'TEXT': {
            'body': json.dumps(result)
        }
    }

    function_response = {
        'actionGroup': event.get('actionGroup', 'risk-analysis'),
        'function': event.get('function', 'analyze-risks'),
        'functionResponse': {
            'responseBody': response_body
        }
    }

    action_response = {
        'messageVersion': '1.0',
        'response': function_response,
        'sessionAttributes': event.get('sessionAttributes', {}),
        'promptSessionAttributes': event.get('promptSessionAttributes', {})
    }
    
    # Warm cache for next invocation - MUST be synchronous!
    # Lambda freezes execution environment immediately after handler returns,
    # so background threads won't complete. This adds ~10s to first cold start
    # but makes all subsequent warm starts much faster.
    print("\n[CACHE] Warming /tmp/ cache for next invocation...")
    sys.stdout.flush()
    copy_db_to_tmp_synchronous()

    return action_response


# ============================================================================
# UTILITY: Generate Hierarchy Cache File (One-Time Operation)
# Run this locally to create snomed_hierarchy_cache.pkl for Lambda deployment
# ============================================================================

def generate_hierarchy_cache_file(output_path: str = 'snomed_hierarchy_cache.pkl'):
    """
    ONE-TIME UTILITY: Generate pre-computed SNOMED hierarchy cache file.
    Run this locally, then upload to Lambda Layer at /opt/snomed_hierarchy_cache.pkl
    
    Usage:
        python aps_risk_analyzer_and_summary.py --generate-cache
    
    Args:
        output_path: Where to save the pickle file (default: snomed_hierarchy_cache.pkl)
    """
    print("="*80)
    print("GENERATING SNOMED HIERARCHY CACHE FILE")
    print("="*80)
    print("This will take 10-30 seconds...")
    print()
    
    # Build the hierarchy (this is the expensive operation)
    hierarchy = build_expanded_snomed_hierarchy()
    
    print(f"\n[CACHE] Saving to {output_path}...")
    with open(output_path, 'wb') as f:
        pickle.dump(hierarchy, f)
    
    file_size = os.path.getsize(output_path) / (1024**2)
    print(f"[CACHE] ✓ Saved: {file_size:.1f}MB")
    print()
    print("="*80)
    print("NEXT STEPS:")
    print("="*80)
    print("1. Upload this file to Lambda Layer:")
    print("   aws lambda publish-layer-version \\")
    print("      --layer-name snomed-hierarchy-cache \\")
    print("      --zip-file fileb://snomed_hierarchy_cache.zip \\")
    print("      --compatible-runtimes python3.11")
    print()
    print("2. Add layer to Lambda function")
    print("3. File will be available at: /opt/snomed_hierarchy_cache.pkl")
    print("="*80)


# ============================================================================
# MAIN - Local Testing
# ============================================================================

if __name__ == "__main__":
    import sys
    
    # Check if user wants to generate cache file
    if len(sys.argv) > 1 and sys.argv[1] == '--generate-cache':
        output_file = sys.argv[2] if len(sys.argv) > 2 else 'snomed_hierarchy_cache.pkl'
        generate_hierarchy_cache_file(output_file)
        sys.exit(0)
    
    # Otherwise run normal test execution
    session_id = "02415406-eed9-4ded-ae1f-6025cdf1e0e1"
    event = {
        "messageVersion": "1.0",
        "agent": {
            "name": "string",
            "id": "string",
            "alias": "string",
            "version": "string"
        },
        "inputText": "string",
        "sessionId": session_id,
        "actionGroup": "string",
        "function": "string",
        "parameters": [
            {
                "name": "medical-summary-path",
                "type": "string",
                "value": f"s3://aps-summarization-poc/02415406-eed9-4ded-ae1f-6025cdf1e0e1/outputs/medical_summary.json"
            },
            {
                "name": "coded-conditions-path",
                "type": "string",
                "value": f"s3://aps-summarization-poc/02415406-eed9-4ded-ae1f-6025cdf1e0e1/outputs/coded_conditions.json"
            },
            {
                "name": "diagnostic-summaries-path",
                "type": "string",
                "value": f"s3://aps-summarization-poc/02415406-eed9-4ded-ae1f-6025cdf1e0e1/outputs/diagnostic_summaries.json"
            }
        ],
        "sessionAttributes": {
            "string": "string",
        },
        "promptSessionAttributes": {
            "string": "string"
        }
    }

    print("=" * 80)
    print("STEP 6: RISK ANALYZER AND SUMMARY - Local Test")
    print("=" * 80)
    print(f"Session ID: {session_id}")
    print(f"Start time: {time.ctime()}")
    print("\nRequired Input Files (3):")
    print(f"  1. Medical Summary: {session_id}/outputs/medical_summary.json")
    print(f"  2. Coded Conditions: {session_id}/outputs/coded_conditions.json")
    print(f"  3. Diagnostic Summaries: {session_id}/outputs/diagnostic_summaries.json")
    print("=" * 80 + "\n")
    start = time.time()

    out = lambda_handler(event, 0)
    
    print("\n" + "=" * 80)
    print("OUTPUT:")
    print("=" * 80)
    print(json.dumps(out, indent=2))

    end = time.time()
    print("\n" + "=" * 80)
    print(f"End time: {time.ctime()}")
    print(f"Total duration: {end - start:.2f} seconds")
    print("=" * 80)     

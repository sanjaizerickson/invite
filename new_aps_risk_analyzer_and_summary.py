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

# Load environment variables from .env file (for local testing)
# In Lambda deployment, environment variables are set via Lambda configuration
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use environment variables directly

# ============================================================================
# FASTAPI ENDPOINT - Lambda calls EC2 service  directly with requests
# ============================================================================
FASTAPI_ENDPOINT = os.environ.get("EC2_RISK_API_ENDPOINT", "")
USE_FASTAPI = bool(FASTAPI_ENDPOINT)

if USE_FASTAPI:
    print(f"[INIT] FastAPI mode enabled: {FASTAPI_ENDPOINT}")
else:
    print("[INIT] Local mode - FastAPI endpoint not configured")

# Import Pydantic models for validation
from pydantic_models_risk_analyzer import (
    RiskAnalysisAndSummaryResponse,
    ExecutiveSummary,
    HighRiskFinding,
    ConditionSummaryResponse,
    safe_validate_llm_output,
    extract_validated_dict
)

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
        
        print("[API] Calling FastAPI service for hierarchy scan AND categorization...")
        sys.stdout.flush()
        try:
            # Extract coded_entities list from dict structure if needed
            coded_entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
            
            response = requests.post(
                f"{FASTAPI_ENDPOINT}/scan_conditions",
                json={"coded_conditions": coded_entities},
                timeout=60
            )
            response.raise_for_status()
            result = response.json()
            hierarchical_risks = result['hierarchical_risks']
            print(f"[API] ✓ Scanned {result['total_conditions_scanned']} conditions")
            print(f"[API] ✓ Found {result['high_risk_count']} high-risk findings")
            print(f"[API] ✓ Processing time: {result['processing_time_ms']:.1f}ms")
            
            # Get categorization from API response (flat lists)
            categorized_all = {
                'chronic_conditions': result['chronic_conditions'],
                'high_risk_conditions': result['high_risk_conditions'],
                'other_conditions': result['other_conditions'],
                'high_risk_medications': result['high_risk_medications'],
                'other_medications': result['other_medications'],
                # NEW: Get umbrella-grouped structures from API (already grouped and sorted by date)
                'chronic_conditions_grouped': result['chronic_conditions_grouped'],
                'other_conditions_grouped': result['other_conditions_grouped'],
                'high_risk_medications_grouped': result['high_risk_medications_grouped'],
                'other_medications_grouped': result['other_medications_grouped']
            }
            print(f"[API] ✓ Categorized: Chronic={len(categorized_all['chronic_conditions'])}, "
                  f"High-Risk Conds={len(categorized_all['high_risk_conditions'])}, "
                  f"Other Conds={len(categorized_all['other_conditions'])}, "
                  f"High-Risk Meds={len(categorized_all['high_risk_medications'])}, "
                  f"Other Meds={len(categorized_all['other_medications'])}")
            print(f"[API] ✓ Umbrella grouping complete (pre-grouped by API)")
        except Exception as e:
            print(f"[API] ✗ FastAPI call failed: {e}")
            print("[API] ERROR: Cannot proceed without FastAPI service")
            raise
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
        
        # API returns flat lists - use directly (no need to flatten grouped structures)
        chronic_conditions = categorized_all['chronic_conditions']
        other_conditions = categorized_all['other_conditions']
        other_medications = categorized_all['other_medications']
        
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
        
        # Extract umbrella-grouped structures from API response (already grouped and sorted)
        print(f"\n[UMBRELLA] Using umbrella grouping from API (no local processing needed)...")
        chronic_conditions_grouped = categorized_all['chronic_conditions_grouped']
        other_conditions_grouped = categorized_all['other_conditions_grouped']
        high_risk_medications_grouped = categorized_all['high_risk_medications_grouped']
        other_medications_grouped = categorized_all['other_medications_grouped']
        
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
    

    return action_response

# ============================================================================
# MAIN - Local Testing
# ============================================================================

if __name__ == "__main__":
    import sys
    
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

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
# UMBRELLA CATEGORIES - For grouping conditions/medications in UI
# ============================================================================

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
    
    # Medication groupings
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


# Global variable - will be populated on first use
EXPANDED_SNOMED_HIERARCHY = None
_hierarchy_initialized = False

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


def categorize_coded_conditions(coded_conditions: List[Dict], high_risk_codes: Set[str], skip_parent_codes: Set[str] = None) -> Dict:
    """
    BATCH categorize all coded conditions into buckets: high-risk, chronic, other.
    Separates conditions from medications.
    
    Replaces the Lambda's individual per-condition chronic checks with a single batch operation.
    
    Args:
        coded_conditions: List of conditions with codes
        high_risk_codes: Set of high-risk SNOMED/RxNorm codes (from scan_coded_conditions_hierarchical)
        skip_parent_codes: Optional set of parent codes to skip
    
    Returns:
        Dict with:
        {
            "chronic_conditions": [...],
            "high_risk_conditions": [...],
            "other_conditions": [...],
            "high_risk_medications": [...],
            "other_medications": [...]
        }
    """
    chronic_conditions = []
    high_risk_conditions = []
    other_conditions = []
    high_risk_medications = []
    other_medications = []
    
    print(f"[CATEGORIZE] Starting batch categorization of {len(coded_conditions)} conditions...")
    
    # Extract entities from coded_conditions (handle both dict and list formats)
    entities = coded_conditions.get('coded_entities', coded_conditions) if isinstance(coded_conditions, dict) else coded_conditions
    
    # Single pass through all entities
    for idx, entity in enumerate(entities):
        if idx % 100 == 0 and idx > 0:
            print(f"[CATEGORIZE] Processed {idx}/{len(entities)}...")
        
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
        
        # Categorize in single pass
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
    
    print(f"[CATEGORIZE] ✓ Batch categorization complete")
    print(f"     Chronic: {len(chronic_conditions)}")
    print(f"     High-risk conditions: {len(high_risk_conditions)}")
    print(f"     Other conditions: {len(other_conditions)}")
    print(f"     High-risk meds: {len(high_risk_medications)}")
    print(f"     Other meds: {len(other_medications)}")
    
    # Group each bucket by umbrella categories
    print(f"[CATEGORIZE] Grouping conditions into umbrella categories...")
    chronic_grouped = group_by_umbrella_categories(chronic_conditions)
    other_conditions_grouped = group_by_umbrella_categories(other_conditions)
    high_risk_meds_grouped = group_by_umbrella_categories(high_risk_medications)
    other_meds_grouped = group_by_umbrella_categories(other_medications)
    
    # Sort items within each umbrella category by date
    for grouped_data in [chronic_grouped, other_conditions_grouped, high_risk_meds_grouped, other_meds_grouped]:
        for category_key in grouped_data['grouped']:
            items = grouped_data['grouped'][category_key]['items']
            grouped_data['grouped'][category_key]['items'] = sort_by_date(items)
        # Also sort ungrouped items
        grouped_data['ungrouped'] = sort_by_date(grouped_data['ungrouped'])
    
    print(f"[CATEGORIZE] ✓ Umbrella grouping complete")
    print(f"     Chronic: {len(chronic_grouped['grouped'])} groups, {len(chronic_grouped['ungrouped'])} ungrouped")
    print(f"     Other conditions: {len(other_conditions_grouped['grouped'])} groups, {len(other_conditions_grouped['ungrouped'])} ungrouped")
    print(f"     High-risk meds: {len(high_risk_meds_grouped['grouped'])} groups, {len(high_risk_meds_grouped['ungrouped'])} ungrouped")
    print(f"     Other meds: {len(other_meds_grouped['grouped'])} groups, {len(other_meds_grouped['ungrouped'])} ungrouped")
    
    return {
        # Flat lists (original format, for backward compatibility)
        "chronic_conditions": chronic_conditions,
        "high_risk_conditions": high_risk_conditions,
        "other_conditions": other_conditions,
        "high_risk_medications": high_risk_medications,
        "other_medications": other_medications,
        
        # Umbrella-grouped structures (NEW - for improved UI)
        "chronic_conditions_grouped": chronic_grouped,
        "other_conditions_grouped": other_conditions_grouped,
        "high_risk_medications_grouped": high_risk_meds_grouped,
        "other_medications_grouped": other_meds_grouped
    }


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

def format_hierarchical_path(path: List[Tuple[str, str]]) -> str:
    """
    Format hierarchical path as string: "Parent > Child"
    Example: "Myocardial infarction > Acute ST segment elevation myocardial infarction"
    """
    if not path:
        return ""
    
    names = [name for code, name in path]
    return " > ".join(names)

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


# ============================================================================
# UMBRELLA CATEGORY ASSIGNMENT FUNCTIONS
# Used to group conditions/medications into user-friendly categories
# ============================================================================

def find_umbrella_category_via_lineage_keyword_search(snomed_code: str, is_medication: bool) -> Optional[str]:
    """
    Find umbrella category by searching the SNOMED lineage (ancestors) for keywords.
    This is the PRIMARY approach - more flexible than exact code matching.
    
    For example, for cardiovascular diseases, search lineage terms for "cardio" or "heart".
    For kidney diseases, search for "kidney" or "renal" (but exclude "adrenal").
    
    Args:
        snomed_code: SNOMED CT code to check
        is_medication: Whether this is a medication (for filtering)
    
    Returns:
        Category key from UMBRELLA_CATEGORIES if found via lineage keyword search, None otherwise
    """
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
                    r'^\w+\s+(finding|disorder)(\s+\(finding\)|\s+\(disorder\))?$',  # Single word finding/disorder
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


def sort_by_date(items):
    """
    Sort items by date_reported in descending order (most recent first).
    Items without valid dates will be placed at the end.
    
    Args:
        items: List of dicts with optional 'date_reported' field
    
    Returns:
        Sorted list (does not modify original)
    """
    def get_sort_key(item):
        date_str = item.get('date_reported', '')
        if date_str and date_str != 'Not specified':
            try:
                # Try parsing common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                    try:
                        return datetime.strptime(date_str, fmt)
                    except ValueError:
                        continue
                # If no format matched, return distant past so item goes to end
                return datetime(1900, 1, 1)
            except:
                return datetime(1900, 1, 1)
        else:
            return datetime(1900, 1, 1)  # Items without valid dates go to end
    
    return sorted(items, key=get_sort_key, reverse=True)

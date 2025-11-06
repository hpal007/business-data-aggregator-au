from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import requests
import json
import re
import os
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


BASE_DIR = "/opt/shared-data/cc/"
OUTPUT_DIR = Path(os.path.join(BASE_DIR, "cc_processed_data"))
INPUT_DIR = Path(os.path.join(BASE_DIR, "cc_split_data"))
USER_AGENT = "cc-get-started/1.0 (Example data retrieval script; yourname@example.com)"

# === Configuration ===
# Option 1: Use Threading for parallel requests (Recommended for Docker)
USE_THREADING = True  # Set to True for parallel processing without Spark overhead
MAX_WORKERS = 10      # Number of parallel threads for fetching pages

# Option 2: Use Spark distributed processing (for larger scale)
USE_SPARK_DISTRIBUTED = False  # Set to True only if processing 1000+ records

# Batch size per file
BATCH_SIZE = 100  # Process 100 records at a time, set to None for all records


# === Improved Regex Definitions ===
PHONE_RE = re.compile(
    r"""
    (?:^|[\s(])
    (?:
        \+61\s?[2-478]\s?\d{4}\s?\d{4}
        |
        \(0[2-478]\)\s?\d{4}\s?\d{4}
        |
        0[2-478]\s?\d{4}\s?\d{4}
        |
        1[38]00\s?\d{3}\s?\d{3}
        |
        04\d{2}\s?\d{3}\s?\d{3}
    )
    (?=[\s),.;]|$)
    """,
    re.VERBOSE | re.IGNORECASE
)

PTY_LTD_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&'\-]{1,40}(?:\s+[A-Z][A-Za-z0-9&'\-]{1,40}){0,3})\s+(Pty\.?\s*Ltd\.?|PTY\s*LTD|Limited|LLC|Inc\.?|Corporation)\b",
    re.IGNORECASE
)

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9][A-Za-z0-9._%+\-]{0,63}@[A-Za-z0-9][A-Za-z0-9.\-]{0,253}\.[A-Za-z]{2,}\b",
    re.IGNORECASE
)

ABN_RE = re.compile(r"\bABN[:\s]*([0-9\s]{11,14})\b", re.IGNORECASE)
ACN_RE = re.compile(r"\bACN[:\s]*([0-9\s]{9,11})\b", re.IGNORECASE)

EXCLUDE_PHONE_PATTERNS = [
    r'^\d{1,2}\.\d{4}',
    r'^\d{4}[-/.]\d{2,4}',
    r'^\d{1,4}[-/]\d{1,4}$',
    r'^\d+\.\d+$',
    r'^\d{2,4}\s\d{3,4}$',
    r'^\d\s\d\s\d',
    r'^\d{2,3}\s\d{3}\s\d{3}\s\d{3}$',
]

STATE_TOKENS = ["vic", "nsw", "qld", "wa", "sa", "tas", "act", "nt"]
STATE_REGEX = re.compile(
    r"\b(" + r"|".join(re.escape(t) for t in STATE_TOKENS) + r")\b", re.IGNORECASE
)

ADDRESS_MARKERS = re.compile(
    r'\b(street|st|road|rd|avenue|ave|drive|dr|lane|ln|place|pl|court|ct|level|building|suite|unit|reply\s+paid)\b',
    re.IGNORECASE
)

POSTCODE_RE = re.compile(r'\b\d{4}\b')
IGNORED_PARENT_TAGS = {"script", "style", "noscript", "meta", "link"}


def fetch_page_from_cc(record, myagent=USER_AGENT):
    """Fetch page from Common Crawl with timeout and error handling"""
    try:
        offset = int(record["warc_record_offset"])
        length = int(record["warc_record_length"])
        filename = record["warc_filename"]
        s3_url = f"https://data.commoncrawl.org/{filename}"

        byte_range = f"bytes={offset}-{offset + length - 1}"
        response = requests.get(
            s3_url,
            headers={"user-agent": myagent, "Range": byte_range},
            stream=True,
            timeout=15,
        )

        if response.status_code == 206:
            stream = ArchiveIterator(response.raw)
            for warc_record in stream:
                if warc_record.rec_type == "response":
                    html_bytes = warc_record.content_stream().read()
                    html_text = html_bytes.decode("utf-8", errors="ignore")
                    return BeautifulSoup(html_text, "html.parser")
        return None
    except Exception as e:
        return None


def normalize_phone(phone_str):
    """Normalize and validate Australian phone numbers"""
    original = phone_str.strip()
    
    for pattern in EXCLUDE_PHONE_PATTERNS:
        if re.match(pattern, original):
            return None
    
    if '.' in original or original.count('-') > 2:
        return None
    
    if re.match(r'^\d{4}[-/]\d{4}$', original):
        return None
    
    if re.match(r'^\d{4}[-/]\d{2}[-/]\d{2}', original):
        return None
    
    cleaned = re.sub(r'[^\d+]', '', original)
    
    if len(cleaned) == 11 and re.match(r'^\d{2}\s\d{3}\s\d{3}\s\d{3}$', original):
        return None
    
    if cleaned.startswith('+61'):
        if len(cleaned) != 12:
            return None
    elif cleaned.startswith('0'):
        if len(cleaned) != 10:
            return None
    elif cleaned.startswith('1'):
        if len(cleaned) != 10:
            return None
    else:
        return None
    
    digits_only = cleaned.replace('+', '').replace('61', '', 1)
    if len(set(digits_only)) == 1:
        return None
    
    return original


def normalize_abn_acn(number_str):
    """Remove spaces from ABN/ACN"""
    return re.sub(r'\s+', '', number_str)


def is_likely_company_name(text, parent_tag):
    """Filter out obvious false positives for company names"""
    if len(text) < 5 or len(text) > 80:
        return False
    
    if any(char in text for char in ['.', '?', '!']):
        word_count = len(text.split())
        if word_count > 8:
            return False
    
    false_positive_keywords = [
        'click here', 'read more', 'learn more', 'find out',
        'contact us', 'about us', 'terms and conditions',
        'privacy policy', 'copyright', 'all rights', 'sign up',
        'log in', 'register', 'subscribe', 'download', 'cart is',
        'email address', 'service provided by', 'advertising service',
        'management and message'
    ]
    
    text_lower = text.lower()
    if any(kw in text_lower for kw in false_positive_keywords):
        return False
    
    if parent_tag and parent_tag.name in {'a', 'button', 'nav', 'footer', 'header'}:
        return False
    
    return True


def is_likely_address(text):
    """Check if text looks like a real address"""
    text_lower = text.lower()
    
    if not STATE_REGEX.search(text_lower):
        return False
    
    generic_phrases = [
        'life in', 'living in', 'progression to', 'visit',
        'located in', 'based in', 'operating in', 'serving',
        'around melbourne', 'around sydney', 'select a suburb',
        'choose from', 'delivered on behalf', 'welcome to',
        'your local', 'trusted resource', 'since 20',
        'discover the best', 'trade services', 'contact hours',
        'support contact', 'public holidays', 'monday', 'tuesday',
        'partnership', 'gov ', 'hospital', 'directory'
    ]
    
    if any(phrase in text_lower for phrase in generic_phrases):
        return False
    
    if text.startswith('http') or '://' in text:
        return False
    
    if any(ext in text_lower for ext in ['.jpg', '.png', '.gif', '.jpeg']):
        return False
    
    if len(text.split()) <= 3 and POSTCODE_RE.search(text):
        return False
    
    if len(text) < 15 or len(text) > 200:
        return False
    
    has_marker = ADDRESS_MARKERS.search(text_lower) is not None
    has_numbers = any(char.isdigit() for char in text)
    has_postcode = POSTCODE_RE.search(text) is not None
    
    if has_marker or (has_numbers and has_postcode and len(text) <= 100):
        return True
    
    return False


def extract_business_info_from_soup(soup):
    """Enhanced extraction with better filtering"""
    if soup is None:
        return {"info": "Failed to fetch page"}

    results = {
        "ABN": [],
        "ACN": [],
        "CompanyName": [],
        "Emails": [],
        "Phones": [],
        "Addresses": [],
        "StructuredData": [],
    }
    seen = {key: set() for key in results.keys()}
    found_abns = set()

    for text_node in soup.find_all(string=True):
        if not text_node.strip():
            continue
        
        parent = text_node.parent
        if parent and parent.name and parent.name.lower() in IGNORED_PARENT_TAGS:
            continue

        txt = text_node.strip()
        
        if len(txt) > 500:
            continue

        for m in ABN_RE.finditer(txt):
            normalized = normalize_abn_acn(m.group(1))
            if len(normalized) == 11 and normalized not in seen["ABN"]:
                seen["ABN"].add(normalized)
                found_abns.add(normalized)
                results["ABN"].append({"matched_text": normalized})
        
        for m in ACN_RE.finditer(txt):
            normalized = normalize_abn_acn(m.group(1))
            if len(normalized) == 9 and normalized not in seen["ACN"]:
                seen["ACN"].add(normalized)
                results["ACN"].append({"matched_text": normalized})

        for m in PTY_LTD_RE.finditer(txt):
            company = m.group(0).strip()
            if is_likely_company_name(company, parent) and company not in seen["CompanyName"]:
                seen["CompanyName"].add(company)
                results["CompanyName"].append({"matched_text": company})

        for em in EMAIL_RE.findall(txt):
            if em not in seen["Emails"] and not em.endswith(('.png', '.jpg', '.gif', '.jpeg')):
                seen["Emails"].add(em)
                results["Emails"].append({"matched_text": em})

        for m in PHONE_RE.finditer(txt):
            phone = normalize_phone(m.group(0))
            if phone:
                phone_digits = re.sub(r'[^\d]', '', phone)
                if phone_digits not in found_abns and phone not in seen["Phones"]:
                    seen["Phones"].add(phone)
                    results["Phones"].append({"matched_text": phone})

        if is_likely_address(txt):
            addr = txt.strip()
            if addr not in seen["Addresses"]:
                seen["Addresses"].add(addr)
                results["Addresses"].append({"matched_text": addr})

    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        
        orgs = []
        if isinstance(data, dict) and data.get("@type") == "Organization":
            orgs.append(data)
        elif isinstance(data, list):
            orgs = [item for item in data if isinstance(item, dict) and item.get("@type") == "Organization"]
        
        for org in orgs:
            org_data = {
                "name": org.get("name"),
                "telephone": org.get("telephone"),
                "email": org.get("email"),
            }
            
            if "address" in org:
                addr = org["address"]
                if isinstance(addr, dict):
                    addr_parts = [
                        addr.get("streetAddress"),
                        addr.get("addressLocality"),
                        addr.get("addressRegion"),
                        addr.get("postalCode"),
                    ]
                    org_data["address"] = ", ".join(filter(None, addr_parts))
                elif isinstance(addr, str):
                    org_data["address"] = addr
            
            org_data = {k: v for k, v in org_data.items() if v}
            if org_data and org_data not in results["StructuredData"]:
                results["StructuredData"].append(org_data)

    results = {k: v for k, v in results.items() if v}
    
    if not results:
        return {"info": "No business info found"}
    
    return results


def process_single_record(record):
    """Process a single record and return JSON string"""
    try:
        html = fetch_page_from_cc(record)
        info = extract_business_info_from_soup(html)
        return json.dumps(info, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)


def process_records_with_threading(rows):
    """Process multiple records in parallel using threading"""
    results = []
    
    def process_row_wrapper(row):
        record = row.asDict()
        business_info = process_single_record(record)
        record["business_info"] = business_info
        return record
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_row_wrapper, row): row for row in rows}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
    
    return results


def process_single_record_wrapper(warc_record_offset, warc_record_length, warc_filename, url, url_host_name, warc_record_date):
    """Wrapper for Spark UDF"""
    try:
        record = {
            "warc_record_offset": warc_record_offset,
            "warc_record_length": warc_record_length,
            "warc_filename": warc_filename,
            "url": url,
            "url_host_name": url_host_name,
            "warc_record_date": warc_record_date,
        }
        
        html = fetch_page_from_cc(record)
        info = extract_business_info_from_soup(html)
        return json.dumps(info, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)


@dag(
    dag_id="cc_business_info_extraction_single",
    description="Extract business information from Common Crawl files",
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["abr", "cc"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)
def cc_business_info_extraction_single_dag():
    @task
    def process_specific_batch() -> dict:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Lightweight Spark configuration for Docker container
        spark = (
            SparkSession.builder
            .appName("CCBusinessInfoExtraction")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.maxResultSize", "512m")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("ERROR")

        parquet_files = list(Path(INPUT_DIR).glob("*.parquet"))
        total_files = len(parquet_files)
        
        print(f"\n{'='*60}")
        print(f"Found {total_files} parquet files to process")
        print(f"Processing mode: {'Threading' if USE_THREADING else 'Spark Distributed'}")
        print(f"Max workers: {MAX_WORKERS if USE_THREADING else 'Spark default'}")
        print(f"Batch size: {BATCH_SIZE if BATCH_SIZE else 'All records'}")
        print(f"{'='*60}\n")

        total_processed = 0

        for idx, parquet_file in enumerate(parquet_files, 1):
            file_name = parquet_file.name
            print(f"[{idx}/{total_files}] Processing: {file_name}")

            try:
                df = spark.read.parquet(str(parquet_file))
                total_records = df.count()
                print(f"  Total records: {total_records:,}")

                if BATCH_SIZE:
                    df = df.limit(BATCH_SIZE)
                    print(f"  Processing: {BATCH_SIZE:,} records")
                
                if USE_THREADING:
                    # Threading approach - Good for I/O bound tasks in Docker
                    rows = df.collect()
                    print(f"  Fetching pages with {MAX_WORKERS} threads...")
                    
                    processed_rows = process_records_with_threading(rows)
                    processed_df = spark.createDataFrame(processed_rows)
                    
                else:
                    # Spark distributed approach
                    print("  Using Spark distributed processing...")
                    extract_business_info_udf = udf(process_single_record_wrapper, StringType())
                    
                    processed_df = df.withColumn(
                        "business_info",
                        extract_business_info_udf(
                            col("warc_record_offset"),
                            col("warc_record_length"),
                            col("warc_filename"),
                            col("url"),
                            col("url_host_name"),
                            col("warc_record_date"),
                        )
                    )

                output_path = f"{OUTPUT_DIR}/{file_name}"
                processed_df.write.mode("overwrite").parquet(output_path)

                processed_count = processed_df.count()
                total_processed += processed_count
                print(f"  ✓ Saved {processed_count:,} records\n")

            except Exception as e:
                print(f"  ✗ Error: {str(e)}\n")
                continue

        spark.stop()
        
        print(f"\n{'='*60}")
        print("Processing complete!")
        print(f"Total files: {total_files}")
        print(f"Total records processed: {total_processed:,}")
        print(f"{'='*60}\n")
        
        return {
            "total_files_processed": total_files,
            "total_records_processed": total_processed,
            "status": "completed"
        }

    process_specific_batch()


single_dag_instance = cc_business_info_extraction_single_dag()
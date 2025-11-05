from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import StringType
import requests
import json
import re
import os
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path


BASE_DIR = "/opt/shared-data"
INPUT_DIR = Path(os.path.join(BASE_DIR, "cc_index_data"))
OUTPUT_DIR = Path(os.path.join(BASE_DIR, "cc_processed_data"))
SPLIT_DIR = Path(os.path.join(BASE_DIR, "cc_split_data"))
CLEANED_URL_DIR = Path(os.path.join(BASE_DIR, "cc_cleaned_data"))
USER_AGENT = "cc-get-started/1.0 (Example data retrieval script; yourname@example.com)"

# === Regex Definitions ===
ABN_RE = re.compile(r"\bABN[:\s]*([0-9\s]{9,15}\d)\b", re.IGNORECASE)
ACN_RE = re.compile(r"\bACN[:\s]*([0-9\s]{9,15}\d)\b", re.IGNORECASE)
PTY_LTD_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&\-,\.\s]{2,}?(?:Pty\s+Ltd|Pty\.Ltd\.|Pty|Ltd|LLP|Inc|Corporation))\b",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.IGNORECASE
)
PHONE_RE = re.compile(
    r"(?:\+?\d{1,3}[\s\-\.])?(?:\(?\d\)?[\s\-\.]?)?(?:\d{2,4}[\s\-\.]){1,3}\d{2,4}",
    re.IGNORECASE,
)

STATE_TOKENS = [
    "vic",
    "nsw",
    "qld",
    "wa",
    "sa",
    "tas",
    "act",
    "nt",
    "canberra",
    "melbourne",
    "sydney",
    "brisbane",
    "perth",
    "adelaide",
    "darwin",
    "hobart",
]
STATE_REGEX = re.compile(
    r"\b(" + r"|".join(re.escape(t) for t in STATE_TOKENS) + r")\b", re.IGNORECASE
)
IGNORED_PARENT_TAGS = {"script", "style", "noscript", "meta", "link"}


def fetch_page_from_cc(record, myagent=USER_AGENT):
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
            timeout=30,
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
        print(f"Error fetching page: {e}")
        return None


def extract_business_info_from_soup(soup):
    if soup is None:
        return {"info": "Failed to fetch page"}

    results = {
        "ABN": [],
        "ACN": [],
        "CompanyName": [],
        "Emails": [],
        "Phones": [],
        "Addresses": [],
        "Industries": [],
        "StructuredData": [],
    }
    seen = set()

    def record(key, tag, text):
        snippet = str(tag)[:400] + "..." if len(str(tag)) > 400 else str(tag)
        sig = (key, text.strip(), hash(snippet))
        if sig not in seen:
            seen.add(sig)
            # results[key].append({"matched_text": text.strip(), "tag_html": snippet})
            results[key].append({"matched_text": text.strip()})

    for text_node in soup.find_all(string=True):
        if not text_node.strip():
            continue
        parent = text_node.parent
        if parent and parent.name and parent.name.lower() in IGNORED_PARENT_TAGS:
            continue

        txt = text_node.strip()

        for m in ABN_RE.finditer(txt):
            record("ABN", parent, m.group(1))
        for m in ACN_RE.finditer(txt):
            record("ACN", parent, m.group(1))
        for m in PTY_LTD_RE.finditer(txt):
            record("CompanyName", parent, m.group(0))
        for em in EMAIL_RE.findall(txt):
            record("Emails", parent, em)
        for ph in PHONE_RE.findall(txt):
            cleaned = re.sub(r"[^\d\+]", "", ph)
            if len(cleaned) >= 6:
                record("Phones", parent, ph)

        if STATE_REGEX.search(txt) and len(txt) <= 300:
            record("Addresses", parent, txt)

    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(script.string)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "Organization":
            results["StructuredData"].append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "Organization":
                    results["StructuredData"].append(item)

    results = {k: v for k, v in results.items() if v}
    if not results:
        return {"info": "No business info found"}
    return results


def process_single_record_py(record):
    """Helper to be used as Spark UDF to extract business info JSON string"""
    try:
        info = process_single_record(record)
        return info
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)


def process_single_record(record):
    html = fetch_page_from_cc(record)
    info = extract_business_info_from_soup(html)
    return json.dumps(info, ensure_ascii=False)


@dag(
    dag_id="cc_business_info_extraction_single",
    description="Extract business information from a single Common Crawl file",
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["abr", "cc"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def cc_business_info_extraction_single_dag():
    @task
    def split_parquet_files():
        os.makedirs(SPLIT_DIR, exist_ok=True)
        split_files = []
        for parquet_file in INPUT_DIR.glob("*.parquet"):
            pf = pq.ParquetFile(str(parquet_file))
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                split_filename = f"{parquet_file.stem}_rg{rg}.parquet"
                split_path = SPLIT_DIR / split_filename

                # Remove existing file if it exists to replace
                if split_path.exists():
                    split_path.unlink()

                pq.write_table(table, split_path)
                split_files.append(str(split_path))
        
        print(f"Number of split files created {len(split_files)}")

        return split_files

    @task
    def filter_info_urls(max_url_length: int = 80) -> str:
        os.makedirs(CLEANED_URL_DIR, exist_ok=True)
        spark = SparkSession.builder.appName("CCBusinessInfoExtraction").getOrCreate()

        parquet_files = list(SPLIT_DIR.glob("*.parquet"))
        total_files = len(parquet_files)

        for i, parquet_file in enumerate(parquet_files, start=1):
            print(
                f"Processing file {i}/{total_files}: {parquet_file} "
                f"(remaining: {total_files - i})"
            )

            df = spark.read.parquet(str(parquet_file))

            keywords = [
                "contact",
                "contact-us",
                "about",
                "about-us",
                "privacy",
                "privacy-policy",
                "help",
                "support",
                "terms",
                "company",
                "services",
            ]
            segment_patterns = [f"(?:/|-){kw}(?:/|$|\\.html|\\.php)" for kw in keywords]
            pattern = "(?i)(?:" + "|".join(segment_patterns) + ")"

            df_filtered = df.filter(
                (col("url").startswith("https:/"))
                & (col("url").rlike(pattern))
                & (length(col("url")) <= max_url_length)
            ).dropDuplicates(["url"])

            parquet_filename = str(parquet_file).split("/")[-1]
            parquet_path = f"{CLEANED_URL_DIR}/{parquet_filename}"

            df_filtered.write.mode("overwrite").parquet(parquet_path)

            print(f"Filtered data saved to: {parquet_path}")

        spark.stop()

    @task
    def process_specific_batch() -> dict:
        batch_file = "cc_au_filtered_part-00019-cd290efe-3ad3-4dd9-9ee4-2edabbb0d248.c000.gz_rg0.parquet"
        input_path = f"{CLEANED_URL_DIR}/{batch_file}"
        output_path = f"{OUTPUT_DIR}/processed_{batch_file}"

        print(f"Reading parquet file: {input_path}")

        spark = SparkSession.builder.appName("CCBusinessInfoExtraction").getOrCreate()

        df = spark.read.parquet(input_path)
        total_records = df.count()
        print(f"Total Records in file : {total_records}")

        # Collect all rows to driver since file is already small enough
        rows = df.limit(10).collect()

        processed_rows = []
        for row in rows:
            record = row.asDict()
            business_info = process_single_record(record)
            record["business_info"] = business_info
            processed_rows.append(record)

        processed_df = spark.createDataFrame(processed_rows)

        processed_df.write.mode("overwrite").parquet(output_path)

        print(f"Saved processed data to: {output_path}")

        spark.stop()

        return {
            "batch_file": batch_file,
            "output_file": output_path,
            "records_processed": total_records,
        }

    split_parquet_files() >> filter_info_urls() >> process_specific_batch()


single_dag_instance = cc_business_info_extraction_single_dag()

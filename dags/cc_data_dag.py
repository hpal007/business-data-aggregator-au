import os
import gzip
import requests
import logging
import duckdb
from io import BytesIO
from datetime import datetime
from airflow.decorators import dag, task  # Use official import for compatibility
from pathlib import Path
import pyarrow.parquet as pq

INDEX_NAME = "CC-MAIN-2025-13"
BASE_URL = "https://data.commoncrawl.org"
BASE_DIR = "/opt/shared-data/cc/"
DATA_DIR = os.path.join(BASE_DIR, "cc_index_data")
SPLIT_DIR = Path(os.path.join(BASE_DIR, "cc_split_data"))
CLEANED_URL_DIR = Path(os.path.join(BASE_DIR, "cc_cleaned_data"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


@dag(
    dag_id="CC_DATA_DAG",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["abr"],
)
def cc_data_dag():
    @task
    def get_parquet_files() -> list[str]:
        paths_url = f"{BASE_URL}/crawl-data/{INDEX_NAME}/cc-index-table.paths.gz"
        logger.info(f"Fetching list from: {paths_url}")
        try:
            response = requests.get(paths_url, timeout=30)
            response.raise_for_status()
            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
                content = gz.read().decode("utf-8")
            parquet_files = [
                line.strip()
                for line in content.split("\n")
                if line.strip() and "subset=warc" in line
            ]
            logger.info(f"Found {len(parquet_files)} warc parquet files")
            return parquet_files
        except Exception as e:
            logger.error(f"Error fetching parquet list: {e}")
            return []

    @task
    def process_files_with_duckdb(parquet_files: list[str], max_url_length: int = 80):
        con = duckdb.connect()
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        os.makedirs(DATA_DIR, exist_ok=True)

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
        keyword_clauses = " OR ".join([f"url ILIKE '%{kw}%'" for kw in keywords])

        au_urls = {}
        processed_count = 0
        failed_count = 0

        for idx, relative_url in enumerate(parquet_files):
            full_url = f"{BASE_URL}/{relative_url}"
            unique_id = os.path.basename(relative_url).replace(".parquet", "")
            try:
                logger.info(f"Processing {idx + 1}/{len(parquet_files)}: {unique_id}")
                select_query = f"""
                    SELECT DISTINCT url, url_host_tld, url_host_registered_domain, fetch_status, 
                        content_mime_detected, content_mime_type, warc_filename, 
                        warc_record_offset, warc_record_length
                    FROM parquet_scan('{full_url}')
                    WHERE url_host_tld = 'au'
                    AND fetch_status IN (200, 304)
                    AND (
                        content_mime_detected IN (
                            'text/html', 'application/xhtml+xml',
                            'application/rss+xml', 'application/xml',
                            'application/json'
                        )
                        OR content_mime_type = 'warc/revisit'
                    )
                    AND ({keyword_clauses})
                    AND length(url) <= {max_url_length}
                """
                count_query = f"SELECT count(*) FROM ({select_query}) AS subquery"
                row_count = con.execute(count_query).fetchone()[0]
                if row_count > 0:
                    logger.info(
                        f"✅ Found {row_count} matching AU rows in file {unique_id}"
                    )
                    output_path = os.path.join(DATA_DIR, f"{unique_id}.parquet")
                    write_query = f"""
                        COPY ({select_query}) TO '{output_path}' (FORMAT PARQUET, CODEC SNAPPY);
                    """
                    con.execute(write_query)
                    au_urls[relative_url] = row_count
                    processed_count += 1
                    logger.info(f"Processed urls with matching records: {au_urls}")
                else:
                    logger.info(f"🟡 No matching rows in file {unique_id}")
            except Exception as e:
                logger.error(
                    f"❌ Error processing {relative_url}: {type(e).__name__}: {e}"
                )
                failed_count += 1
                continue
        logger.info("🧾 Processing complete:")
        logger.info(f"   Total files processed: {len(parquet_files)}")
        logger.info(f"   Files with AU data: {processed_count}")
        logger.info(f"   Failed files: {failed_count}")
        logger.info(f"   Total AU records found: {sum(au_urls.values())}")
        con.close()
        return au_urls

    @task
    def split_parquet_files():
        os.makedirs(SPLIT_DIR, exist_ok=True)
        split_files = []
        for parquet_file in Path(DATA_DIR).glob("*.parquet"):
            pf = pq.ParquetFile(str(parquet_file))
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                split_filename = f"{parquet_file.stem}_rg{rg}.parquet"
                split_path = SPLIT_DIR / split_filename
                if split_path.exists():
                    split_path.unlink()
                pq.write_table(table, split_path)
                split_files.append(str(split_path))
        logger.info(f"Number of split files created {len(split_files)}")
        return split_files

    parquet_files_list = get_parquet_files()
    split_output = (
        process_files_with_duckdb(parquet_files_list) >> split_parquet_files()
    )


dag = cc_data_dag()

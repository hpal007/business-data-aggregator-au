import os
import math
import gzip
import requests
import logging
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import fsspec

from airflow.sdk import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import get_current_context

# ----------------------------- CONFIG -----------------------------------
INDEX_NAME = "CC-MAIN-2025-13"
BASE_URL = "https://data.commoncrawl.org"
BATCH_SIZE = 10
BASE_DIR = "/opt/shared-data"
DATA_DIR = os.path.join(BASE_DIR, "cc_index_data")

# os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------


def read_table_from_url(url: str):
    """Read a Parquet file from the Common Crawl bucket."""
    try:
        full_url = f"{BASE_URL}/{url}"
        logger.info(f"Reading Parquet file from: {full_url}")

        with fsspec.open(full_url, "rb") as f:
            cc_index_data = pq.read_table(f, columns=["url_host_tld"])

        logger.info(f"au_records:{cc_index_data.filter(pc.equal(cc_index_data['url_host_tld'], 'au')).num_rows}")

        if (cc_index_data.filter(pc.equal(cc_index_data["url_host_tld"], "au")).num_rows> 0):
            with fsspec.open(full_url, "rb") as f:
                return pq.read_table(
                    f,
                    columns=[
                        "url_host_tld",
                        "url",
                        "warc_filename",
                        "warc_record_offset",
                        "warc_record_length",
                        "fetch_status",
                        "content_mime_detected",
                        "url_host_registered_domain"
                    ],
                )
        else:
            return "NO AU RECORDS"
    except Exception as e:
        logger.warning(f"Failed to read {url}: {e}")
        return None


# =============================== DAG ====================================
@dag(
    dag_id="CC_DATA_DAG",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["abr", "cc"],
)
def cc_data_dag_dynamic():
    """Dynamically process Common Crawl index files in batches."""

    # --------------------------------------------------------------------
    @task
    def get_parquet_files() -> list[str]:
        """Download the list of Parquet paths for this crawl index."""
        paths_url = f"{BASE_URL}/crawl-data/{INDEX_NAME}/cc-index-table.paths.gz"
        logger.info(f"Fetching list from: {paths_url}")

        try:
            response = requests.get(paths_url, timeout=30)
            response.raise_for_status()

            with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
                content = gz.read().decode("utf-8")

            parquet_files = [
                line.strip() for line in content.split("\n") if line.strip()
            ]
            logger.info(f"Found {len(parquet_files)} parquet files")
            return parquet_files

        except Exception as e:
            logger.error(f"Error fetching parquet list: {e}")
            return []

    # --------------------------------------------------------------------
    @task
    def split_into_batches(parquet_files: list[str]) -> list[list[str]]:
        """Split the full list into batches for dynamic mapping."""
        total = len(parquet_files)
        num_batches = math.ceil(total / BATCH_SIZE)
        batches = [
            parquet_files[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
            for i in range(num_batches)
        ]
        logger.info(
            f"Split {total} files into {num_batches} batches of size {BATCH_SIZE}"
        )
        return batches[0:10]

    # --------------------------------------------------------------------
    @task
    def process_batch(batch_files: list[str]):
        """Process one batch dynamically – runs as a separate mapped task."""
        context = get_current_context()
        ti = context["ti"]
        batch_idx = getattr(ti, "map_index", 0)

        logger.info(f"Processing batch {batch_idx} with {len(batch_files)} files")

        failed_count = 0
        filtered_chunks = []

        # Threaded read for better throughput (safe inside mapped task)
        with ThreadPoolExecutor(max_workers=10) as executor:
            for tbl in executor.map(read_table_from_url, batch_files):
                if tbl is None:
                    failed_count += 1
                    continue
                if tbl == "NO AU RECORDS":
                    logger.info(f"Batch {batch_idx} found no AU records")
                    continue
                try:
                    mask = pc.equal(tbl["url_host_tld"], "au")
                    filtered_tbl = tbl.filter(mask)
                    if filtered_tbl.num_rows > 0:
                        filtered_chunks.append(filtered_tbl)
                except Exception as e:
                    logger.warning(f"Error filtering table: {e}")
                    failed_count += 1

        if filtered_chunks:
            result = pa.concat_tables(filtered_chunks)
            df = result.to_pandas()
            os.makedirs(DATA_DIR, exist_ok=True)
            output_csv = os.path.join(
                DATA_DIR, f"cc_au_domain_records_batch_{batch_idx}.csv"
            )
            df.to_csv(output_csv, index=False)  # TODO change to parquet
            logger.info(f"✅ Written {len(df)} AU records to {output_csv}")
        else:
            logger.info(f"No AU records found in batch {batch_idx}")

        logger.info(f"Batch {batch_idx} done. Failed count: {failed_count}")

    # --------------------------------------------------------------------
    parquet_files = get_parquet_files()
    batches = split_into_batches(parquet_files)
    process_batch.expand(batch_files=batches)
    # --------------------------------------------------------------------


# Instantiate DAG
dag = cc_data_dag_dynamic()

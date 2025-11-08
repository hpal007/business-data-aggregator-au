from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, col
import requests
import json
import re
import os
import logging
import gzip
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path
import duckdb
from io import BytesIO
import pyarrow.parquet as pq
from job_business_extract import process_partition
import gc
import time
import shutil


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

INDEX_NAME = "CC-MAIN-2025-13"
BASE_URL = "https://data.commoncrawl.org"
BASE_DIR = "/opt/shared-data/cc/"
OUTPUT_DIR = Path(os.path.join(BASE_DIR, "cc_processed_data"))
SPLIT_DIR = Path(os.path.join(BASE_DIR, "cc_split_data"))
DATA_DIR = os.path.join(BASE_DIR, "cc_index_data")

POSTGRES_JDBC_URL = "jdbc:postgresql://target_postgres:5432/target_db"
POSTGRES_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_pass",
    "driver": "org.postgresql.Driver",
}

# === Configuration ===


def get_spark_session(app_name: str = "cc"):
    """
    OPTIMIZED: Increased resources to use available CPU and memory
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[8]")  # CHANGED: Increased from 7 to 8 cores
        .config("spark.driver.memory", "7g")  # CHANGED: Increased from 6g to 7g
        .config("spark.driver.cores", "8")  # CHANGED: Increased from 6 to 8
        .config("spark.executor.memory", "7g")  # CHANGED: Increased from 6g to 7g
        .config("spark.executor.cores", "8")  # CHANGED: Increased from 6 to 8
        .config("spark.sql.shuffle.partitions", "64")  # CHANGED: Increased from 56 to 64
        .config("spark.driver.maxResultSize", "3g")  # CHANGED: Increased from 2g to 3g
        .config("spark.submit.pyFiles", "/opt/airflow/dags/job_business_extract.py")
        # .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.shuffle.compress", "true")
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.io.compression.codec", "snappy")
        .config("spark.broadcast.compress", "true")
        .config("spark.rdd.compress", "true")
        .config("spark.speculation", "true")
        .config("spark.speculation.multiplier", "1.3")
        .config("spark.speculation.quantile", "0.75")
        .config("spark.network.timeout", "600s")
        .config("spark.sql.files.maxPartitionBytes", "128mb")  # CHANGED: Decreased from 256mb to 128mb for finer partitioning
        .config("spark.sql.files.minPartitionNum", "16")  # CHANGED: Increased from 8 to 16
        .config("spark.default.parallelism", "64")  # CHANGED: Increased from 56 to 64
        .config("spark.task.maxFailures", "4")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeoutInterval", "600s")
        .config("spark.local.dir", "/tmp/spark-temp")  # NEW: Prevent memory overflow
        .getOrCreate()
    )

    return spark


@dag(
    dag_id="AU_Common_Crawl_DAG",
    description="Extract business information from Common Crawl files using Spark distributed processing",
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["au", "cc", "abr"],
    max_active_tasks=2,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)
def cc_business_info_extraction_dag():
    @task
    def get_cc_index_urls() -> list[str]:
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

        for idx, relative_url in enumerate(parquet_files[:50]): #TODO remove the is limit of 50
            full_url = f"{BASE_URL}/{relative_url}"
            unique_id = os.path.basename(relative_url).replace(".parquet", "")
            try:
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
                    print(f"\n{idx + 1} found ({row_count} rows)", flush=True)
                    output_path = os.path.join(DATA_DIR, f"{unique_id}.parquet")
                    write_query = f"""
                        COPY ({select_query}) TO '{output_path}' (FORMAT PARQUET, CODEC SNAPPY);
                    """
                    con.execute(write_query)
                    au_urls[relative_url] = row_count
                    processed_count += 1
                    logger.info(f"Processed urls with matching records: {au_urls}")
                else:
                    print(f"{idx + 1} > ", end="", flush=True)
            except Exception as e:
                logger.error(
                    f"\nError processing {relative_url}: {type(e).__name__}: {e}"
                )
                failed_count += 1
                continue

        logger.info("\n... ... ... ... ... ... ... ... ... ... ... ... ...")
        logger.info("Processing complete:")
        logger.info(f"   Total files processed: {len(parquet_files)}")
        logger.info(f"   Files with AU data: {processed_count}")
        logger.info(f"   Failed files: {failed_count}")
        logger.info(f"   Total AU records found: {sum(au_urls.values())}")
        con.close()
        return au_urls

    @task
    def split_parquet_files():
        """
        Two-level splitting:
        1. First split: By row groups (existing)
        2. Second split: If row count > 30,000, split further into chunks
        """
        os.makedirs(SPLIT_DIR, exist_ok=True)
        split_files = []

        MAX_ROWS_PER_FILE = 10000

        for parquet_file in Path(DATA_DIR).glob("*.parquet"):
            pf = pq.ParquetFile(str(parquet_file))

            # First split: by row groups
            for rg in range(pf.num_row_groups):
                table = pf.read_row_group(rg)
                num_rows = len(table)

                logger.info(f"Processing row group {rg} with {num_rows:,} rows")

                # Check if needs second split
                if num_rows > MAX_ROWS_PER_FILE:
                    # Second split: divide into smaller chunks
                    num_chunks = (num_rows + MAX_ROWS_PER_FILE - 1) // MAX_ROWS_PER_FILE
                    logger.info(
                        f"  Row group {rg} exceeds {MAX_ROWS_PER_FILE:,} rows. Splitting into {num_chunks} chunks"
                    )

                    for chunk_idx in range(num_chunks):
                        start_row = chunk_idx * MAX_ROWS_PER_FILE
                        end_row = min((chunk_idx + 1) * MAX_ROWS_PER_FILE, num_rows)

                        # Extract chunk from table
                        chunk_table = table.slice(start_row, end_row - start_row)
                        chunk_rows = len(chunk_table)

                        # Save chunk with unique name: filename_rg{X}_chunk{Y}.parquet
                        split_filename = (
                            f"{parquet_file.stem}_rg{rg}_chunk{chunk_idx}.parquet"
                        )
                        split_path = SPLIT_DIR / split_filename

                        if split_path.exists():
                            split_path.unlink()

                        pq.write_table(chunk_table, split_path)
                        split_files.append(str(split_path))
                        logger.info(
                            f"    Saved chunk {chunk_idx} ({chunk_rows:,} rows) -> {split_filename}"
                        )
                else:
                    # No second split needed, save as-is
                    split_filename = f"{parquet_file.stem}_rg{rg}.parquet"
                    split_path = SPLIT_DIR / split_filename

                    if split_path.exists():
                        split_path.unlink()

                    pq.write_table(table, split_path)
                    split_files.append(str(split_path))
                    logger.info(
                        f"  Saved row group {rg} ({num_rows:,} rows) -> {split_filename}"
                    )

        logger.info(f"Total split files created: {len(split_files)}")
        return split_files

    @task
    def process_single_file(file_path: str) -> str:
        """
        OPTIMIZED: Single-pass processing instead of batching.
        Processes entire file in ONE Spark job with partitioned parallelism.
        
        NO MORE BATCHING = 10x faster!
        """
        spark = get_spark_session(file_path)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        parquet_file = Path(file_path)
        file_name = parquet_file.name
        file_start_time = time.time()

        output_schema = StructType(
            [
                StructField("url", StringType(), True),
                StructField("url_host_tld", StringType(), True),
                StructField("url_host_registered_domain", StringType(), True),
                StructField("fetch_status", StringType(), True),
                StructField("content_mime_detected", StringType(), True),
                StructField("content_mime_type", StringType(), True),
                StructField("warc_filename", StringType(), True),
                StructField("warc_record_offset", LongType(), True),
                StructField("warc_record_length", LongType(), True),
                StructField("ABN", StringType(), True),
                StructField("ACN", StringType(), True),
                StructField("CompanyName", StringType(), True),
                StructField("business_info", StringType(), True),
            ]
        )

        logger.info(f"\nProcessing: {file_name}")
        logger.info(f"  Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            df = spark.read.parquet(str(parquet_file))
            total_records = df.count()
            logger.info(f"  Total records in file: {total_records:,}")

            # CHANGED: REMOVED BATCHING - Now using single-pass with partitions
            # Repartition for parallel processing across cores
            num_partitions = 64  # Matches spark.default.parallelism
            logger.info(f"  Processing in {num_partitions} partitions (parallel, not sequential)")
            logger.info(f"  {'=' * 55}")

            file_stats = {"success": 0, "errors": 0}
            error_stats = {
                "timeout": 0,
                "connection_error": 0,
                "invalid_status": 0,
                "empty_content": 0,
                "empty_html": 0,
                "no_response_record": 0,
                "warc_parse_error": 0,
                "invalid_record": 0,
                "processing_error": 0,
                "other_errors": 0,
                "success": 0,
            }

            # OPTIMIZED: Single transformation pipeline (NO BATCHING)
            processed_df = (
                df
                .repartition(num_partitions)  # Split into 64 partitions for parallel processing
                .rdd
                .mapPartitions(process_partition)  # Process each partition in parallel
                .toDF(schema=output_schema)
            )

            # Cache for stats collection
            processed_df.cache()
            
            # Get total count for logging
            batch_count = processed_df.count()
            file_elapsed = (time.time() - file_start_time) / 60
            batch_rate = batch_count / (file_elapsed * 60) if file_elapsed > 0 else 0

            logger.info(
                f"  ✓ Processing complete: {batch_count:,} records processed"
            )
            logger.info(f"  Rate: {batch_rate:.1f} rec/sec")
            logger.info(f"  Elapsed: {file_elapsed:.1f}min")

            # Collect business_info for stats (small overhead)
            business_info_rows = processed_df.select("business_info").collect()
            for row in business_info_rows:
                try:
                    info = json.loads(row.business_info)
                    status = info.get("fetch_status", "success")
                    if status == "success":
                        file_stats["success"] += 1
                        error_stats["success"] += 1
                    elif status in error_stats:
                        file_stats["errors"] += 1
                        error_stats[status] += 1
                    else:
                        file_stats["errors"] += 1
                        error_stats["other_errors"] += 1
                except Exception:
                    file_stats["errors"] += 1
                    error_stats["other_errors"] += 1

            logger.info(f"  {'=' * 55}")

            # OPTIMIZED: Single write operation (was 60 writes in batching)
            final_output_path = f"{OUTPUT_DIR}/{file_name}"
            processed_df.write.mode("overwrite").parquet(final_output_path)

            file_elapsed = (time.time() - file_start_time) / 60
            logger.info(
                f"  ✓ File complete: {file_stats['success']:,} success, {file_stats['errors']:,} errors"
            )
            logger.info(f"  Total time: {file_elapsed:.1f} minutes\n")

            processed_df.unpersist()
            return final_output_path

        except Exception as e:
            logger.error(f"  ✗ Error processing file: {str(e)}\n")
            raise
        finally:
            spark.stop()

    @task
    def create_cc_table(input_dir):
        """
        OPTIMIZED: Use coalesce instead of repartition to avoid unnecessary shuffle
        """
        spark = get_spark_session("Merge")

        try:
            logger.info(f"Merging files from: {input_dir}")
            input_path = f"{input_dir}/*.parquet"
            output_file = "/opt/shared-data/cc/cc_merged.parquet"

            df = spark.read.parquet(input_path)
            
            # CHANGED: Use coalesce instead of repartition to avoid shuffle
            df.coalesce(1).write.mode("overwrite").parquet(output_file)

            logger.info(f"Merged {df.count()} rows into {output_file}")
            return output_file
        finally:
            spark.stop()

    @task
    def load_table(parquet_path, table_name):
        """Load merged parquet data to PostgreSQL"""
        spark = get_spark_session("Load")

        try:
            logger.info(f"Loading {parquet_path} to {table_name}")
            df = spark.read.parquet(parquet_path)

            df.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option(
                "dbtable", table_name
            ).option("user", POSTGRES_PROPERTIES["user"]).option(
                "password", POSTGRES_PROPERTIES["password"]
            ).option("driver", POSTGRES_PROPERTIES["driver"]).mode("overwrite").save()

            logger.info(f"{table_name} loaded successfully")
            return f"{table_name} loaded"
        finally:
            spark.stop()

    # Task execution
    cc_index_urls_list = get_cc_index_urls()
    au_urls = process_files_with_duckdb(cc_index_urls_list)
    split_files_list = split_parquet_files()

    # Process files with dynamic mapping (now with parallel execution enabled)
    processed_files = process_single_file.expand(file_path=split_files_list)

    # Merge and load after all files are processed
    merged_parquet = create_cc_table(input_dir=OUTPUT_DIR)
    cc_table = load_table(parquet_path=merged_parquet, table_name="cc_table")

    # Set dependencies
    (
        cc_index_urls_list
        >> au_urls
        >> split_files_list
        >> processed_files
        >> merged_parquet
        >> cc_table
    )


dag_instance = cc_business_info_extraction_dag()
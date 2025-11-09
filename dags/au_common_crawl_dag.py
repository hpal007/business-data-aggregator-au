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
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints")
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, f"dag_checkpoint_{INDEX_NAME}.json")

POSTGRES_JDBC_URL = "jdbc:postgresql://target_postgres:5432/target_db"
POSTGRES_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_pass",
    "driver": "org.postgresql.Driver",
}

# === Configuration ===
BATCH_SIZE = 500  # Process records at a time, set to None for all records


class CheckpointManager:
    """Manages checkpoint state for DAG tasks."""

    def __init__(self, checkpoint_file, index_name):
        self.checkpoint_file = checkpoint_file
        self.index_name = index_name
        self.state = self._load_checkpoint()

    def _load_checkpoint(self):
        """Load checkpoint state from file."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}. Starting fresh.")
                return {
                    "index_name": self.index_name,
                    "tasks": {},
                    "created_at": datetime.now().isoformat(),
                }
        return {
            "index_name": self.index_name,
            "tasks": {},
            "created_at": datetime.now().isoformat(),
        }

    def _save_checkpoint(self):
        """Save checkpoint state to file."""
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.state, f, indent=2)
        logger.info(f"Checkpoint saved: {self.checkpoint_file}")

    def is_task_completed(self, task_name):
        """Check if a task has been completed."""
        return (
            self.state.get("tasks", {}).get(task_name, {}).get("status") == "completed"
        )

    def mark_task_completed(self, task_name, metadata=None):
        """Mark a task as completed."""
        if "tasks" not in self.state:
            self.state["tasks"] = {}
        self.state["tasks"][task_name] = {
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "metadata": metadata or {},
        }
        self._save_checkpoint()
        logger.info(f"✅ Task '{task_name}' marked as completed")

    def mark_task_failed(self, task_name, error_msg=None):
        """Mark a task as failed."""
        if "tasks" not in self.state:
            self.state["tasks"] = {}
        self.state["tasks"][task_name] = {
            "status": "failed",
            "failed_at": datetime.now().isoformat(),
            "error": error_msg,
        }
        self._save_checkpoint()
        logger.warning(f"❌ Task '{task_name}' marked as failed")

    def get_task_metadata(self, task_name):
        """Get metadata for a completed task."""
        return self.state.get("tasks", {}).get(task_name, {}).get("metadata", {})

    def reset_checkpoint(self):
        """Reset checkpoint for a fresh start."""
        self.state = {
            "index_name": self.index_name,
            "tasks": {},
            "created_at": datetime.now().isoformat(),
        }
        self._save_checkpoint()
        logger.info("✅ Checkpoint reset")


checkpoint_manager = CheckpointManager(CHECKPOINT_FILE, INDEX_NAME)


def get_spark_session(app_name: str = "cc"):
    """
    OPTIMIZED: Increased resources to use available CPU and memory
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[8]")
        .config("spark.driver.memory", "7g")
        .config("spark.driver.cores", "8")
        .config("spark.executor.memory", "7g")
        .config("spark.executor.cores", "8")
        .config(
            "spark.sql.shuffle.partitions", "64"
        )  # CHANGED: Increased from 56 to 64
        .config("spark.driver.maxResultSize", "3g")
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
        .config("spark.sql.files.maxPartitionBytes", "128mb")
        .config("spark.sql.files.minPartitionNum", "16")
        .config("spark.default.parallelism", "34")
        .config("spark.task.maxFailures", "4")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeoutInterval", "600s")
        .config("spark.local.dir", "/tmp/spark-temp")
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
        if checkpoint_manager.is_task_completed("get_cc_index_urls"):
            logger.info("⏭️  Skipping get_cc_index_urls - already completed")
            return checkpoint_manager.get_task_metadata("get_cc_index_urls").get(
                "parquet_files", []
            )

        try:
            paths_url = f"{BASE_URL}/crawl-data/{INDEX_NAME}/cc-index-table.paths.gz"
            logger.info(f"Fetching list from: {paths_url}")
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
            checkpoint_manager.mark_task_completed(
                "get_cc_index_urls", {"parquet_files": parquet_files}
            )
            return parquet_files
        except Exception as e:
            logger.error(f"Error fetching parquet list: {e}")
            checkpoint_manager.mark_task_failed("get_cc_index_urls", str(e))
            raise

    @task
    def process_files_with_duckdb(parquet_files: list[str], max_url_length: int = 80):
        if checkpoint_manager.is_task_completed("process_files_with_duckdb"):
            logger.info("⏭️  Skipping process_files_with_duckdb - already completed")
            return checkpoint_manager.get_task_metadata(
                "process_files_with_duckdb"
            ).get("au_urls", {})

        try:
            os.makedirs(DATA_DIR, exist_ok=True)

            con = duckdb.connect()
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")

            keywords = [
                "contact",
                "contact-us",
                "about",
                "about-us",
                "privacy",
                "privacy-policy",
                "help",
                "support",
            ]
            keyword_clauses = " OR ".join([f"url ILIKE '%{kw}%'" for kw in keywords])

            au_urls = {}
            processed_count = 0
            failed_count = 0

            for idx, relative_url in enumerate(parquet_files[:30]):  # TODO remove this
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

                        deduplicated_query = f"""
                                SELECT url, url_host_tld, url_host_registered_domain, fetch_status, 
                                    content_mime_detected, content_mime_type, warc_filename, 
                                    warc_record_offset, warc_record_length
                                FROM ({select_query}) AS base_query
                                WHERE (url_host_registered_domain, length(url)) IN (
                                    SELECT url_host_registered_domain, MIN(length(url))
                                    FROM ({select_query}) AS dedup_query
                                    GROUP BY url_host_registered_domain
                                )
                            """

                        dedup_count_query = f"SELECT count(*) FROM ({deduplicated_query}) AS final_count"
                        dedup_row_count = con.execute(dedup_count_query).fetchone()[0]

                        output_path = os.path.join(DATA_DIR, f"{unique_id}.parquet")
                        write_query = f"""
                                COPY ({deduplicated_query}) TO '{output_path}' (FORMAT PARQUET, CODEC SNAPPY);
                            """
                        con.execute(write_query)
                        au_urls[relative_url] = dedup_row_count
                        processed_count += 1
                        logger.info(f"Processed urls with matching records: {au_urls}")
                        logger.info(
                            f"Original row count: {row_count}, Deduplicated row count: {dedup_row_count}"
                        )
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

            checkpoint_manager.mark_task_completed(
                "process_files_with_duckdb", {"au_urls": au_urls}
            )
            return au_urls
        except Exception as e:
            checkpoint_manager.mark_task_failed("process_files_with_duckdb", str(e))
            raise

    @task
    def split_parquet_files():
        """
        Two-level splitting:
        1. First split: By row groups (existing)
        2. Second split: If row count > 30,000, split further into chunks
        """
        if checkpoint_manager.is_task_completed("split_parquet_files"):
            logger.info("⏭️  Skipping split_parquet_files - already completed")
            return checkpoint_manager.get_task_metadata("split_parquet_files").get(
                "split_files", []
            )

        try:
            os.makedirs(SPLIT_DIR, exist_ok=True)
            split_files = []

            MAX_ROWS_PER_FILE = 2000

            for parquet_file in Path(DATA_DIR).glob("*.parquet"):
                pf = pq.ParquetFile(str(parquet_file))

                for rg in range(pf.num_row_groups):
                    table = pf.read_row_group(rg)
                    num_rows = len(table)

                    logger.info(f"Processing row group {rg} with {num_rows:,} rows")

                    if num_rows > MAX_ROWS_PER_FILE:
                        num_chunks = (
                            num_rows + MAX_ROWS_PER_FILE - 1
                        ) // MAX_ROWS_PER_FILE
                        logger.info(
                            f"  Row group {rg} exceeds {MAX_ROWS_PER_FILE:,} rows. Splitting into {num_chunks} chunks"
                        )

                        for chunk_idx in range(num_chunks):
                            start_row = chunk_idx * MAX_ROWS_PER_FILE
                            end_row = min((chunk_idx + 1) * MAX_ROWS_PER_FILE, num_rows)

                            chunk_table = table.slice(start_row, end_row - start_row)
                            chunk_rows = len(chunk_table)

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
            checkpoint_manager.mark_task_completed(
                "split_parquet_files", {"split_files": split_files}
            )
            return split_files
        except Exception as e:
            checkpoint_manager.mark_task_failed("split_parquet_files", str(e))
            raise

    @task
    def process_single_file(file_path: str) -> str:
        """
        Process a single parquet file with progress monitoring.
        Returns the output path.
        """
        task_id = f"process_single_file_{Path(file_path).stem}"

        if checkpoint_manager.is_task_completed(task_id):
            logger.info(f"⏭️  Skipping {task_id} - already completed")
            return checkpoint_manager.get_task_metadata(task_id).get("output_path")

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
                StructField("cc_abn", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("business_info", StringType(), True),
                StructField("raw_text_body", StringType(), True),
            ]
        )

        logger.info(f"\nProcessing: {file_name}")
        logger.info(f"  Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            df = spark.read.parquet(str(parquet_file))
            total_records = df.count()
            logger.info(f"  Total records in file: {total_records:,}")

            BATCH_RECORDS = 500
            num_batches = (total_records + BATCH_RECORDS - 1) // BATCH_RECORDS

            logger.info(
                f"  Processing in {num_batches} batches of {BATCH_RECORDS} records"
            )
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

            batch_output_dir = (
                f"{OUTPUT_DIR}/.batch_{Path(file_name).stem}_{int(time.time())}"
            )
            os.makedirs(batch_output_dir, exist_ok=True)

            for batch_idx in range(num_batches):
                batch_start = time.time()
                offset = batch_idx * BATCH_RECORDS
                batch_end = min(offset + BATCH_RECORDS, total_records)
                actual_batch_size = batch_end - offset

                batch_df = (
                    df.limit(batch_end).subtract(df.limit(offset))
                    if offset > 0
                    else df.limit(BATCH_RECORDS)
                )

                num_partitions = max(1, actual_batch_size // 50)
                batch_df = batch_df.repartition(num_partitions)

                processed_rdd = batch_df.rdd.mapPartitions(process_partition)
                processed_batch_df = spark.createDataFrame(
                    processed_rdd, schema=output_schema
                )
                processed_batch_df.cache()

                batch_count = processed_batch_df.count()
                batch_elapsed = time.time() - batch_start
                batch_rate = batch_count / batch_elapsed if batch_elapsed > 0 else 0

                batch_output_path = f"{batch_output_dir}/batch_{batch_idx:05d}.parquet"
                processed_batch_df.write.mode("overwrite").parquet(batch_output_path)

                elapsed_minutes = (time.time() - file_start_time) / 60
                logger.info(
                    f"  Batch {batch_idx + 1}/{num_batches}: "
                    f"Records {offset:,}-{batch_end:,} | "
                    f"Saved {batch_count:,} | "
                    f"Rate: {batch_rate:.1f} rec/sec | "
                    f"Elapsed: {elapsed_minutes:.1f}min"
                )

                business_info_rows = processed_batch_df.select(
                    "business_info"
                ).collect()
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

                processed_batch_df.unpersist()
                gc.collect()

            logger.info(f"  {'=' * 55}")
            logger.info(f"  Merging {num_batches} batches into final file...")

            merged_df = spark.read.parquet(f"{batch_output_dir}/batch_*.parquet")
            final_output_path = f"{OUTPUT_DIR}/{file_name}"
            merged_df.write.mode("overwrite").parquet(final_output_path)

            shutil.rmtree(batch_output_dir, ignore_errors=True)

            file_elapsed = (time.time() - file_start_time) / 60
            logger.info(
                f"  ✅ File complete: {file_stats['success']:,} success, {file_stats['errors']:,} errors"
            )
            logger.info(f"  Total time: {file_elapsed:.1f} minutes\n")

            checkpoint_manager.mark_task_completed(
                task_id, {"output_path": final_output_path}
            )
            return final_output_path

        except Exception as e:
            logger.info(f"  ❌ Error processing file: {str(e)}\n")
            checkpoint_manager.mark_task_failed(task_id, str(e))
            raise
        finally:
            spark.stop()

    @task
    def create_cc_table(input_dir):
        """Merge all processed parquet files into a single output file"""
        if checkpoint_manager.is_task_completed("create_cc_table"):
            logger.info("⏭️  Skipping create_cc_table - already completed")
            return checkpoint_manager.get_task_metadata("create_cc_table").get(
                "output_file"
            )

        spark = get_spark_session("Merge")

        try:
            logger.info(f"Merging files from: {input_dir}")
            input_path = f"{input_dir}/*.parquet"
            output_file = "/opt/shared-data/cc/cc_merged.parquet"

            df = spark.read.parquet(input_path)
            df.coalesce(1).write.mode("overwrite").parquet(output_file)

            logger.info(f"Merged {df.count()} rows into {output_file}")
            checkpoint_manager.mark_task_completed(
                "create_cc_table", {"output_file": output_file}
            )
            return output_file
        except Exception as e:
            checkpoint_manager.mark_task_failed("create_cc_table", str(e))
            raise
        finally:
            spark.stop()

    @task
    def load_table(parquet_path):
        """Load merged parquet data to PostgreSQL"""
        if checkpoint_manager.is_task_completed("load_table"):
            logger.info("⏭️  Skipping load_table - already completed")
            return "cc_table loaded (from checkpoint)"

        spark = get_spark_session("Load")

        try:
            table_name = "cc_table"
            logger.info(f"Loading {parquet_path} to {table_name}")
            df = spark.read.parquet(parquet_path)

            df.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option(
                "dbtable", table_name
            ).option("user", POSTGRES_PROPERTIES["user"]).option(
                "password", POSTGRES_PROPERTIES["password"]
            ).option("driver", POSTGRES_PROPERTIES["driver"]).mode("overwrite").save()

            logger.info(f"{table_name} loaded successfully")
            checkpoint_manager.mark_task_completed(
                "load_table", {"table_name": table_name}
            )
            return f"{table_name} loaded"
        except Exception as e:
            checkpoint_manager.mark_task_failed("load_table", str(e))
            raise
        finally:
            spark.stop()

    # Task execution
    cc_index_urls_list = get_cc_index_urls()
    au_urls = process_files_with_duckdb(cc_index_urls_list)
    split_files_list = split_parquet_files()

    processed_files = process_single_file.expand(file_path=split_files_list)

    # Merge and load after all files are processed
    merged_parquet = create_cc_table(input_dir=OUTPUT_DIR)
    cc_table = load_table(parquet_path=merged_parquet)

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

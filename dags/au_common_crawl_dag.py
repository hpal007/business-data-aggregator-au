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
BATCH_SIZE = 100  # Process records at a time, set to None for all records


spark = spark = (
    SparkSession.builder.appName("CCBusinessInfoExtraction")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.submit.pyFiles", "/opt/airflow/dags/job_business_extract.py")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)


@dag(
    dag_id="AU_Common_Crawl_DAG",
    description="Extract business information from Common Crawl files using Spark distributed processing",
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["au", "cc", "abr"],
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

        for idx, relative_url in enumerate(parquet_files):
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
                    f"\n❌ Error processing {relative_url}: {type(e).__name__}: {e}"
                )
                failed_count += 1
                continue

        print()  # Final newline
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

    @task
    def process_with_spark_distributed() -> dict:
        """
        Process all parquet files using Spark distributed processing with mapPartitions.
        Processes each file completely in chunks before moving to the next file.
        """

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        parquet_files = list(Path(SPLIT_DIR).glob("*.parquet"))
        total_files = len(parquet_files)

        print(f"\n{'=' * 60}")
        print(f"Found {total_files} parquet files to process")
        print(
            "Processing mode: Spark Distributed (mapPartitions) with File-Level Chunking"
        )
        print(f"Chunk size: {BATCH_SIZE if BATCH_SIZE else 100} records")
        print(f"{'=' * 60}\n")

        total_processed = 0
        total_error_stats = {
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

        # Define output schema
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

        for file_idx, parquet_file in enumerate(parquet_files, 1):
            file_name = parquet_file.name
            print(f"[{file_idx}/{total_files}] Processing: {file_name}")

            try:
                # Read parquet file
                df = spark.read.parquet(str(parquet_file))
                total_records = df.count()
                print(f"  Total records: {total_records:,}")

                chunk_size = BATCH_SIZE if BATCH_SIZE else 100
                num_chunks = (total_records + chunk_size - 1) // chunk_size

                print(
                    f"  Chunking strategy: {num_chunks} chunks of {chunk_size} records each"
                )
                print(f"  {'=' * 55}")

                file_processed_count = 0
                file_chunk_stats = {"success": 0, "errors": 0}

                # Collect all chunks first (write to temporary directory)
                temp_chunk_dir = (
                    f"{OUTPUT_DIR}/.temp_{Path(file_name).stem}_{int(time.time())}"
                )
                os.makedirs(temp_chunk_dir, exist_ok=True)

                # Process each chunk sequentially
                for chunk_idx in range(num_chunks):
                    offset = chunk_idx * chunk_size
                    chunk_end = min(offset + chunk_size, total_records)
                    actual_chunk_size = chunk_end - offset

                    print(
                        f"  Chunk {chunk_idx + 1}/{num_chunks}: Processing records {offset:,} to {chunk_end:,} ({actual_chunk_size:,} records)..."
                    )

                    try:
                        # Use offset and limit for efficient chunking
                        chunk_df = (
                            df.limit(chunk_end).subtract(df.limit(offset))
                            if offset > 0
                            else df.limit(chunk_size)
                        )

                        # Process chunk using mapPartitions
                        processed_rdd = chunk_df.rdd.mapPartitions(process_partition)
                        processed_chunk_df = spark.createDataFrame(
                            processed_rdd, schema=output_schema
                        )
                        processed_chunk_df.cache()

                        chunk_count = processed_chunk_df.count()
                        file_processed_count += chunk_count
                        total_processed += chunk_count

                        # Write this chunk to temporary storage
                        chunk_output_path = (
                            f"{temp_chunk_dir}/chunk_{chunk_idx:05d}.parquet"
                        )
                        processed_chunk_df.write.mode("overwrite").parquet(
                            chunk_output_path
                        )

                        print(
                            f"    ✓ Chunk {chunk_idx + 1}/{num_chunks} saved ({chunk_count:,} records)"
                        )

                        # Collect statistics for this chunk
                        business_info_rows = processed_chunk_df.select(
                            "business_info"
                        ).collect()

                        for row in business_info_rows:
                            try:
                                info = json.loads(row.business_info)
                                status = info.get("fetch_status", "success")
                                if status == "success":
                                    file_chunk_stats["success"] += 1
                                    total_error_stats["success"] += 1
                                elif status in total_error_stats:
                                    file_chunk_stats["errors"] += 1
                                    total_error_stats[status] += 1
                                else:
                                    file_chunk_stats["errors"] += 1
                                    total_error_stats["other_errors"] += 1
                            except Exception:
                                file_chunk_stats["errors"] += 1
                                total_error_stats["other_errors"] += 1

                        # Unpersist to free memory
                        processed_chunk_df.unpersist()
                        gc.collect()

                    except Exception as chunk_e:
                        print(
                            f"    ✗ Error processing chunk {chunk_idx + 1}: {str(chunk_e)}"
                        )
                        continue

                # After all chunks are processed, merge them into final output
                print(f"  {'=' * 55}")
                print(f"  Merging {num_chunks} chunks into final output...")

                try:
                    # Read all chunks and write as single parquet file
                    merged_df = spark.read.parquet(f"{temp_chunk_dir}/chunk_*.parquet")
                    final_output_path = f"{OUTPUT_DIR}/{file_name}"
                    merged_df.write.mode("overwrite").parquet(final_output_path)

                    print(
                        f"  ✓ File complete: {file_processed_count:,} records processed"
                    )
                    print(
                        f"  Status: {file_chunk_stats['success']:,} success, {file_chunk_stats['errors']:,} errors\n"
                    )
                except Exception as merge_e:
                    print(f"  ✗ Error merging chunks: {str(merge_e)}\n")

                try:
                    shutil.rmtree(temp_chunk_dir, ignore_errors=True)
                except:
                    pass

            except Exception as e:
                print(f"  ✗ Error processing file: {str(e)}\n")
                continue

        # Print summary statistics
        print(f"\n{'=' * 60}")
        print("Processing Complete!")
        print(f"Total files: {total_files}")
        print(f"Total records processed: {total_processed:,}")
        print("\nError Statistics:")
        print(f"  ✓ Success: {total_error_stats['success']:,}")
        print(f"  ⏱ Timeout: {total_error_stats['timeout']:,}")
        print(f"  🔌 Connection errors: {total_error_stats['connection_error']:,}")
        print(f"  📄 Invalid status: {total_error_stats['invalid_status']:,}")
        print(f"  📭 Empty content: {total_error_stats['empty_content']:,}")
        print(f"  📄 Empty HTML: {total_error_stats['empty_html']:,}")
        print(f"  📭 No response record: {total_error_stats['no_response_record']:,}")
        print(f"  ⚠ WARC parse errors: {total_error_stats['warc_parse_error']:,}")
        print(f"  ⚠ Invalid record: {total_error_stats['invalid_record']:,}")
        print(f"  ⚠ Processing errors: {total_error_stats['processing_error']:,}")
        print(f"  ⚠ Other errors: {total_error_stats['other_errors']:,}")

        success_rate = (
            (total_error_stats["success"] / total_processed * 100)
            if total_processed > 0
            else 0
        )
        print(f"\nSuccess rate: {success_rate:.1f}%")
        print(f"{'=' * 60}\n")

        logger.info(
            f"Task completed. Total files: {total_files}, Total records: {total_processed:,}, Success rate: {success_rate:.1f}%"
        )

        return {"input_dir": str(OUTPUT_DIR)}

    @task
    def create_cc_table(input_dir):
        """Merge all processed parquet files into a single output file."""
        logger.info(f"input_dir: {input_dir} ")

        input_dir = f"{input_dir}/*.parquet"
        output_file = "/opt/shared-data/cc/cc_merged.parquet"

        df = spark.read.parquet(input_dir)
        df.write.mode("overwrite").parquet(output_file)

        logger.info(f"✓ Merged {df.count()} rows into {output_file}")
        return output_file

    @task
    def load_table(parquet_path, table_name):
        logger.info(f"parquet_path: {parquet_path} and table :{table_name} ")

        df = spark.read.parquet(parquet_path)

        df.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option(
            "dbtable", table_name
        ).option("user", POSTGRES_PROPERTIES["user"]).option(
            "password", POSTGRES_PROPERTIES["password"]
        ).option("driver", POSTGRES_PROPERTIES["driver"]).mode("overwrite").save()

        return f"{table_name} loaded"

    @task
    def cleanup():
        """Clean up resources after all tasks are complete."""
        spark.stop()
        logger.info("✅ Spark session stopped successfully")

    # Task chaining using TaskFlow API
    cc_index_urls_list = get_cc_index_urls()

    processed_parquet = process_with_spark_distributed()
    merged_parquet = create_cc_table(input_dir=processed_parquet["input_dir"])
    cc_table = load_table(parquet_path=merged_parquet, table_name="cc_table")

    # Set task dependencies
    (
        cc_index_urls_list
        >> process_files_with_duckdb(cc_index_urls_list)
        >> split_parquet_files()
        >> processed_parquet
        >> merged_parquet
        >> cc_table
        >> cleanup()
    )


dag_instance = cc_business_info_extraction_dag()

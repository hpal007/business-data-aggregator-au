from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import requests
import json
import re
import os
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pathlib import Path

from business_extract import process_partition


BASE_DIR = "/opt/shared-data/cc/"
OUTPUT_DIR = Path(os.path.join(BASE_DIR, "cc_processed_data"))
INPUT_DIR = Path(os.path.join(BASE_DIR, "cc_split_data"))

POSTGRES_JDBC_URL = "jdbc:postgresql://target_postgres:5432/target_db"
POSTGRES_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_pass",
    "driver": "org.postgresql.Driver",
}

# === Configuration ===
BATCH_SIZE = 10  # Process records at a time, set to None for all records


spark = spark = (
    SparkSession.builder.appName("CCBusinessInfoExtraction")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.submit.pyFiles", "/opt/airflow/dags/business_extract.py")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)


@dag(
    dag_id="AU_CC_DATA_WARC_DAG",
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
    def process_with_spark_distributed() -> dict:
        """
        Process all parquet files using Spark distributed processing with mapPartitions.
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        parquet_files = list(Path(INPUT_DIR).glob("*.parquet"))
        total_files = len(parquet_files)

        print(f"\n{'=' * 60}")
        print(f"Found {total_files} parquet files to process")
        print("Processing mode: Spark Distributed (mapPartitions)")
        print(f"Batch size: {BATCH_SIZE if BATCH_SIZE else 'All records'}")
        print(f"{'=' * 60}\n")

        total_processed = 0
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
                StructField("business_info", StringType(), True),
            ]
        )

        for idx, parquet_file in enumerate(parquet_files, 1):
            file_name = parquet_file.name
            print(f"[{idx}/{total_files}] Processing: {file_name}")

            try:
                # Read parquet file
                df = spark.read.parquet(str(parquet_file))
                total_records = df.count()
                print(f"  Total records: {total_records:,}")

                # Apply batch size limit if specified
                if BATCH_SIZE:
                    df = df.limit(BATCH_SIZE)
                    print(f"  Processing: {BATCH_SIZE:,} records")

                # Process using mapPartitions - this distributes work across Spark executors
                print(
                    "  Extracting business information with Spark distributed processing..."
                )

                processed_rdd = df.rdd.mapPartitions(process_partition)
                processed_df = spark.createDataFrame(
                    processed_rdd, schema=output_schema
                )

                # Cache the processed dataframe
                processed_df.cache()

                # Write output
                output_path = f"{OUTPUT_DIR}/{file_name}"
                processed_df.write.mode("overwrite").parquet(output_path)

                processed_count = processed_df.count()
                total_processed += processed_count

                # Collect and count error statistics
                print("  Collecting statistics...")
                business_info_rows = processed_df.select("business_info").collect()
                for row in business_info_rows:
                    try:
                        info = json.loads(row.business_info)
                        status = info.get("fetch_status", "success")
                        if status == "success":
                            error_stats["success"] += 1
                        elif status in error_stats:
                            error_stats[status] += 1
                        else:
                            error_stats["other_errors"] += 1
                    except Exception:
                        error_stats["other_errors"] += 1

                # Unpersist cached dataframe
                processed_df.unpersist()

                print(f"  ✓ Saved {processed_count:,} records")
                print(
                    f"  Status: {error_stats['success']} success, "
                    f"{processed_count - error_stats['success']} errors\n"
                )

            except Exception as e:
                print(f"  ✗ Error processing file: {str(e)}\n")
                continue

        # Print summary statistics
        print(f"\n{'=' * 60}")
        print("Processing Complete!")
        print(f"Total files: {total_files}")
        print(f"Total records processed: {total_processed:,}")
        print("\nError Statistics:")
        print(f"  ✓ Success: {error_stats['success']:,}")
        print(f"  ⏱ Timeout: {error_stats['timeout']:,}")
        print(f"  🔌 Connection errors: {error_stats['connection_error']:,}")
        print(f"  📄 Invalid status: {error_stats['invalid_status']:,}")
        print(f"  📭 Empty content: {error_stats['empty_content']:,}")
        print(f"  📄 Empty HTML: {error_stats['empty_html']:,}")
        print(f"  📭 No response record: {error_stats['no_response_record']:,}")
        print(f"  ⚠ WARC parse errors: {error_stats['warc_parse_error']:,}")
        print(f"  ⚠ Invalid record: {error_stats['invalid_record']:,}")
        print(f"  ⚠ Processing errors: {error_stats['processing_error']:,}")
        print(f"  ⚠ Other errors: {error_stats['other_errors']:,}")

        success_rate = (
            (error_stats["success"] / total_processed * 100)
            if total_processed > 0
            else 0
        )
        print(f"\nSuccess rate: {success_rate:.1f}%")
        print(f"{'=' * 60}\n")

        return {"input_dir": str(OUTPUT_DIR)}

    @task
    def create_cc_table(input_dir):
        """Merge all processed parquet files into a single output file."""
        print(f"input_dir: {input_dir} ")

        input_dir = f"{input_dir}/*.parquet"
        output_file = "/opt/shared-data/cc/cc_merged.parquet"

        df = spark.read.parquet(input_dir)
        df.write.mode("overwrite").parquet(output_file)

        print(f"✓ Merged {df.count()} rows into {output_file}")
        return output_file

    @task
    def load_table(parquet_path, table_name):
        print(f"parquet_path: {parquet_path} and table :{table_name} ")

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
        print("✅ Spark session stopped successfully")

    # Task chaining using TaskFlow API
    processed_parquet = process_with_spark_distributed()
    merged_parquet = create_cc_table(input_dir=processed_parquet["input_dir"])
    cc_table = load_table(parquet_path=merged_parquet, table_name="cc_table")

    # If you want explicit dependencies:
    processed_parquet >> merged_parquet >> cc_table >> cleanup()


dag_instance = cc_business_info_extraction_dag()

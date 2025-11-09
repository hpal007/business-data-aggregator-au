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
        .config(
            "spark.sql.shuffle.partitions", "64"
        )  # CHANGED: Increased from 56 to 64
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
        .config(
            "spark.sql.files.maxPartitionBytes", "128mb"
        )  # CHANGED: Decreased from 256mb to 128mb for finer partitioning
        .config(
            "spark.sql.files.minPartitionNum", "16"
        )  # CHANGED: Increased from 8 to 16
        .config("spark.default.parallelism", "64")  # CHANGED: Increased from 56 to 64
        .config("spark.task.maxFailures", "4")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeoutInterval", "600s")
        .config("spark.local.dir", "/tmp/spark-temp")  # NEW: Prevent memory overflow
        .getOrCreate()
    )

    return spark


@dag(
    dag_id="PROCESS_Parquet_AND_push_to_DB",
    description="Extract business information from Common Crawl files using Spark distributed processing",
    schedule=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["au", "cc", "abr"],
    max_active_tasks=3,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)
def tester_dag_process_and_push_to_db():
    @task
    def create_cc_table(input_dir):
        """Merge all processed parquet files into a single output file"""
        spark = get_spark_session("Merge")

        try:
            logger.info(f"Merging files from: {input_dir}")
            input_path = f"{input_dir}/*.parquet"
            output_file = "/opt/shared-data/cc/temp/cc_merged.parquet"

            df = spark.read.parquet(input_path)
            df.coalesce(1).write.mode("overwrite").parquet(output_file)

            logger.info(f"Merged {df.count()} rows into {output_file}")
            return output_file
        finally:
            spark.stop()

    @task
    def load_cc_table(parquet_path, table_name):
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

    @task
    def load_abr_table(parquet_path, table_name):
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


    abr_path = os.path.join('/opt/shared-data/abr/', "abr_tbl")
    merged_parquet = create_cc_table(OUTPUT_DIR)
    cc_table = load_cc_table(parquet_path=merged_parquet, table_name="cc_table")
    abr_table = load_abr_table(parquet_path=abr_path, table_name="abr_tbl")


    merged_parquet >> cc_table  >> abr_table


dag_instance = tester_dag_process_and_push_to_db()

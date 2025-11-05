import requests
import os
import zipfile
from airflow.decorators import dag, task
from datetime import datetime
import time

from pyspark.sql.types import LongType
from pyspark.sql.functions import to_date, year, month, coalesce, lit

from utils.abr_spark import (
    get_spark_session,
    ABR_SCHEMA,
    stop_spark,
    parse_abn_xml_iterative,
)

BASE_DIR = "/opt/shared-data"
DATA_DIR = os.path.join(BASE_DIR, "abr_xml_data")
PARQUET_DIR = os.path.join(BASE_DIR, "parquet_output")

# https://data.gov.au/data/dataset/activity/abn-bulk-extract
URLS = [
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
]


@dag(
    dag_id="ABR_DATA_DAG",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["abr"],
)
def download_and_unzip_dag():
    @task
    def download_files():
        os.makedirs(DATA_DIR, exist_ok=True)
        file_paths = []
        for url in URLS:
            filename = os.path.join(DATA_DIR, url.split("/")[-1])
            print(f"Downloading {url} → {filename}")
            response = requests.get(url)
            response.raise_for_status()
            with open(filename, "wb") as f:
                f.write(response.content)
            file_paths.append(filename)
        return file_paths

    @task
    def unzip_and_delete(file_paths: list):
        for file_path in file_paths:
            print(f"Unzipping {file_path}")
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(DATA_DIR)
            print(f"Deleting {file_path}")
            os.remove(file_path)
        print(f"All files extracted and cleaned up in {DATA_DIR}")

    @task
    def process_xml_to_parquet():
        batch_size = 100000
        # Ensure output dir exists
        os.makedirs(PARQUET_DIR, exist_ok=True)

        # Get Spark session
        spark = get_spark_session("ABN_XML_Processor")

        xml_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".xml")]
        if not xml_files:
            raise FileNotFoundError(f"No XML files found in {DATA_DIR}")

        for xml_file in xml_files:
            file_path = os.path.join(DATA_DIR, xml_file)
            print(f"Processing: {file_path}")
            start_time = time.time()

            df = spark.createDataFrame([], schema=ABR_SCHEMA)
            batch_num = 0
            print("Processing batch with batch_size of", batch_size)
            for batch in parse_abn_xml_iterative(file_path, batch_size=batch_size):
                batch_df = spark.createDataFrame(batch, schema=ABR_SCHEMA)
                df = df.union(batch_df)
                batch_num += 1

                print(f"{batch_num} ->", end=" ")

            count = df.count()
            print(f"Total records in {xml_file}: {count}")
            parquet_path = os.path.join(
                PARQUET_DIR, f"{xml_file.replace('.xml', '.parquet')}"
            )
            df.write.mode("overwrite").parquet(parquet_path)

            print(f"Saved → {parquet_path}")
            print(f"Time taken: {time.time() - start_time:.2f}s")

        print("✅ All XML files processed successfully")

    @task
    def process_parquet_to_table():
        # Get Spark session
        spark = get_spark_session("ABN_Parquet_Processor")

        # Read parquet files
        df = spark.read.schema(ABR_SCHEMA).parquet(f"{PARQUET_DIR}/*.parquet")

        # Process the data using the imported function
        active_df = (
            df.filter(df["abn_status"] == "ACT")
            .withColumn("abn_start_date", to_date("abn_start_date", "yyyyMMdd"))
            .withColumn("year", year("abn_start_date"))
            .withColumn("month", month("abn_start_date"))
            .withColumn("abn", df["abn"].cast(LongType()))
            .withColumn("entity_state", coalesce("entity_state", lit("unknown")))
            .orderBy("abn", "entity_name", "entity_state")
        )
        # Write the processed data
        output_path = os.path.join(BASE_DIR, "abr_tbl")
        active_df.write.partitionBy("year", "entity_state").mode("overwrite").parquet(
            output_path
        )

        print("✅ Parquet files processed and partitioned successfully")

    @task
    def cleanup():
        """Clean up resources after all tasks are complete."""
        stop_spark()
        print("✅ Spark session stopped successfully")

    downloaded_files = download_files()
    unzip_task = unzip_and_delete(downloaded_files)
    process_xml_to_parquet = process_xml_to_parquet()
    create_abr_table = process_parquet_to_table()
    cleanup_task = cleanup()

    # Set task dependencies
    unzip_task >> process_xml_to_parquet >> create_abr_table >> cleanup_task


dag = download_and_unzip_dag()

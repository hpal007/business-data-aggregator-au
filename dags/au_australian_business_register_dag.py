import requests
from pyspark.sql import SparkSession
import os
import zipfile

from airflow.decorators import dag, task
from datetime import datetime
import time
import xml.etree.ElementTree as ET
from utils import POSTGRES_JDBC_URL, POSTGRES_PROPERTIES, URLS, ABR_SCHEMA, logger

from pyspark.sql.types import LongType
from pyspark.sql.functions import to_date, year, month, coalesce, lit


BASE_DIR = "/opt/shared-data/abr/"
DATA_DIR = os.path.join(BASE_DIR, "abr_xml_data")
PARQUET_DIR = os.path.join(BASE_DIR, "parquet_output")


spark = spark = (
    SparkSession.builder.appName("CCBusinessInfoExtraction")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)


def safe_get(elem, path, attr=None):
    """Get text or attribute from XML element, return None if not found."""
    found = elem.find(path) if path else elem
    if found is None:
        return None
    return found.get(attr) if attr else found.text


def parse_abn_xml_iterative(file_path, batch_size=50000):
    """Parse large XML file iteratively in batches to avoid memory issues."""
    context = ET.iterparse(file_path, events=("end",))
    batch = []
    for event, elem in context:
        if elem.tag == "ABR":
            record = {
                "abn": safe_get(elem, "ABN"),
                "abn_status": safe_get(elem, "ABN", "status"),
                "abn_start_date": safe_get(elem, "ABN", "ABNStatusFromDate"),
                "entity_type": safe_get(elem, "EntityType/EntityTypeInd"),
                "entity_type_text": safe_get(elem, "EntityType/EntityTypeText"),
                "entity_name": safe_get(
                    elem, "MainEntity/NonIndividualName/NonIndividualNameText"
                ),
                "entity_state": safe_get(
                    elem, "MainEntity/BusinessAddress/AddressDetails/State"
                ),
                "entity_postcode": safe_get(
                    elem, "MainEntity/BusinessAddress/AddressDetails/Postcode"
                ),
            }
            batch.append(record)
            elem.clear()
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


@dag(
    dag_id="AU_Australian_Business_Register_DAG",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["au", "abr"],
)
def download_and_unzip_dag():
    @task
    def download_files():
        os.makedirs(DATA_DIR, exist_ok=True)
        file_paths = []
        for url in URLS:
            filename = os.path.join(DATA_DIR, url.split("/")[-1])
            logger.info(f"Downloading {url} → {filename}")
            response = requests.get(url)
            response.raise_for_status()
            with open(filename, "wb") as f:
                f.write(response.content)
            file_paths.append(filename)
        return file_paths

    @task
    def unzip_and_delete(file_paths: list):
        for file_path in file_paths:
            logger.info(f"Unzipping {file_path}")
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(DATA_DIR)
            logger.info(f"Deleting {file_path}")
            os.remove(file_path)
        logger.info(f"All files extracted and cleaned up in {DATA_DIR}")

    @task
    def process_xml_to_parquet():
        batch_size = 100000
        # Ensure output dir exists
        os.makedirs(PARQUET_DIR, exist_ok=True)

        xml_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".xml")]
        if not xml_files:
            raise FileNotFoundError(f"No XML files found in {DATA_DIR}")

        for xml_file in xml_files:
            file_path = os.path.join(DATA_DIR, xml_file)
            logger.info(f"Processing: {file_path}")
            start_time = time.time()

            df = spark.createDataFrame([], schema=ABR_SCHEMA)
            batch_num = 0
            logger.info("Processing batch with batch_size of", batch_size)
            for batch in parse_abn_xml_iterative(file_path, batch_size=batch_size):
                batch_df = spark.createDataFrame(batch, schema=ABR_SCHEMA)
                df = df.union(batch_df)
                batch_num += 1

                logger.info(f"{batch_num} ->", end=" ")

            count = df.count()
            logger.info(f"Total records in {xml_file}: {count}")
            parquet_path = os.path.join(
                PARQUET_DIR, f"{xml_file.replace('.xml', '.parquet')}"
            )
            df.write.mode("overwrite").parquet(parquet_path)

            logger.info(f"Saved → {parquet_path}")
            logger.info(f"Time taken: {time.time() - start_time:.2f}s")

        logger.info("✅ All XML files processed successfully")

    @task
    def process_parquet_to_table():
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

        logger.info("✅ Parquet files processed and partitioned successfully")

        return output_path

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

    downloaded_files = download_files()
    unzip_task = unzip_and_delete(downloaded_files)
    process_xml_to_parquet = process_xml_to_parquet()
    create_abr_table = process_parquet_to_table()
    abr_tbl = load_table(parquet_path=create_abr_table, table_name="abr_table")

    # Set task dependencies
    unzip_task >> process_xml_to_parquet >> create_abr_table >> abr_tbl >> cleanup()


dag = download_and_unzip_dag()

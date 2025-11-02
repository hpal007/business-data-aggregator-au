from airflow.decorators import dag, task
from datetime import datetime
import os
import time


from utils.abr_spark import (
    get_spark_session,
    ABR_SCHEMA,
    stop_spark,
    process_active_records,
    parse_abn_xml_iterative,
)

BASE_DIR = "/opt/shared-data"
DATA_DIR = os.path.join(BASE_DIR, "abr_xml_data")
PARQUET_DIR = os.path.join(BASE_DIR, "parquet_output")


@dag(
    dag_id="process_abn_xml_files",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["abr", "spark", "xml", "parquet"],
)
def process_abn_xml_files_dag():
    @task
    def process_xml_files():
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
    def process_parquet_files():
        # Get Spark session
        spark = get_spark_session("ABN_Parquet_Processor")

        # Read parquet files
        df = spark.read.schema(ABR_SCHEMA).parquet(f"{PARQUET_DIR}/*.parquet")

        # Process the data using the imported function
        active_df = process_active_records(df)

        # Write the processed data
        output_path = os.path.join(BASE_DIR, "abr_tbl")
        active_df.write.partitionBy("year", "Entity_State").mode("overwrite").parquet(
            output_path
        )

        print("✅ Parquet files processed and partitioned successfully")

    @task
    def cleanup():
        """Clean up resources after all tasks are complete."""
        stop_spark()
        print("✅ Spark session stopped successfully")

    # Set up task dependencies
    process_xml = process_xml_files()
    process_parquet = process_parquet_files()
    cleanup_task = cleanup()

    # Set the task dependencies
    process_xml >> process_parquet >> cleanup_task


dag = process_abn_xml_files_dag()

import os
from airflow.decorators import dag, task
from datetime import datetime
from pyspark.sql import SparkSession

BASE_DIR = "/opt/shared-data/"
POSTGRES_JDBC_URL = "jdbc:postgresql://targetpostgres:5432/targetdb"
POSTGRES_PROPERTIES = {
    "user": "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver",
}


@dag(
    dag_id="AU_TABLE_DAG",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["au"],
)
def parquet_to_postgres_dag():
    @task
    def load_table(parquet_path, table_name):
        spark = SparkSession.builder.config(
            "spark.jars.packages", "org.postgresql:postgresql:42.6.0"
        ).getOrCreate()
        df = spark.read.parquet(parquet_path)
        # Overwrite mode will DROP and recreate the table in Postgres
        df.write.format("jdbc").option("url", POSTGRES_JDBC_URL).option(
            "dbtable", table_name
        ).option("user", POSTGRES_PROPERTIES["user"]).option(
            "password", POSTGRES_PROPERTIES["password"]
        ).option("driver", POSTGRES_PROPERTIES["driver"]).mode("overwrite").save()
        return f"{table_name} loaded"

    abr_parquet_path = os.path.join(f"{BASE_DIR}/abr/", "abr_tbl")  # Or BASE_DIR/abr/
    cc_parquet_path = os.path.join(BASE_DIR, "cc")  # Or BASE_DIR/cc/

    load_table(abr_parquet_path, "abr_table") >> load_table(cc_parquet_path, "cc_table")


dag_obj = parquet_to_postgres_dag()

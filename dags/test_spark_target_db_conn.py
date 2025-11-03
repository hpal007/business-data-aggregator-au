import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="test_spark_target_db_connection",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["abr", "test", "spark", "postgres"],
) as dag:
    spark_to_db = SparkSubmitOperator(
        task_id="spark_write_to_target_db",
        conn_id="spark_default",
        application="/opt/airflow/dags/utils/test_spark_target_db_job.py",
        # conf={"spark.master": "spark://spark-master:7077"},
        jars="/opt/spark/jars/postgresql-42.6.0.jar",  # ensure JDBC jar path matches your mount
    )

    spark_to_db

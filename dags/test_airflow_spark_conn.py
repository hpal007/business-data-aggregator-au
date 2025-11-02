from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

with DAG(
    dag_id="test_airflow_spark_connection",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["abr", "test", "spark"],
) as dag:
    # Simple Spark job to verify cluster connectivity

    spark_test = SparkSubmitOperator(
        task_id="spark_test_job",
        application="/opt/airflow/dags/utils/test_airflow_spark_job.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            "spark.driver.host": "airflow-worker",
            "spark.driver.bindAddress": "0.0.0.0",
        },
        verbose=True,
    )
    spark_test

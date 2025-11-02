from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkToTargetPostgres").getOrCreate()

# Create sample data
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, ["id", "name"])

# Write to target_postgres DB
pg_url = "jdbc:postgresql://target_postgres:5432/target_db"
pg_props = {
    "user": "spark_user",
    "password": "spark_pass",
    "driver": "org.postgresql.Driver",
}

df.write.jdbc(url=pg_url, table="spark_test_table", mode="overwrite", properties=pg_props)

print("✅ Data written successfully to target_postgres.spark_test_table")

spark.stop()
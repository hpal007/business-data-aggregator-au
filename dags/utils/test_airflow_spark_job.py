from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirflowSparkTest").getOrCreate()

print("✅ Spark session created successfully!")
df = spark.range(0, 10).toDF("number")
print("✅ Sample data:")
df.show()

spark.stop()

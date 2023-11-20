from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("QueryHive").getOrCreate()

QUERY = '[QUERY]'

spark.sql(QUERY)

# Stop the Spark session
spark.stop()
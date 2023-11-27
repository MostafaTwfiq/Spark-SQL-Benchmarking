from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DataDeletion").getOrCreate()

# Use the spark.catalog.listTables method to get a list of tables in the database
tables = spark.catalog.listTables("iceberg_temp")

# Drop benchmark database
spark.sql("DROP DATABASE IF EXISTS [DATABASE_NAME]")

# Stop the Spark session
spark.stop()

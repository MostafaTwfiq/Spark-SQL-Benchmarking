from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DataDeletionIceberg").getOrCreate()

# Use the spark.catalog.listTables method to get a list of tables in the database
tables = spark.catalog.listTables("iceberg_temp")

# Iterate over the tables and drop each one
for table in tables:
    table_name = table.name
    spark.sql(f"DROP TABLE IF EXISTS iceberg_temp.{table_name}")

# Stop the Spark session
spark.stop()

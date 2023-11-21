from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("DataDeletionHive").getOrCreate()

# Use the spark.catalog.listTables method to get a list of tables in the database
tables = spark.catalog.listTables("iceberg_temp")

# Iterate over the tables and drop each one
for table in tables:
    if str(table.name).endswith('_hive'):
        spark.sql(f"DROP TABLE IF EXISTS iceberg_temp.{str(table.name)}")

# Stop the Spark session
spark.stop()

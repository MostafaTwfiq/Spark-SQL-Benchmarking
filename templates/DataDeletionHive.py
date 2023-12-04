from pyspark.sql import SparkSession
import sys

# Specify the Hive server host
hive_host = sys.argv[1]      # 192.168.168.91
hive_port = sys.argv[2]      # 8020
hive_database = sys.argv[3]  # iceberg_temp 

# Create a Spark session with Hive support and host information
spark = SparkSession.builder.appName("DataDeletionHive") \
    .config("spark.sql.hive.metastore.uris", 
            f"thrift://{hive_host}:{hive_port}") \
    .enableHiveSupport() \
    .getOrCreate()

# Drop benchmark database
spark.sql(f"DROP DATABASE IF EXISTS {hive_database} CASCADE")

# Stop the Spark session
spark.stop()

from pyspark.sql import SparkSession
import sys

# Specify the Iceberg server host
iceberg_host =  sys.argv[1]     # 192.168.168.91
iceberg_port = sys.argv[2]      # 8020
iceberg_database = sys.argv[3]  # iceberg_temp
iceberg_warehouse = sys.argv[4]  

# Create a Spark session with Iceberg support and host information
spark = SparkSession.builder \
    .appName("TableCreationIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", 
            f"hadoop://{iceberg_host}:{iceberg_port}/{iceberg_warehouse}") \
    .getOrCreate()

# Create a Spark session

# Execute Hive SQL command to create the database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {iceberg_database}")

# Stop the Spark session
spark.stop()
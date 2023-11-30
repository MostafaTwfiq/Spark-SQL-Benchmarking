from pyspark.sql import SparkSession
import sys

# Specify the Iceberg server host
iceberg_host =  sys.argv[1]     # 192.168.168.91
iceberg_port = sys.argv[2]      # 8020
iceberg_database = sys.argv[3]  # iceberg_temp
iceberg_warehouse = sys.argv[4]

# Create a Spark session with Iceberg support and host information
spark = SparkSession.builder \
    .appName("QueryIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", 
            f"hadoop://{iceberg_host}:{iceberg_port}/{iceberg_warehouse}/{iceberg_database}.db") \
    .getOrCreate()


# Set the Iceberg catalog as the current catalog
spark.sql(f"USE {iceberg_database}")

QUERY = '''{{QUERY}}'''

spark.sql(QUERY)

# Stop the Spark session
spark.stop()
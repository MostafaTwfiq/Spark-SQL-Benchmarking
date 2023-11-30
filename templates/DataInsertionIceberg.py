from pyspark.sql import SparkSession
from schemas import schemas
import sys
import os

# Specify the Iceberg server host
iceberg_host =  sys.argv[1]     # 192.168.168.91
iceberg_port = sys.argv[2]      # 8020
iceberg_database = sys.argv[3]  # iceberg_temp
iceberg_warehouse = sys.argv[4]  

# Create a Spark session with Iceberg support and host information
spark = SparkSession.builder \
    .appName("DataInsertionIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", 
            f"hadoop://{iceberg_host}:{iceberg_port}/{iceberg_warehouse}") \
    .getOrCreate()

# CHANGE HERE!
GENERATE_INSERT_TABLES_FOLDER = '{{GENERATED_INSERT_TABLES_FOLDER}}'

for table in schemas:
    table_name = table[0]
    file_path = os.path.join(GENERATE_INSERT_TABLES_FOLDER, table[1])
    schema = table[2]
    temp_insert_view_name = f"{table_name}_temp_insert"
    
    df = spark.read.csv(file_path, sep='|', schema=schema)
    df.createOrReplaceTempView(temp_insert_view_name)
    
    spark.sql(f"INSERT INTO TABLE {iceberg_database}.{table_name} SELECT * FROM {temp_insert_view_name}")


# Stop the Spark session
spark.stop()

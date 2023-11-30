from pyspark.sql import SparkSession
from schemas import schemas
import sys
import os

# Specify the Hive server host
hive_host = sys.argv[1]      # 192.168.168.91
hive_port = sys.argv[2]      # 8020
hive_database = sys.argv[3]  # iceberg_temp 

# Create a Spark session with Hive support and host information
spark = SparkSession.builder.appName("DataInsertionHive") \
    .config("spark.sql.hive.metastore.uris", 
            f"thrift://{hive_host}:{hive_port}") \
    .enableHiveSupport() \
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
    
    spark.sql(f"INSERT INTO TABLE {hive_database}.{table_name} SELECT * FROM {temp_insert_view_name}")

# Stop the Spark session
spark.stop()

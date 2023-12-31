from pyspark.sql import SparkSession
from schemas import schemas
import sys
import os

# Specify the Hive server host
hive_host = sys.argv[1]      # 192.168.168.91
hive_port = sys.argv[2]      # 8020
hive_database = sys.argv[3]  # iceberg_temp 

# Create a Spark session with Hive support and host information
spark = SparkSession.builder.appName("TableCreationHive") \
    .config("spark.sql.hive.metastore.uris", 
            f"thrift://{hive_host}:{hive_port}") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a Spark session
# spark = SparkSession.builder.appName("TableCreationHive").getOrCreate()


# CHANGE HERE!
PARTITIONING_DICT = {{PARTITIONING_DICT}}
GENERATE_TABLES_FOLDER = '{{GENERATED_TABLES_FOLDER}}'

# Creating DataFrames
def schema_to_table(schema, file_path, table_name, partitioning=''):
    # df = spark.createDataFrame([], schema=schema)
    df = spark.read.csv(file_path, sep='|', schema=schema)
    writer = df.write.format('parquet')
    
    if partitioning != '':
        writer = df.partitionBy(partitioning)
    
    writer.mode("overwrite") \
    .saveAsTable(f"{hive_database}.{table_name}")

for table in schemas:
    table_name = table[0]
    file_path = os.path.join(GENERATE_TABLES_FOLDER, table[1])
    schema = table[2]
    if table_name in PARTITIONING_DICT.keys():    
        schema_to_table(schema, file_path, table_name, PARTITIONING_DICT[table_name])
    else:
        schema_to_table(schema, file_path, table_name)

# Stop the Spark session
spark.stop()

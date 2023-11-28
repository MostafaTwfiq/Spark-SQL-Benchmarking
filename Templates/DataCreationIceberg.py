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
    .appName("DataCreationIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", 
            f"hadoop://{iceberg_host}:{iceberg_port}/{iceberg_warehouse}") \
    .getOrCreate()

# Create a Spark session
# spark = SparkSession.builder.appName("DataCreationIceberg").getOrCreate()

# CHANGE HERE!
DELETE_MODE = '{{DELETE_MODE}}'
UPDATE_MODE= '{{UPDATE_MODE}}'
MERGE_MODE = '{{MERGE_MODE}}'
PARTITIONING_DICT = {{PARTITIONING_DICT}}
GENERATE_TABLES_FOLDER = '{{GENERATED_TABLES_FOLDER}}'

# Creating DataFrames
def schema_to_table(schema, file_path, table_name, partitioning=''):
    # global delete_mode, update_mode, merge_mode, PARTITIONING
    # df = spark.createDataFrame([], schema=schema)
    df = spark.read.csv(file_path, sep='|', schema=schema)
    df.write.format('iceberg')
    
    if partitioning != '':
        df.partitionBy(partitioning)
    
    df.mode("overwrite") \
    .option("write.delete.mode", DELETE_MODE) \
    .option("write.update.mode", UPDATE_MODE) \
    .option("write.merge.mode", MERGE_MODE) \
    .saveAsTable(f"{iceberg_database}.{table_name}")
           

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
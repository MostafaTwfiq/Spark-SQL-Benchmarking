from pyspark.sql import SparkSession
from schemas import schemas
import sys

# Specify the Hive server host
hive_host = sys.argv[1]      # 192.168.168.91
hive_port = sys.argv[2]      # 8020
hive_database = sys.argv[3]  # iceberg_temp 

# Create a Spark session with Hive support and host information
spark = SparkSession.builder.appName("DataCreationHive") \
    .config("spark.sql.hive.metastore.uris", 
            f"thrift://{hive_host}:{hive_port}") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a Spark session
# spark = SparkSession.builder.appName("DataCreationHive").getOrCreate()


# CHANGE HERE!
partitioning_dict = {{partitioning_dict}}

# Creating DataFrames
def schema_to_table(schema, file_path, table_name, partitioning=''):
    # df = spark.createDataFrame([], schema=schema)
    df = spark.read.csv(file_path, sep='|', schema=schema)
    df.write.format('parquet')
    
    if partitioning != '':
        df.partitionBy(partitioning)
    
    df.mode("overwrite") \
    .saveAsTable(f"iceberg_temp.{table_name}")

for table in schemas:
    table_name = table[0] + "_hive"
    file_path = table[1]
    schema = table[2]
    if table_name in partitioning_dict.keys():    
        schema_to_table(schema, file_path, table_name, partitioning_dict[table_name])
    else:
        schema_to_table(schema, file_path, table_name)

# Stop the Spark session
spark.stop()

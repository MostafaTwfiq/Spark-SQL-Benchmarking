from pyspark.sql import SparkSession
from schemas import schemas

# Create a Spark session
spark = SparkSession.builder.appName("DataCreationHive").getOrCreate()

# CHANGE HERE!
PARTITIONING = ''
partitioning_dict = {}

# Creating DataFrames
def schema_to_table(schema, file_path, table_name, partitioning=''):
    # df = spark.createDataFrame([], schema=schema)
    df = spark.read.csv(file_path, sep='|', schema=schema)
    df.write.format('hive')
    
    if partitioning != '':
        df.partitionBy(partitioning)
    
    df.mode("overwrite") \
    .format('parquet') \
    .saveAsTable(f"iceberg_temp.{table_name}")

for table in schemas:
    table_name = table[0]
    file_path = table[1]
    schema = table[2]
    if table_name in partitioning_dict.keys():    
        schema_to_table(schema, file_path, table_name, partitioning_dict[table_name])
    else:
        schema_to_table(schema, file_path, table_name)

# Stop the Spark session
spark.stop()

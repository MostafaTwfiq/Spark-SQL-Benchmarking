from pyspark.sql import SparkSession
from schemas import schemas

# Create a Spark session
spark = SparkSession.builder.appName("DataCreationIceberg").getOrCreate()

# CHANGE HERE!
DELETE_MODE = '[DELETE_MODE]'
UPDATE_MODE= '[UPDATE_MODE]'
MERGE_MODE = '[MERGE_MODE]'
partitioning_dict = '[partitioning_dict]'

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
from pyspark.sql import SparkSession
from schemas import schemas, PRIMARY_KEY_DICT
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
NUM_TEST_ROWS = {{NUM_TEST_ROWS}}

for table in schemas:
    table_name = table[0]
    dml_temp_view = f"{table_name}_temp_view"
    
    # Read random (NUM_TEST_ROWS) from table
    df = spark.read.table(f"{hive_database}.{table_name}").limit(NUM_TEST_ROWS)
    
    # Create TempView
    df.createOrReplaceTempView(dml_temp_view)

    # Get the primary keys of these rows
    table_pk = PRIMARY_KEY_DICT[table_name]
    # primary_key_values = df.select(*table_pk).collect()

    # Construct the DELETE query
    delete_query = f"""
        DELETE FROM {hive_database}.{table_name}
        WHERE ({[", ".join(table_pk)]}) IN (
            SELECT {[", ".join(table_pk)]}
            FROM {dml_temp_view}
        );
    """

    # Execute the DELETE statement
    spark.sql(delete_query)

    # Construct the INSERT query
    insert_query = f"""
        INSERT INTO {hive_database}.{table_name}
        SELECT * FROM {dml_temp_view};
    """
    
    # Insert the rows from the TempView back to the table
    spark.sql(insert_query)
    
# Stop the Spark session
spark.stop()

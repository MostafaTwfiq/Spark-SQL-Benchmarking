from pyspark.sql import SparkSession
from schemas import schemas, PRIMARY_KEY_DICT
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
NUM_TEST_ROWS = {{NUM_TEST_ROWS}}

for table in schemas:
    table_name = table[0]
    dml_temp_view = f"{table_name}_temp_view"
    
    # Read random (NUM_TEST_ROWS) from table
    df = spark.read.table(f"{iceberg_database}.{table_name}").limit(NUM_TEST_ROWS)
    
    # Create TempView
    df.createOrReplaceTempView(dml_temp_view)

    # Get the primary keys of these rows
    table_pk = PRIMARY_KEY_DICT[table_name]
    # primary_key_values = df.select(*table_pk).collect()

    # Construct the DELETE query
    delete_query = f"""
        DELETE FROM {iceberg_database}.{table_name}
        WHERE ({[", ".join(table_pk)]}) IN (
            SELECT {[", ".join(table_pk)]}
            FROM {dml_temp_view}
        );
    """

    # Execute the DELETE statement
    spark.sql(delete_query)

    # Construct the INSERT query
    insert_query = f"""
        INSERT INTO {iceberg_database}.{table_name}
        SELECT * FROM {dml_temp_view};
    """
    
    # Insert the rows from the TempView back to the table
    spark.sql(insert_query)

# Stop the Spark session
spark.stop()

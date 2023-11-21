from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("DataCreationIceberg").getOrCreate()

# CHANGE HERE!
DELETE_MODE = 'merge-on-read'
UPDATE_MODE= 'merge-on-read'
MERGE_MODE = 'merge-on-read'
# PARTITIONING = ''

# Schema Definition
schemas = [
    ('lineitem_ice', StructType([
        StructField("L_ORDERKEY", LongType()),
        StructField("L_PARTKEY", LongType()),
        StructField("L_SUPPKEY", LongType()),
        StructField("L_LINENUMBER", IntegerType()),
        StructField("L_QUANTITY", DoubleType()),
        StructField("L_EXTENDEDPRICE", DoubleType()),
        StructField("L_DISCOUNT", DoubleType()),
        StructField("L_TAX", DoubleType()),
        StructField("L_RETURNFLAG", StringType()),
        StructField("L_LINESTATUS", StringType()),
        StructField("L_SHIPDATE", StringType()),
        StructField("L_COMMITDATE", StringType()),
        StructField("L_RECEIPTDATE", StringType()),
        StructField("L_SHIPINSTRUCT", StringType()),
        StructField("L_SHIPMODE", StringType()),
        StructField("L_COMMENT", StringType())
    ])),
    ('part_ice', StructType([
        StructField("P_PARTKEY", IntegerType()),
        StructField("P_NAME", StringType()),
        StructField("P_MFGR", StringType()),
        StructField("P_BRAND", StringType()),
        StructField("P_TYPE", StringType()),
        StructField("P_SIZE", IntegerType()),
        StructField("P_CONTAINER", StringType()),
        StructField("P_RETAILPRICE", DoubleType()),
        StructField("P_COMMENT", StringType())
    ])),
    ('supplier_ice', StructType([
        StructField("S_SUPPKEY", LongType()),
        StructField("S_NAME", StringType()),
        StructField("S_ADDRESS", StringType()),
        StructField("S_NATIONKEY", IntegerType()),
        StructField("S_PHONE", StringType()),
        StructField("S_ACCTBAL", DoubleType()),
        StructField("S_COMMENT", StringType())
    ])),
    ('partsupp_ice', StructType([
        StructField("PS_PARTKEY", LongType()),
        StructField("PS_SUPPKEY", LongType()),
        StructField("PS_AVAILQTY", IntegerType()),
        StructField("PS_SUPPLYCOST", DoubleType()),
        StructField("PS_COMMENT", StringType())
    ])),
    ('nation_ice', StructType([
        StructField("N_NATIONKEY", IntegerType()),
        StructField("N_NAME", StringType()),
        StructField("N_REGIONKEY", IntegerType()),
        StructField("N_COMMENT", StringType())
    ])),
    ('region_ice', StructType([
        StructField("R_REGIONKEY", IntegerType()),
        StructField("R_NAME", StringType()),
        StructField("R_COMMENT", StringType())
    ])),
    ('customer_ice', StructType([
        StructField("C_CUSTKEY", LongType()),
        StructField("C_NAME", StringType()),
        StructField("C_ADDRESS", StringType()),
        StructField("C_NATIONKEY", IntegerType()),
        StructField("C_PHONE", StringType()),
        StructField("C_ACCTBAL", DoubleType()),
        StructField("C_MKTSEGMENT", StringType()),
        StructField("C_COMMENT", StringType())
    ])),
    ('orders_ice', StructType([
        StructField("O_ORDERKEY", LongType()),
        StructField("O_CUSTKEY", LongType()),
        StructField("O_ORDERSTATUS", StringType()),
        StructField("O_TOTALPRICE", DoubleType()),
        StructField("O_ORDERDATE", StringType()),
        StructField("O_ORDERPRIORITY", StringType()),
        StructField("O_CLERK", StringType()),
        StructField("O_SHIPPRIORITY", IntegerType()),
        StructField("O_COMMENT", StringType())
    ]))
]

# Creating DataFrames
def schema_to_table(schema, db, table_name):
    # global delete_mode, update_mode, merge_mode, PARTITIONING
    df = spark.createDataFrame([], schema=schema)
    df.write.format('iceberg').mode("overwrite") \
        .option("write.delete.mode", DELETE_MODE) \
        .option("write.update.mode", UPDATE_MODE) \
        .option("write.merge.mode", MERGE_MODE) \
        .saveAsTable(f"{db}.{table_name}") \
           

for table in schemas:
    table_name = table[0]
    schema = table[1]
    schema_to_table(schema, 'iceberg_temp', table_name)

# Stop the Spark session
spark.stop()
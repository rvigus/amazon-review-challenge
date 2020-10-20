from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Process sku data") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file sku_data.json from the HDFS
df = spark.read.csv('hdfs://namenode:9000/dim_sku/sku_data.csv', sep='|', header=True)

# Drop the duplicated rows based on the base and last_update columns
dim_sku = df.select('asin', 'title', 'price', 'brand') \
    .dropDuplicates(['asin'])

# Export the dataframe into the Hive table dim_sku
dim_sku.write.mode("append").insertInto("dim_sku")
# dim_sku.write.saveAsTable('dim_sku')
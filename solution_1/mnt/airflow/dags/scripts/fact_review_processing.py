from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Process fact data") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file sku_data.json from the HDFS
df = spark.read.csv('hdfs://namenode:9000/fact_review/review_data.csv', sep='|', header=True)

# Drop the duplicated rows based on the base and last_update columns
fact_review = df.dropDuplicates(['reviewerID', 'asin', 'unixReviewTime'])

# Export the dataframe into the Hive table dim_sku
fact_review.write.mode("append").insertInto("fact_review")

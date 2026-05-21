from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Orders ETL") \
    .getOrCreate()

# Read CSV file
orders = spark.read.csv(
    "/home/jovyan/work/data/raw/orders.csv",
    header=True,
    inferSchema=True
)

print("Raw Orders Data:")
orders.show()

# Remove duplicate rows
clean_orders = orders.dropDuplicates()

# Create total_amount column
clean_orders = clean_orders.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

# Convert timestamp to date
clean_orders = clean_orders.withColumn(
    "order_date",
    to_date(col("order_timestamp"))
)

print("Transformed Orders Data:")
clean_orders.show()

# Save processed data as parquet
clean_orders.write.mode("overwrite").parquet(
    "/home/jovyan/work/data/processed/orders_parquet"
)

print("ETL completed successfully")

spark.stop()
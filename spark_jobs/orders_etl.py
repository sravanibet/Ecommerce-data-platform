from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# Fix Spark hostname issue on Windows
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Create Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Orders ETL") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Read CSV file
orders = spark.read.csv(
    "data/raw/orders.csv",
    header=True,
    inferSchema=True
)

# Show raw data
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

# Show transformed data
print("Transformed Orders Data:")
clean_orders.show()

# Save processed data as parquet
clean_orders.write.mode("overwrite").parquet(
    "data/processed/orders"
)

print("ETL completed successfully")

# Stop Spark
spark.stop()
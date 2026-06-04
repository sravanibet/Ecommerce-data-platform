import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--conf spark.jars.ivy=/tmp/.ivy2 --packages io.delta:delta-core_2.12:2.4.0 pyspark-shell"


def create_spark_session(master_url: str, app_name: str) -> SparkSession:
    return (
        SparkSession.builder.master(master_url)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        .getOrCreate()
    )


def build_orders_schema():
    return StructType(
        [
            StructField("order_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("category", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DoubleType()),
            StructField("status", StringType()),
            StructField("event_time", StringType()),
        ]
    )


def main() -> None:
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Delta Merge Orders",
    )

    delta_path = "/opt/workspace/data/bronze/orders"
    csv_path = "/opt/workspace/data/raw/orders.csv"

    source = spark.read.csv(csv_path, header=True, inferSchema=True)
    updates = (
        source.withColumn("event_time", to_timestamp(col("order_timestamp")))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("amount", col("quantity") * col("price"))
        .withColumn("source", lit("cdc"))
    )

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            updates.alias("src"),
            "target.order_id = src.order_id",
        ).whenMatchedUpdate(
            set={
                "quantity": col("src.quantity"),
                "price": col("src.price"),
                "amount": col("src.amount"),
                "status": col("src.status"),
                "event_time": col("src.event_time"),
            }
        ).whenNotMatchedInsertAll().execute()
    else:
        updates.write.format("delta").mode("overwrite").partitionBy("event_date").save(delta_path)

    print("Delta merge/upsert completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()

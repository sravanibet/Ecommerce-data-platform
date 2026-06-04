import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    to_date,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Required by Delta Lake and Kafka connectors in PySpark
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 pyspark-shell"


def create_spark_session(master_url: str, app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.master(master_url)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = builder.enableHiveSupport().getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version", "2"
    )
    return spark


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("status", StringType()),
        StructField("event_time", StringType()),
        StructField("event_id", StringType()),
    ]
)


BRONZE_PATH = "/opt/workspace/data/bronze/orders"
SILVER_PATH = "/opt/workspace/data/silver/orders"
GOLD_PATH = "/opt/workspace/data/gold/orders"
CHECKPOINT_BASE = "/opt/workspace/data/checkpoints"


def parse_stream(kafka_bootstrap: str, topic: str):
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Streaming Orders Bronze",
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        spark,
        raw_stream.selectExpr("CAST(value AS STRING) as value")
        .select(from_json(col("value"), ORDER_SCHEMA).alias("payload"))
        .select("payload.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("amount", col("quantity") * col("price"))
    )


def bronze_stream(kafka_bootstrap: str, topic: str):
    spark, parsed = parse_stream(kafka_bootstrap=kafka_bootstrap, topic=topic)
    checkpoint = f"{CHECKPOINT_BASE}/bronze_orders"

    query = (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("bronze_orders")
        .start(BRONZE_PATH)
    )

    return spark, query


def silver_stream():
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Streaming Orders Silver",
    )

    bronze = (
        spark.readStream.format("delta")
        .load(BRONZE_PATH)
    )

    cleaned = (
        bronze.where(col("status").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("stage", lit("silver"))
    )

    checkpoint = f"{CHECKPOINT_BASE}/silver_orders"
    query = (
        cleaned.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("silver_orders")
        .start(SILVER_PATH)
    )

    return spark, query


def gold_stream():
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Streaming Orders Gold",
    )

    silver = (
        spark.readStream.format("delta")
        .load(SILVER_PATH)
    )

    aggregated = (
        silver.withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "5 minutes"), col("category"))
        .agg(
            {"amount": "sum"},
            {"order_id": "count"},
        )
        .withColumnRenamed("sum(amount)", "total_revenue")
        .withColumnRenamed("count(order_id)", "order_count")
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("category"),
            col("total_revenue"),
            col("order_count"),
        )
    )

    checkpoint = f"{CHECKPOINT_BASE}/gold_orders"
    query = (
        aggregated.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("gold_orders")
        .start(GOLD_PATH)
    )

    return spark, query


def create_hive_gold_table(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS ecommerce.orders_gold USING DELTA LOCATION '/opt/workspace/data/gold/orders'"
    )


def main():
    parser = argparse.ArgumentParser(description="Run the Spark streaming medallion pipeline.")
    parser.add_argument("--duration", type=int, default=120, help="Seconds to run the streaming job")
    parser.add_argument("--kafka-bootstrap", default="kafka:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="orders", help="Kafka topic to consume")
    parser.add_argument(
        "--layer",
        default="all",
        choices=["all", "bronze", "silver", "gold"],
        help="Choose which medallion layer to execute",
    )
    args = parser.parse_args()

    queries = []
    spark_sessions = []

    if args.layer in ("all", "bronze"):
        spark, query = bronze_stream(kafka_bootstrap=args.kafka_bootstrap, topic=args.topic)
        spark_sessions.append(spark)
        queries.append(query)

    if args.layer in ("all", "silver"):
        spark, query = silver_stream()
        spark_sessions.append(spark)
        queries.append(query)

    if args.layer in ("all", "gold"):
        spark, query = gold_stream()
        spark_sessions.append(spark)
        queries.append(query)

    if args.layer in ("all", "gold"):
        create_hive_gold_table(spark_sessions[-1])

    print(f"Streaming layer={args.layer} started. Running for {args.duration} seconds.")
    try:
        if queries:
            spark_sessions[0].streams.awaitAnyTermination(timeout=args.duration)
    finally:
        for query in queries:
            query.stop()
        for spark in spark_sessions:
            spark.stop()
        print("Streaming pipeline stopped.")


if __name__ == "__main__":
    main()

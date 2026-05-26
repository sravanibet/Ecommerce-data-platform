from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def create_spark_session(
    *,
    master_url: str,
    app_name: str,
    enable_hive: bool = True,
) -> SparkSession:
    """Create a SparkSession configured for this project.

    Why this exists:
    - We keep SparkSession configuration in one place (so jobs stay readable).
    - The rest of the ETL code can focus on extract/transform/load steps.
    """

    builder = SparkSession.builder.master(master_url).appName(app_name)
    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()

    # Helps avoid "rename" patterns in some file output committers.
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version", "2"
    )

    return spark


def extract_orders_csv(*, spark: SparkSession, input_path: str) -> DataFrame:
    """Extract raw orders data from a CSV file into a Spark DataFrame."""

    return spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
    )


def extract_customers_csv(*, spark: SparkSession, input_path: str) -> DataFrame:
    """Extract customers dimension data from a CSV file."""

    return spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
    )


def extract_products_csv(*, spark: SparkSession, input_path: str) -> DataFrame:
    """Extract products dimension data from a CSV file."""

    return spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
    )

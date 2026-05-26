from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def write_parquet(
    *,
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite",
) -> None:
    """Load a DataFrame to a Parquet dataset on disk."""

    df.write.mode(mode).parquet(output_path)


def write_partitioned_parquet(
    *,
    df: DataFrame,
    output_path: str,
    partition_col: str,
    mode: str = "overwrite",
) -> None:
    """Load a DataFrame to partitioned Parquet on disk (for faster queries)."""

    df.write.mode(mode).partitionBy(partition_col).parquet(output_path)


def write_partitioned_hive_table(
    *,
    spark: SparkSession,
    df: DataFrame,
    database: str,
    table: str,
    table_path: str,
    partition_col: str,
    mode: str = "overwrite",
) -> None:
    """Load a partitioned Hive table (fact table pattern)."""

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
    (
        df.write.mode(mode)
        .partitionBy(partition_col)
        .option("path", table_path)
        .saveAsTable(f"{database}.{table}")
    )


def write_hive_table(
    *,
    spark: SparkSession,
    df: DataFrame,
    database: str,
    table: str,
    table_path: str,
    mode: str = "overwrite",
) -> None:
    """Load a DataFrame into a Hive table.

    The table is stored at `table_path` so data persists in the Hive warehouse
    volume (see `docker-compose.yml`).
    """

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
    df.write.mode(mode).option("path", table_path).saveAsTable(f"{database}.{table}")

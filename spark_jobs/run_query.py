from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Spark SQL query with Hive support.")
    parser.add_argument("sql", help="SQL to execute (wrap in quotes in your shell).")
    parser.add_argument(
        "--master",
        default="spark://spark-master:7077",
        help="Spark master URL (default: spark://spark-master:7077)",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=50,
        help="Max rows to show (default: 50)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master(args.master)
        .appName("Run Query")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql(args.sql).show(args.show, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()


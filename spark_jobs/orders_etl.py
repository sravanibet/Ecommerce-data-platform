from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def main():
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("Orders ETL")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version", "2"
    )

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/workspace/data/processed/orders_{run_id}"
    table_path = f"/opt/hive/data/warehouse/ecommerce.db/orders_clean_{run_id}"

    orders = spark.read.csv(
        "/opt/workspace/data/raw/orders.csv",
        header=True,
        inferSchema=True,
    )

    clean_orders = (
        orders.dropDuplicates()
        .withColumn("total_amount", col("quantity") * col("price"))
        .withColumn("order_date", to_date(col("order_timestamp")))
    )

    print("Raw Orders Data:")
    orders.show()

    print("Transformed Orders Data:")
    clean_orders.show()

    clean_orders.write.mode("overwrite").parquet(output_path)

    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")
    spark.sql("DROP TABLE IF EXISTS ecommerce.orders_clean")
    clean_orders.write.mode("overwrite").option("path", table_path).saveAsTable(
        "ecommerce.orders_clean"
    )

    spark.sql(
        """
        SELECT
            customer_id,
            SUM(total_amount) AS revenue
        FROM ecommerce.orders_clean
        GROUP BY customer_id
        ORDER BY revenue DESC
        """
    ).show()

    print("ETL completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()

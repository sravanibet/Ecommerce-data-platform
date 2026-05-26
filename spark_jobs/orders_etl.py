from datetime import datetime
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from retail_etl.extract import create_spark_session, extract_orders_csv
from retail_etl.load import write_hive_table, write_parquet
from retail_etl.transform import transform_orders


def main():
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Orders ETL",
        enable_hive=True,
    )

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/workspace/data/processed/orders_{run_id}"
    table_path = f"/opt/hive/data/warehouse/ecommerce.db/orders_clean_{run_id}"

    orders = extract_orders_csv(spark=spark, input_path="/opt/workspace/data/raw/orders.csv")
    clean_orders = transform_orders(orders)

    print("Raw Orders Data:")
    orders.show()

    print("Transformed Orders Data:")
    clean_orders.show()

    write_parquet(df=clean_orders, output_path=output_path, mode="overwrite")
    write_hive_table(
        spark=spark,
        df=clean_orders,
        database="ecommerce",
        table="orders_clean",
        table_path=table_path,
        mode="overwrite",
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

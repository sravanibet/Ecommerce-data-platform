from __future__ import annotations

from datetime import datetime
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from retail_etl.extract import create_spark_session
from retail_etl.warehouse import WarehousePaths, run_warehouse_etl


def main() -> None:
    spark = create_spark_session(
        master_url="spark://spark-master:7077",
        app_name="Warehouse ETL",
        enable_hive=True,
    )

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    raw_base = "/opt/workspace/data/raw"
    processed_base = "/opt/workspace/data/processed"
    hive_base = "/opt/hive/data/warehouse/ecommerce.db"

    paths = WarehousePaths(
        raw_orders_csv=f"{raw_base}/orders.csv",
        raw_customers_csv=f"{raw_base}/customers.csv",
        raw_products_csv=f"{raw_base}/products.csv",
        # Use a run-specific output directory to avoid deletion issues on
        # Windows/OneDrive mounts where Spark may fail to clear overwrite
        # targets due to lingering `_temporary` files.
        processed_orders_clean=f"{processed_base}/orders/orders_clean/run_id={run_id}",
        processed_fact_orders=f"{processed_base}/fact_orders/run_id={run_id}",
        processed_dim_customers=f"{processed_base}/dimensions/dim_customers/run_id={run_id}",
        processed_dim_products=f"{processed_base}/dimensions/dim_products/run_id={run_id}",
        hive_fact_orders_path=f"{hive_base}/fact_orders_{run_id}",
        hive_dim_customers_path=f"{hive_base}/dim_customers_{run_id}",
        hive_dim_products_path=f"{hive_base}/dim_products_{run_id}",
    )

    fact_orders = run_warehouse_etl(spark=spark, paths=paths)

    spark.sql(
        """
        SELECT
            category,
            SUM(total_amount) AS revenue
        FROM ecommerce.fact_orders
        GROUP BY category
        ORDER BY revenue DESC
        """
    ).show()

    print("Warehouse ETL completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from retail_etl.extract import extract_customers_csv, extract_orders_csv, extract_products_csv
from retail_etl.joins import join_orders_customers, join_with_products
from retail_etl.load import (
    write_hive_table,
    write_partitioned_hive_table,
    write_partitioned_parquet,
    write_parquet,
)
from retail_etl.transform import transform_orders
from retail_etl.validations import assert_no_duplicate_keys, assert_no_nulls


@dataclass(frozen=True)
class WarehousePaths:
    raw_orders_csv: str
    raw_customers_csv: str
    raw_products_csv: str
    processed_orders_clean: str
    processed_fact_orders: str
    processed_dim_customers: str
    processed_dim_products: str
    hive_fact_orders_path: str
    hive_dim_customers_path: str
    hive_dim_products_path: str


def build_fact_orders(
    *,
    orders_clean: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> DataFrame:
    orders_customers = join_orders_customers(orders_clean=orders_clean, customers=customers)
    return join_with_products(orders_customers=orders_customers, products=products, broadcast_products=True)


def run_warehouse_etl(*, spark: SparkSession, paths: WarehousePaths) -> DataFrame:
    """Run Phase 2 ETL: build fact + dimensions and write outputs.

    Returns the final fact dataframe for optional downstream queries/printing.
    """

    orders = extract_orders_csv(spark=spark, input_path=paths.raw_orders_csv)
    customers = extract_customers_csv(spark=spark, input_path=paths.raw_customers_csv)
    products = extract_products_csv(spark=spark, input_path=paths.raw_products_csv)

    orders_clean = transform_orders(orders)
    fact_orders = build_fact_orders(
        orders_clean=orders_clean,
        customers=customers,
        products=products,
    )

    assert_no_nulls(df=fact_orders, column="customer_id")
    assert_no_nulls(df=fact_orders, column="product_id")
    assert_no_duplicate_keys(df=fact_orders, key_column="order_id")

    # Optional: persist the cleaned single-table output (useful for debugging).
    write_parquet(df=orders_clean, output_path=paths.processed_orders_clean, mode="overwrite")

    # Fact table: partitioned for query pruning.
    write_partitioned_parquet(
        df=fact_orders,
        output_path=paths.processed_fact_orders,
        partition_col="order_date",
        mode="overwrite",
    )
    write_partitioned_hive_table(
        spark=spark,
        df=fact_orders,
        database="ecommerce",
        table="fact_orders",
        table_path=paths.hive_fact_orders_path,
        partition_col="order_date",
        mode="overwrite",
    )

    # Dimensions: overwrite snapshot (simple, beginner-friendly pattern).
    write_parquet(df=customers, output_path=paths.processed_dim_customers, mode="overwrite")
    write_hive_table(
        spark=spark,
        df=customers,
        database="ecommerce",
        table="dim_customers",
        table_path=paths.hive_dim_customers_path,
        mode="overwrite",
    )

    write_parquet(df=products, output_path=paths.processed_dim_products, mode="overwrite")
    write_hive_table(
        spark=spark,
        df=products,
        database="ecommerce",
        table="dim_products",
        table_path=paths.hive_dim_products_path,
        mode="overwrite",
    )

    return fact_orders

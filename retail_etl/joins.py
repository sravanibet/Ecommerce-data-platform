from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast


def join_orders_customers(*, orders_clean: DataFrame, customers: DataFrame) -> DataFrame:
    """Join cleaned orders with customers (dimension)."""

    return orders_clean.join(customers, on="customer_id", how="inner")


def join_with_products(
    *,
    orders_customers: DataFrame,
    products: DataFrame,
    broadcast_products: bool = True,
) -> DataFrame:
    """Join the enriched orders with products (dimension).

    Broadcasting the products dimension is a common Spark optimization when the
    dimension table is small.
    """

    products_df = broadcast(products) if broadcast_products else products
    return orders_customers.join(products_df, on="product_id", how="inner")


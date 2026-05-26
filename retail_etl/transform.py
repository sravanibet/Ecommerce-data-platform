from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


def transform_orders(orders: DataFrame) -> DataFrame:
    """Clean and enrich orders data.

    What this does (in simple terms):
    - Drops duplicate rows.
    - Creates `total_amount = quantity * price`.
    - Creates `order_date` extracted from `order_timestamp`.
    """

    return (
        orders.dropDuplicates()
        .withColumn("total_amount", col("quantity") * col("price"))
        .withColumn("order_date", to_date(col("order_timestamp")))
    )

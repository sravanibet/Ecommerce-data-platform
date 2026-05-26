from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def assert_no_nulls(*, df: DataFrame, column: str) -> None:
    """Fail the pipeline if `column` contains NULLs."""

    null_count = df.filter(col(column).isNull()).count()
    if null_count > 0:
        raise ValueError(f"Data quality check failed: `{column}` has {null_count} NULLs")


def assert_no_duplicate_keys(*, df: DataFrame, key_column: str) -> None:
    """Fail the pipeline if `key_column` contains duplicate values."""

    duplicate_groups = df.groupBy(key_column).count().filter(col("count") > 1).count()
    if duplicate_groups > 0:
        raise ValueError(
            f"Data quality check failed: `{key_column}` has {duplicate_groups} duplicate keys"
        )


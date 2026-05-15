from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class SourceTableSpec:
    source_system: str
    table_name: str
    full_table_name: str


def infer_simple_type(column_name: str) -> str:
    normalized = column_name.lower()

    if "email" in normalized:
        return "email"
    if "phone" in normalized or "mobile" in normalized:
        return "phone"
    if "birth" in normalized or "dob" in normalized:
        return "date"
    if "postal" in normalized or "zip" in normalized:
        return "postal_code"
    if "name" in normalized:
        return "name"
    if "address" in normalized:
        return "address"
    if normalized.endswith("_id") or normalized == "id":
        return "identifier"

    return "unknown"


def profile_column(
    *,
    df: DataFrame,
    source_system: str,
    table_name: str,
    column_name: str,
    batch_id: str,
) -> DataFrame:
    value_as_string = F.col(column_name).cast("string")

    return df.agg(
        F.lit(source_system).alias("source_system"),
        F.lit(table_name).alias("table_name"),
        F.lit(column_name).alias("column_name"),
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.when(F.col(column_name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "null_count"
        ),
        (
            F.sum(F.when(F.col(column_name).isNull(), F.lit(1)).otherwise(F.lit(0)))
            / F.count(F.lit(1))
        ).alias("null_rate"),
        F.countDistinct(F.col(column_name)).alias("distinct_count"),
        (F.countDistinct(F.col(column_name)) / F.count(F.lit(1))).alias(
            "distinct_rate"
        ),
        F.min(F.length(value_as_string)).alias("min_length"),
        F.max(F.length(value_as_string)).alias("max_length"),
        F.avg(F.length(value_as_string)).alias("avg_length"),
        F.array_join(
            F.slice(
                F.sort_array(F.collect_set(value_as_string)),
                1,
                5,
            ),
            " | ",
        ).alias("sample_values"),
        F.lit(infer_simple_type(column_name)).alias("inferred_type"),
        F.current_timestamp().alias("profiled_at"),
        F.lit(batch_id).alias("batch_id"),
    )


def profile_table(
    *,
    spark: SparkSession,
    spec: SourceTableSpec,
    batch_id: str,
) -> DataFrame:
    df = spark.table(spec.full_table_name)

    profile_dfs = [
        profile_column(
            df=df,
            source_system=spec.source_system,
            table_name=spec.table_name,
            column_name=column_name,
            batch_id=batch_id,
        )
        for column_name in df.columns
    ]

    if not profile_dfs:
        raise ValueError(f"Table {spec.full_table_name} has no columns.")

    result_df = profile_dfs[0]
    for profile_df in profile_dfs[1:]:
        result_df = result_df.unionByName(profile_df)

    return result_df


def write_source_column_profile(
    df: DataFrame,
    target_table: str,
) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def profile_sources(
    *,
    spark: SparkSession,
    specs: list[SourceTableSpec],
    target_table: str,
    batch_id: str,
) -> None:
    table_profiles = [
        profile_table(
            spark=spark,
            spec=spec,
            batch_id=batch_id,
        )
        for spec in specs
    ]

    if not table_profiles:
        raise ValueError("No source tables were provided for profiling.")

    result_df = table_profiles[0]
    for table_profile in table_profiles[1:]:
        result_df = result_df.unionByName(table_profile)

    write_source_column_profile(result_df, target_table)
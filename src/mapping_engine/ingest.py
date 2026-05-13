from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class BronzeIngestSpec:
    source_system: str
    landing_path: str
    file_name: str
    target_table: str

    @property
    def source_file_path(self) -> str:
        return f"{self.landing_path.rstrip('/')}/{self.file_name}"


def read_landing_csv(spark: SparkSession, source_file_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(source_file_path)
    )


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_system: str,
    source_file: str,
    batch_id: str,
) -> DataFrame:
    business_columns = df.columns

    record_hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit(""))
                for column in business_columns
            ],
        ),
        256,
    )

    return (
        df.withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_record_hash", record_hash_expr)
    )


def write_bronze_table(df: DataFrame, target_table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def ingest_csv_to_bronze(
    *,
    spark: SparkSession,
    spec: BronzeIngestSpec,
    batch_id: str,
) -> None:
    df = read_landing_csv(spark, spec.source_file_path)

    bronze_df = add_bronze_metadata(
        df,
        source_system=spec.source_system,
        source_file=spec.source_file_path,
        batch_id=batch_id,
    )

    write_bronze_table(bronze_df, spec.target_table)
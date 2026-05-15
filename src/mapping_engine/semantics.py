from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def normalize_name_expr(column: F.Column) -> F.Column:
    return F.lower(
        F.regexp_replace(
            F.trim(column.cast("string")),
            r"[^a-zA-Z0-9]+",
            "_",
        )
    )


def load_profile_and_concepts(
    spark: SparkSession,
    *,
    profile_table: str,
    concepts_table: str,
) -> tuple[DataFrame, DataFrame]:
    profiles = spark.table(profile_table)
    concepts = spark.table(concepts_table)

    return profiles, concepts


def build_semantic_mapping_candidates(
    profiles: DataFrame,
    concepts: DataFrame,
    *,
    batch_id: str,
) -> DataFrame:
    prepared_profiles = (
        profiles.filter(~F.col("column_name").startswith("_"))
        .withColumn("column_name_lc", F.lower(F.col("column_name")))
        .withColumn("column_name_normalized", normalize_name_expr(F.col("column_name")))
    )

    prepared_concepts = (
        concepts.withColumn("concept_name_lc", F.lower(F.col("concept_name")))
        .withColumn(
            "concept_name_normalized",
            normalize_name_expr(F.col("concept_name")),
        )
        .withColumn(
            "canonical_field_name_lc",
            F.lower(F.col("canonical_field_name")),
        )
        .withColumn(
            "canonical_field_name_normalized",
            normalize_name_expr(F.col("canonical_field_name")),
        )
    )

    candidate_df = prepared_profiles.crossJoin(prepared_concepts)

    scored_df = (
        candidate_df.withColumn(
            "canonical_exact_match",
            F.col("column_name_normalized")
            == F.col("canonical_field_name_normalized"),
        )
        .withColumn(
            "concept_exact_match",
            F.col("column_name_normalized") == F.col("concept_name_normalized"),
        )
        .withColumn(
            "canonical_partial_match",
            F.col("column_name_normalized").contains(
                F.col("canonical_field_name_normalized")
            )
            | F.col("canonical_field_name_normalized").contains(
                F.col("column_name_normalized")
            ),
        )
        .withColumn(
            "concept_partial_match",
            F.col("column_name_normalized").contains(F.col("concept_name_normalized"))
            | F.col("concept_name_normalized").contains(F.col("column_name_normalized")),
        )
        .withColumn(
            "inferred_type_match",
            (
                F.lower(F.coalesce(F.col("inferred_type"), F.lit("")))
                == F.col("canonical_field_name_lc")
            )
            | (
                (F.lower(F.col("inferred_type")) == F.lit("name"))
                & (F.col("canonical_field_name_lc") == F.lit("full_name"))
            )
            | (
                (F.lower(F.col("inferred_type")) == F.lit("date"))
                & (F.col("canonical_field_name_lc") == F.lit("date_of_birth"))
            ),
        )
        .withColumn(
            "mapping_score",
            F.least(
                F.lit(1.0),
                F.when(F.col("canonical_exact_match"), F.lit(0.65)).otherwise(
                    F.lit(0.0)
                )
                + F.when(F.col("concept_exact_match"), F.lit(0.55)).otherwise(
                    F.lit(0.0)
                )
                + F.when(F.col("canonical_partial_match"), F.lit(0.25)).otherwise(
                    F.lit(0.0)
                )
                + F.when(F.col("concept_partial_match"), F.lit(0.15)).otherwise(
                    F.lit(0.0)
                )
                + F.when(F.col("inferred_type_match"), F.lit(0.20)).otherwise(
                    F.lit(0.0)
                ),
            ),
        )
        .withColumn(
            "mapping_reason",
            F.concat_ws(
                ",",
                F.when(F.col("canonical_exact_match"), F.lit("CANONICAL_EXACT")),
                F.when(F.col("concept_exact_match"), F.lit("CONCEPT_EXACT")),
                F.when(F.col("canonical_partial_match"), F.lit("CANONICAL_PARTIAL")),
                F.when(F.col("concept_partial_match"), F.lit("CONCEPT_PARTIAL")),
                F.when(F.col("inferred_type_match"), F.lit("INFERRED_TYPE_MATCH")),
            ),
        )
        .filter(F.col("mapping_score") >= F.lit(0.30))
    )

    return scored_df.select(
        "source_system",
        "table_name",
        "column_name",
        "row_count",
        "null_rate",
        "distinct_count",
        "distinct_rate",
        "inferred_type",
        "concept_id",
        "concept_name",
        "canonical_field_name",
        "pii_category",
        "description",
        "mapping_score",
        "mapping_reason",
        F.current_timestamp().alias("created_at"),
        F.lit(batch_id).alias("batch_id"),
    )


def build_mapping_decisions(candidates: DataFrame) -> DataFrame:
    ranking_window = Window.partitionBy(
        "source_system",
        "table_name",
        "column_name",
    ).orderBy(
        F.desc("mapping_score"),
        F.asc("concept_id"),
    )

    ranked = candidates.withColumn(
        "candidate_rank",
        F.row_number().over(ranking_window),
    )

    return (
        ranked.filter(F.col("candidate_rank") == 1)
        .withColumn(
            "decision",
            F.when(F.col("mapping_score") >= F.lit(0.75), F.lit("AUTO_ACCEPT"))
            .when(F.col("mapping_score") >= F.lit(0.50), F.lit("REVIEW_REQUIRED"))
            .otherwise(F.lit("LOW_CONFIDENCE")),
        )
        .select(
            "source_system",
            "table_name",
            "column_name",
            "concept_id",
            "concept_name",
            "canonical_field_name",
            "pii_category",
            "description",
            "mapping_score",
            "mapping_reason",
            "decision",
            "created_at",
            "batch_id",
        )
    )


def write_semantic_mapping_outputs(
    *,
    candidates: DataFrame,
    decisions: DataFrame,
    candidates_table: str,
    decisions_table: str,
) -> None:
    (
        candidates.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(candidates_table)
    )

    (
        decisions.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(decisions_table)
    )


def run_semantic_mapping(
    *,
    spark: SparkSession,
    profile_table: str,
    concepts_table: str,
    candidates_table: str,
    decisions_table: str,
    batch_id: str,
) -> None:
    profiles, concepts = load_profile_and_concepts(
        spark,
        profile_table=profile_table,
        concepts_table=concepts_table,
    )

    candidates = build_semantic_mapping_candidates(
        profiles,
        concepts,
        batch_id=batch_id,
    )

    decisions = build_mapping_decisions(candidates)

    write_semantic_mapping_outputs(
        candidates=candidates,
        decisions=decisions,
        candidates_table=candidates_table,
        decisions_table=decisions_table,
    )
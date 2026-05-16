from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from mapping_engine.compliance import SuppressionReason

DELETION_BLOCKING_STATUSES = ["APPROVED", "ACTIVE", "COMPLETED"]


def normalize_source_system(column_name: str = "source_system") -> Column:
    return F.lower(F.trim(F.col(column_name).cast("string")))


def normalize_dataset_name(column_name: str = "dataset_name") -> Column:
    return F.lower(F.trim(F.col(column_name).cast("string")))


def parse_boolean_column(column_name: str) -> Column:
    normalized = F.lower(F.trim(F.col(column_name).cast("string")))

    return (
        F.when(normalized.isin("true", "1", "yes", "y"), F.lit(True))
        .when(normalized.isin("false", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(False))
    )


def build_consent_status(consent_raw: DataFrame) -> DataFrame:
    return consent_raw.select(
        normalize_source_system("source_system").alias("source_system"),
        F.col("source_customer_id").cast("string").alias("source_record_id"),
        parse_boolean_column("analytics_consent").alias(
            "raw_analytics_consent"
        ),
        parse_boolean_column("marketing_consent").alias(
            "raw_marketing_consent"
        ),
        F.to_timestamp("consent_updated_at").alias("consent_updated_at"),
        F.col("_source_file").alias("_source_file"),
        F.col("_ingested_at").alias("_ingested_at"),
        F.col("_batch_id").alias("_batch_id"),
        F.current_timestamp().alias("processed_at"),
    )


def build_deletion_requests(deletion_raw: DataFrame) -> DataFrame:
    return deletion_raw.select(
        F.col("request_id").cast("string").alias("request_id"),
        normalize_source_system("source_system").alias("source_system"),
        F.col("source_customer_id").cast("string").alias("source_record_id"),
        F.to_timestamp("requested_at").alias("requested_at"),
        F.upper(F.trim(F.col("status").cast("string"))).alias("status"),
        F.col("_source_file").alias("_source_file"),
        F.col("_ingested_at").alias("_ingested_at"),
        F.col("_batch_id").alias("_batch_id"),
        F.current_timestamp().alias("processed_at"),
    )


def build_retention_policy(retention_raw: DataFrame) -> DataFrame:
    return retention_raw.select(
        normalize_source_system("source_system").alias("source_system"),
        normalize_dataset_name("dataset_name").alias("dataset_name"),
        F.col("date_column").cast("string").alias("date_column"),
        F.col("retention_days").cast("int").alias("retention_days"),
        F.col("policy_owner").cast("string").alias("policy_owner"),
        F.col("_source_file").alias("_source_file"),
        F.col("_ingested_at").alias("_ingested_at"),
        F.col("_batch_id").alias("_batch_id"),
        F.current_timestamp().alias("processed_at"),
    )


def prepare_identity_candidates(identity_candidates: DataFrame) -> DataFrame:
    if "source_dataset_name" in identity_candidates.columns:
        source_dataset_name = F.lower(
            F.trim(F.col("source_dataset_name").cast("string"))
        )
    else:
        source_dataset_name = (
            F.when(
                F.lower(F.col("source_system")) == F.lit("crm"),
                F.lit("crm_customers"),
            )
            .when(
                F.lower(F.col("source_system")) == F.lit("erp"),
                F.lit("erp_clients"),
            )
            .otherwise(F.lit(None))
        )

    return identity_candidates.select(
        F.lower(F.trim(F.col("source_system").cast("string"))).alias(
            "source_system"
        ),
        source_dataset_name.alias("source_dataset_name"),
        F.col("source_record_id").cast("string").alias("source_record_id"),
        "customer_pseudo_id",
        "email_hash",
        "phone_hash",
        "standard_email",
        "standard_phone",
        "standard_name",
        "date_of_birth",
        "postal_code",
        "address_text",
        "source_record_hash",
        "_source_file",
        "_ingested_at",
        "_batch_id",
    )


def build_processing_eligibility(
    *,
    identity_candidates: DataFrame,
    consent_status: DataFrame,
    deletion_requests: DataFrame,
    retention_policy: DataFrame,
) -> DataFrame:
    prepared_identity = prepare_identity_candidates(identity_candidates)

    active_deletions = (
        deletion_requests.filter(F.col("status").isin(DELETION_BLOCKING_STATUSES))
        .select(
            "source_system",
            "source_record_id",
            F.col("request_id").alias("deletion_request_id"),
            F.lit(True).alias("has_deletion_request"),
            F.col("requested_at").alias("deletion_requested_at"),
            F.col("status").alias("deletion_status"),
        )
        .dropDuplicates(["source_system", "source_record_id"])
    )

    joined = (
        prepared_identity.join(
            consent_status.select(
                "source_system",
                "source_record_id",
                "raw_analytics_consent",
                "raw_marketing_consent",
                "consent_updated_at",
            ),
            on=["source_system", "source_record_id"],
            how="left",
        )
        .join(
            active_deletions,
            on=["source_system", "source_record_id"],
            how="left",
        )
        .join(
            retention_policy.select(
                F.col("source_system").alias("retention_source_system"),
                F.col("dataset_name").alias("retention_dataset_name"),
                "date_column",
                "retention_days",
                "policy_owner",
            ),
            (
                F.col("source_system")
                == F.col("retention_source_system")
            )
            & (
                F.col("source_dataset_name")
                == F.col("retention_dataset_name")
            ),
            how="left",
        )
    )

    has_valid_source_record_id = F.col("source_record_id").isNotNull()

    has_match_identifier = (
        F.col("email_hash").isNotNull()
        | F.col("phone_hash").isNotNull()
        | F.col("customer_pseudo_id").isNotNull()
    )

    has_valid_identifier = has_valid_source_record_id & has_match_identifier

    raw_analytics_consent = F.coalesce(
        F.col("raw_analytics_consent"),
        F.lit(False),
    )

    raw_marketing_consent = F.coalesce(
        F.col("raw_marketing_consent"),
        F.lit(False),
    )

    has_deletion_request = F.coalesce(
        F.col("has_deletion_request"),
        F.lit(False),
    )

    retention_days = F.coalesce(
        F.col("retention_days"),
        F.lit(3650),
    )

    retention_expired = (
        F.datediff(
            F.current_date(),
            F.to_date(F.col("_ingested_at")),
        )
        > retention_days
    )

    global_blocked = (
        has_deletion_request
        | retention_expired
        | ~has_valid_identifier
    )

    can_process_for_analytics = raw_analytics_consent & ~global_blocked
    can_process_for_marketing = raw_marketing_consent & ~global_blocked

    can_process_for_entity_resolution = can_process_for_analytics

    deletion_reason = F.when(
        has_deletion_request,
        F.lit(SuppressionReason.DELETION_REQUESTED.value),
    )

    retention_reason = F.when(
        retention_expired,
        F.lit(SuppressionReason.RETENTION_EXPIRED.value),
    )

    invalid_identifier_reason = F.when(
        ~has_valid_identifier,
        F.lit(SuppressionReason.INVALID_IDENTIFIER.value),
    )

    analytics_consent_reason = F.when(
        ~raw_analytics_consent,
        F.lit(SuppressionReason.NO_ANALYTICS_CONSENT.value),
    )

    marketing_consent_reason = F.when(
        ~raw_marketing_consent,
        F.lit(SuppressionReason.NO_MARKETING_CONSENT.value),
    )

    enriched = joined.withColumn(
        "_suppression_reasons",
        F.array_compact(
            F.array(
                deletion_reason,
                retention_reason,
                invalid_identifier_reason,
                analytics_consent_reason,
                marketing_consent_reason,
            )
        ),
    )

    return enriched.select(
        "source_system",
        "source_dataset_name",
        "source_record_id",
        "customer_pseudo_id",
        "email_hash",
        "phone_hash",
        "standard_email",
        "standard_phone",
        "standard_name",
        "date_of_birth",
        "postal_code",
        "address_text",
        raw_analytics_consent.alias("raw_analytics_consent"),
        raw_marketing_consent.alias("raw_marketing_consent"),
        can_process_for_analytics.alias("can_process_for_analytics"),
        can_process_for_marketing.alias("can_process_for_marketing"),
        has_deletion_request.alias("has_deletion_request"),
        F.col("deletion_request_id"),
        F.col("deletion_requested_at"),
        F.col("deletion_status"),
        retention_days.alias("retention_days"),
        F.col("date_column").alias("retention_date_column"),
        F.col("policy_owner").alias("retention_policy_owner"),
        retention_expired.alias("retention_expired"),
        has_valid_source_record_id.alias("has_valid_source_record_id"),
        has_match_identifier.alias("has_match_identifier"),
        has_valid_identifier.alias("has_valid_identifier"),
        F.col("_suppression_reasons").alias("suppression_reasons"),
        F.element_at(F.col("_suppression_reasons"), 1).alias(
            "suppression_reason"
        ),
        can_process_for_entity_resolution.alias(
            "can_process_for_entity_resolution"
        ),
        F.col("consent_updated_at"),
        "source_record_hash",
        "_source_file",
        "_ingested_at",
        F.col("_batch_id").alias("batch_id"),
        F.current_timestamp().alias("evaluated_at"),
    )


def build_compliance_events(processing_eligibility: DataFrame) -> DataFrame:
    return (
        processing_eligibility.select(
            "source_system",
            "source_dataset_name",
            "source_record_id",
            "customer_pseudo_id",
            "deletion_request_id",
            "batch_id",
            F.explode("suppression_reasons").alias("event_type"),
        )
        .withColumn("event_category", F.lit("SUPPRESSION"))
        .withColumn(
            "related_request_id",
            F.when(
                F.col("event_type")
                == F.lit(SuppressionReason.DELETION_REQUESTED.value),
                F.col("deletion_request_id"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("event_timestamp", F.current_timestamp())
        .select(
            "source_system",
            "source_dataset_name",
            "source_record_id",
            "customer_pseudo_id",
            "event_type",
            "event_category",
            "related_request_id",
            "event_timestamp",
            "batch_id",
        )
    )


def write_table(df: DataFrame, target_table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def run_compliance_controls(
    *,
    spark: SparkSession,
    catalog: str,
) -> None:
    identity_candidates = spark.table(
        f"{catalog}.silver.customer_identity_candidates"
    )
    consent_raw = spark.table(f"{catalog}.bronze.consent_preferences_raw")
    deletion_raw = spark.table(f"{catalog}.bronze.deletion_requests_raw")
    retention_raw = spark.table(f"{catalog}.bronze.data_retention_policy_raw")

    consent_status = build_consent_status(consent_raw)
    deletion_requests = build_deletion_requests(deletion_raw)
    retention_policy = build_retention_policy(retention_raw)

    processing_eligibility = build_processing_eligibility(
        identity_candidates=identity_candidates,
        consent_status=consent_status,
        deletion_requests=deletion_requests,
        retention_policy=retention_policy,
    )

    compliance_events = build_compliance_events(processing_eligibility)

    write_table(
        consent_status,
        f"{catalog}.compliance.consent_status",
    )

    write_table(
        deletion_requests,
        f"{catalog}.compliance.deletion_requests",
    )

    write_table(
        retention_policy,
        f"{catalog}.compliance.retention_policy",
    )

    write_table(
        processing_eligibility,
        f"{catalog}.compliance.processing_eligibility",
    )

    write_table(
        compliance_events,
        f"{catalog}.compliance.compliance_events",
    )
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def add_src_to_python_path() -> None:
    script_path = Path(add_src_to_python_path.__code__.co_filename)
    bundle_root = script_path.parent.parent
    src_path = bundle_root / "src"

    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pseudonymization-salt", required=False, default=None)

    return parser.parse_args()


def first_existing_column(df: DataFrame, candidates: list[str]) -> Column:
    normalized_columns = {
        column_name.lower(): column_name
        for column_name in df.columns
    }

    existing_columns = [
        F.col(normalized_columns[candidate.lower()]).cast("string")
        for candidate in candidates
        if candidate.lower() in normalized_columns
    ]

    if existing_columns:
        return F.coalesce(*existing_columns)

    return F.lit(None).cast("string")


def build_standardized_customer_df(
    df: DataFrame,
    *,
    source_system: str,
    pseudonymization_salt: str | None,
) -> DataFrame:
    from mapping_engine.masking import build_pseudo_id_column, hash_column
    from mapping_engine.standardization import (
        empty_string_to_null,
        normalize_email_column,
        normalize_phone_column,
        normalize_postal_code_column,
        normalize_text_column,
    )

    source_record_id = first_existing_column(
        df,
        [
            "source_customer_id",
            "source_record_id",
            "customer_id",
            "crm_customer_id",
            "erp_client_id",
            "client_id",
            "id",
        ],
    )

    first_name = first_existing_column(
        df,
        [
            "first_name",
            "given_name",
            "forename",
        ],
    )

    last_name = first_existing_column(
        df,
        [
            "last_name",
            "surname",
            "family_name",
        ],
    )

    full_name = first_existing_column(
        df,
        [
            "full_name",
            "name",
            "customer_name",
            "client_name",
        ],
    )

    raw_name = F.coalesce(
        full_name,
        F.concat_ws(" ", first_name, last_name),
    )

    raw_email = first_existing_column(
        df,
        [
            "email",
            "email_address",
            "contact_email",
        ],
    )

    raw_phone = first_existing_column(
        df,
        [
            "phone",
            "phone_number",
            "phone_number_e164",
            "contact_phone",
            "primary_phone",
            "mobile",
            "mobile_number",
            "mobile_phone",
            "telephone",
            "tel",
        ],
    )

    raw_date_of_birth = first_existing_column(
        df,
        [
            "date_of_birth",
            "birth_date",
            "dob",
        ],
    )

    raw_postal_code = first_existing_column(
        df,
        [
            "postal_code",
            "zip",
            "zip_code",
        ],
    )

    raw_address = F.concat_ws(
        " ",
        first_existing_column(
            df,
            [
                "address",
                "street",
                "street_address",
            ],
        ),
        first_existing_column(
            df,
            [
                "city",
            ],
        ),
        first_existing_column(
            df,
            [
                "country",
            ],
        ),
    )

    standard_name = normalize_text_column(raw_name)
    standard_email = normalize_email_column(raw_email)
    standard_phone = normalize_phone_column(raw_phone)
    standard_date_of_birth = empty_string_to_null(raw_date_of_birth)
    standard_postal_code = normalize_postal_code_column(raw_postal_code)
    standard_address = normalize_text_column(raw_address)

    return df.select(
        F.lit(source_system).alias("source_system"),
        empty_string_to_null(source_record_id).alias("source_record_id"),
        standard_name.alias("standard_name"),
        standard_email.alias("standard_email"),
        standard_phone.alias("standard_phone"),
        standard_date_of_birth.alias("date_of_birth"),
        standard_postal_code.alias("postal_code"),
        standard_address.alias("address_text"),
        hash_column(
            standard_email,
            salt=pseudonymization_salt,
        ).alias("email_hash"),
        hash_column(
            standard_phone,
            salt=pseudonymization_salt,
        ).alias("phone_hash"),
        build_pseudo_id_column(
            standard_email,
            standard_phone,
            standard_name,
            standard_date_of_birth,
            salt=pseudonymization_salt,
        ).alias("customer_pseudo_id"),
        F.col("_record_hash").alias("source_record_hash"),
        F.col("_source_file").alias("_source_file"),
        F.col("_ingested_at").alias("_ingested_at"),
        F.col("_batch_id").alias("_batch_id"),
    )


def write_table(df: DataFrame, target_table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def main() -> None:
    add_src_to_python_path()

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    crm_raw = spark.table(f"{args.catalog}.bronze.crm_customers_raw")
    erp_raw = spark.table(f"{args.catalog}.bronze.erp_clients_raw")

    crm_standardized = build_standardized_customer_df(
        crm_raw,
        source_system="crm",
        pseudonymization_salt=args.pseudonymization_salt,
    )

    erp_standardized = build_standardized_customer_df(
        erp_raw,
        source_system="erp",
        pseudonymization_salt=args.pseudonymization_salt,
    )

    write_table(
        crm_standardized,
        f"{args.catalog}.silver.crm_customers_standardized",
    )

    write_table(
        erp_standardized,
        f"{args.catalog}.silver.erp_clients_standardized",
    )

    customer_identity_candidates = crm_standardized.unionByName(erp_standardized)

    write_table(
        customer_identity_candidates,
        f"{args.catalog}.silver.customer_identity_candidates",
    )


if __name__ == "__main__":
    main()
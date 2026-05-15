from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def add_src_to_python_path() -> None:
    script_path = Path(add_src_to_python_path.__code__.co_filename)
    bundle_root = script_path.parent.parent
    src_path = bundle_root / "src"

    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--batch-id", required=True)

    return parser.parse_args()


def main() -> None:
    add_src_to_python_path()

    from mapping_engine.profiling import SourceTableSpec, profile_sources

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    specs = [
        SourceTableSpec(
            source_system="crm",
            table_name="crm_customers_raw",
            full_table_name=f"{args.catalog}.bronze.crm_customers_raw",
        ),
        SourceTableSpec(
            source_system="erp",
            table_name="erp_clients_raw",
            full_table_name=f"{args.catalog}.bronze.erp_clients_raw",
        ),
    ]

    profile_sources(
        spark=spark,
        specs=specs,
        target_table=f"{args.catalog}.metadata.source_column_profile",
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    main()
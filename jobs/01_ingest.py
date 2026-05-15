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
    parser.add_argument("--crm-landing-path", required=True)
    parser.add_argument("--erp-landing-path", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--reference-landing-path", required=True)

    return parser.parse_args()


def main() -> None:
    add_src_to_python_path()

    from mapping_engine.ingest import BronzeIngestSpec, ingest_csv_to_bronze

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    specs = [
        BronzeIngestSpec(
            source_system="crm",
            landing_path=args.crm_landing_path,
            file_name="crm_customers.csv",
            target_table=f"{args.catalog}.bronze.crm_customers_raw",
        ),
        BronzeIngestSpec(
            source_system="erp",
            landing_path=args.erp_landing_path,
            file_name="erp_clients.csv",
            target_table=f"{args.catalog}.bronze.erp_clients_raw",
        ),
        BronzeIngestSpec(
            source_system="reference",
            landing_path=args.reference_landing_path,
            file_name="semantic_concepts.csv",
            target_table=f"{args.catalog}.bronze.semantic_concepts_raw",
        ),
    ]

    for spec in specs:
        ingest_csv_to_bronze(
            spark=spark,
            spec=spec,
            batch_id=args.batch_id,
        )


if __name__ == "__main__":
    main()
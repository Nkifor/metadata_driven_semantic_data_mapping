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

    from mapping_engine.semantics import run_semantic_mapping

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    run_semantic_mapping(
        spark=spark,
        profile_table=f"{args.catalog}.metadata.source_column_profile",
        concepts_table=f"{args.catalog}.bronze.semantic_concepts_raw",
        candidates_table=f"{args.catalog}.metadata.semantic_mapping_candidates",
        decisions_table=f"{args.catalog}.metadata.mapping_decisions",
        batch_id=args.batch_id,
    )


if __name__ == "__main__":
    main()
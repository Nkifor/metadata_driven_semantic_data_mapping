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

    return parser.parse_args()


def main() -> None:
    add_src_to_python_path()

    from mapping_engine.compliance_controls import run_compliance_controls

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    run_compliance_controls(
        spark=spark,
        catalog=args.catalog,
    )


if __name__ == "__main__":
    main()
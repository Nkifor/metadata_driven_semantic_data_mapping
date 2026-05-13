"""Smoke test for Unity Catalog external volumes.

This job validates the repo-2 outputs consumed by the application repository:
- volume paths exist,
- job cluster can write to them,
- job cluster can read from them,
- job cluster can clean up test files.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class VolumeCheckResult:
    path: str
    listed: bool
    written: bool
    read_back: bool
    removed: bool
    test_file: str


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate read/write/delete access to UC volumes.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--paths", nargs="+", required=True)
    return parser.parse_args(argv)


def get_dbutils(spark: SparkSession):  # noqa: ANN201
    """Create Databricks dbutils object in a Python script task."""

    try:
        from pyspark.dbutils import DBUtils
    except ImportError as exc:  # pragma: no cover - Databricks-only path
        raise RuntimeError("This smoke test must run on Databricks with dbutils available.") from exc
    return DBUtils(spark)


def check_path(dbutils, base_path: str, run_id: str) -> VolumeCheckResult:  # noqa: ANN001
    normalized_base = base_path.rstrip("/")
    test_dir = f"{normalized_base}/_smoke_tests/{run_id}"
    test_file = f"{test_dir}/smoke.txt"
    payload = f"smoke-test-ok run_id={run_id} ts={datetime.now(tz=UTC).isoformat()}"

    dbutils.fs.ls(normalized_base)
    dbutils.fs.mkdirs(test_dir)
    dbutils.fs.put(test_file, payload, overwrite=True)
    read_payload = dbutils.fs.head(test_file, max_bytes=len(payload) + 100)
    if read_payload != payload:
        raise AssertionError(f"Read-back mismatch for {test_file}")
    dbutils.fs.rm(test_dir, recurse=True)

    return VolumeCheckResult(
        path=base_path,
        listed=True,
        written=True,
        read_back=True,
        removed=True,
        test_file=test_file,
    )


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("external-volume-smoke-test").getOrCreate()
    dbutils = get_dbutils(spark)

    spark.sql(f"USE CATALOG {args.catalog}")
    run_id = str(uuid.uuid4())
    results = [check_path(dbutils, path, run_id) for path in args.paths]

    print(json.dumps({"run_id": run_id, "results": [asdict(item) for item in results]}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])

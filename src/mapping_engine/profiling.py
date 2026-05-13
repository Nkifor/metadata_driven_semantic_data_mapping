"""Column profiling helpers.

Spark dataframe profiling will be added in Phase 5. This file starts with small pure helpers so
we can keep unit tests fast and deterministic.
"""

import re
from collections.abc import Iterable
from statistics import mean

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")


def infer_scalar_type(value: object) -> str:
    """Infer a simple semantic scalar type from one value."""

    if value is None or value == "":
        return "null"
    text = str(value).strip()
    if _EMAIL_RE.match(text):
        return "email"
    if _DATE_RE.match(text):
        return "date"
    if _INT_RE.match(text):
        return "integer"
    if _FLOAT_RE.match(text):
        return "float"
    return "string"


def summarize_lengths(values: Iterable[object]) -> dict[str, float | int | None]:
    """Return min/max/avg string length for non-empty values."""

    lengths = [len(str(value)) for value in values if value is not None and str(value) != ""]
    if not lengths:
        return {"min_length": None, "max_length": None, "avg_length": None}
    return {
        "min_length": min(lengths),
        "max_length": max(lengths),
        "avg_length": round(mean(lengths), 2),
    }

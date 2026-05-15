"""Pure standardization helpers used before hashing and matching."""

import re
import unicodedata

from pyspark.sql import Column
from pyspark.sql import functions as F

_WHITESPACE_RE = re.compile(r"\s+")
_NON_DIGIT_RE = re.compile(r"\D+")


def normalize_whitespace(value: str | None) -> str | None:
    """Trim text and collapse repeated whitespace.

    Empty strings become ``None`` so downstream checks can treat missing values consistently.
    """

    if value is None:
        return None
    normalized = _WHITESPACE_RE.sub(" ", str(value).strip())
    return normalized or None


def strip_accents(value: str | None) -> str | None:
    """Remove diacritics from text while preserving base characters."""

    value = normalize_whitespace(value)
    if value is None:
        return None
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def normalize_name(value: str | None) -> str | None:
    """Normalize a personal or company name for matching, not for display."""

    value = strip_accents(value)
    if value is None:
        return None
    value = re.sub(r"[^A-Za-z0-9 -]", "", value)
    return normalize_whitespace(value.upper())


def normalize_email(value: str | None) -> str | None:
    """Normalize email for deterministic hashing and exact matching."""

    value = normalize_whitespace(value)
    if value is None:
        return None
    return value.lower()


def normalize_phone(value: str | None, default_country_code: str = "48") -> str | None:
    """Normalize phone to a simple E.164-like representation.

    This is intentionally conservative for v1. Production logic should use a phone parsing
    library and country-aware rules.
    """

    value = normalize_whitespace(value)
    if value is None:
        return None

    has_plus = value.startswith("+")
    digits = _NON_DIGIT_RE.sub("", value)
    if not digits:
        return None

    if has_plus:
        return f"+{digits}"
    if len(digits) == 9:
        return f"+{default_country_code}{digits}"
    if digits.startswith("00"):
        return f"+{digits[2:]}"
    return f"+{digits}"


def normalize_postal_code(value: str | None) -> str | None:
    """Normalize postal code to uppercase without spaces."""

    value = normalize_whitespace(value)
    if value is None:
        return None
    return value.replace(" ", "").upper()


def empty_string_to_null(value: Column) -> Column:
    normalized = F.trim(value.cast("string"))

    return F.when(F.length(normalized) == 0, F.lit(None)).otherwise(normalized)


def normalize_email_column(value: Column) -> Column:
    return F.lower(empty_string_to_null(value))


def normalize_phone_column(value: Column, default_country_code: str = "48") -> Column:
    raw_value = F.trim(value.cast("string"))
    has_plus = raw_value.startswith("+")
    digits = F.regexp_replace(raw_value, r"\D+", "")

    return (
        F.when(F.length(digits) == 0, F.lit(None))
        .when(has_plus, F.concat(F.lit("+"), digits))
        .when(F.length(digits) == 9, F.concat(F.lit(f"+{default_country_code}"), digits))
        .when(digits.startswith("00"), F.concat(F.lit("+"), F.substring(digits, 3, 100)))
        .otherwise(F.concat(F.lit("+"), digits))
    )


def normalize_text_column(value: Column) -> Column:
    normalized = F.lower(
        F.regexp_replace(
            F.trim(value.cast("string")),
            r"\s+",
            " ",
        )
    )

    return F.when(F.length(normalized) == 0, F.lit(None)).otherwise(normalized)


def normalize_postal_code_column(value: Column) -> Column:
    normalized = F.upper(
        F.regexp_replace(
            F.trim(value.cast("string")),
            r"\s+",
            "",
        )
    )

    return F.when(F.length(normalized) == 0, F.lit(None)).otherwise(normalized)
"""Deterministic pseudonymization helpers.

Do not store production salts or pepper values in this repository. In production,
inject them from Databricks secret scopes backed by Azure Key Vault or an equivalent
secret manager.

The v1 implementation uses deterministic salted SHA-256 for consistency between
local Python helpers and Spark Column helpers.
"""

from __future__ import annotations

import hashlib
from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

from mapping_engine.standardization import (
    normalize_email,
    normalize_phone,
    normalize_whitespace,
)

LOCAL_DEV_SALT = "LOCAL_DEV_ONLY_REPLACE_WITH_SECRET_SCOPE_VALUE"


def _effective_salt(salt: str | None) -> str:
    return salt or LOCAL_DEV_SALT


def _canonical(value: str | None) -> str | None:
    value = normalize_whitespace(value)
    return value.lower() if value else None


def hash_identifier(value: str | None, *, salt: str | None = LOCAL_DEV_SALT) -> str | None:
    """Return a deterministic salted SHA-256 hash for an identifier."""

    canonical = _canonical(value)
    if canonical is None:
        return None

    payload = f"{_effective_salt(salt)}||{canonical}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_email(value: str | None, *, salt: str | None = LOCAL_DEV_SALT) -> str | None:
    """Normalize and hash email."""

    return hash_identifier(normalize_email(value), salt=salt)


def hash_phone(value: str | None, *, salt: str | None = LOCAL_DEV_SALT) -> str | None:
    """Normalize and hash phone number."""

    return hash_identifier(normalize_phone(value), salt=salt)


def build_customer_pseudo_id(
    *identifiers: str | None,
    salt: str | None = LOCAL_DEV_SALT,
) -> str | None:
    """Build a stable pseudonymous customer ID from the first available identifier."""

    for identifier in identifiers:
        hashed = hash_identifier(identifier, salt=salt)
        if hashed is not None:
            return f"cust_{hashed[:24]}"

    return None


def mask_email_for_display(value: str | None) -> str | None:
    """Return a minimally useful masked email for logs or QA views."""

    email = normalize_email(value)
    if email is None or "@" not in email:
        return None

    local, domain = email.split("@", 1)
    prefix = local[:2] if len(local) > 1 else "*"

    return f"{prefix}***@{domain}"


def _canonical_column(value: Column) -> Column:
    normalized = F.lower(F.trim(value.cast("string")))

    return F.when(F.length(normalized) == 0, F.lit(None)).otherwise(normalized)


def hash_column(value: Column, salt: str | None = LOCAL_DEV_SALT) -> Column:
    """Return a deterministic salted SHA-256 hash expression for a Spark column."""

    canonical_value = _canonical_column(value)
    payload = F.concat_ws("||", F.lit(_effective_salt(salt)), canonical_value)

    return F.when(canonical_value.isNull(), F.lit(None)).otherwise(F.sha2(payload, 256))


def build_pseudo_id_column(
    *values: Column,
    salt: str | None = LOCAL_DEV_SALT,
) -> Column:
    """Build a stable pseudonymous customer ID expression from available identifiers."""

    canonical_values = [_canonical_column(value) for value in values]

    has_any_value = reduce(
        lambda left, right: left | right,
        [value.isNotNull() for value in canonical_values],
    )

    payload = F.concat_ws(
        "||",
        F.lit(_effective_salt(salt)),
        *[F.coalesce(value, F.lit("")) for value in canonical_values],
    )

    pseudo_hash = F.sha2(payload, 256)

    return F.when(
        has_any_value,
        F.concat(F.lit("cust_"), F.substring(pseudo_hash, 1, 24)),
    ).otherwise(F.lit(None))
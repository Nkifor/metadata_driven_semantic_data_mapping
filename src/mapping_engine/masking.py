"""Deterministic pseudonymization helpers.

Do not store production salts or pepper values in this repository. In production, inject them
from Databricks secret scopes backed by Azure Key Vault or an equivalent secret manager.
"""

import hashlib
import hmac

from mapping_engine.standardization import normalize_email, normalize_phone, normalize_whitespace

LOCAL_DEV_SALT = "LOCAL_DEV_ONLY_REPLACE_WITH_SECRET_SCOPE_VALUE"


def _canonical(value: str | None) -> str | None:
    value = normalize_whitespace(value)
    return value.lower() if value else None


def hash_identifier(value: str | None, *, salt: str = LOCAL_DEV_SALT) -> str | None:
    """Return a deterministic SHA-256 HMAC for an identifier.

    HMAC is used instead of plain SHA-256 so the hash is not easily brute-forced without the salt.
    """

    canonical = _canonical(value)
    if canonical is None:
        return None
    return hmac.new(salt.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def hash_email(value: str | None, *, salt: str = LOCAL_DEV_SALT) -> str | None:
    """Normalize and hash email."""

    return hash_identifier(normalize_email(value), salt=salt)


def hash_phone(value: str | None, *, salt: str = LOCAL_DEV_SALT) -> str | None:
    """Normalize and hash phone number."""

    return hash_identifier(normalize_phone(value), salt=salt)


def build_customer_pseudo_id(*identifiers: str | None, salt: str = LOCAL_DEV_SALT) -> str | None:
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

"""Similarity scoring utilities for entity resolution."""

from difflib import SequenceMatcher

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - fallback for very small local environments
    fuzz = None

from mapping_engine.standardization import (
    normalize_name,
    normalize_postal_code,
    normalize_whitespace,
)


def text_similarity(left: str | None, right: str | None) -> float:
    """Return a 0..1 similarity score for two text values."""

    left_norm = normalize_whitespace(left)
    right_norm = normalize_whitespace(right)
    if not left_norm or not right_norm:
        return 0.0
    if fuzz is not None:
        return fuzz.token_sort_ratio(left_norm, right_norm) / 100.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def name_similarity(left: str | None, right: str | None) -> float:
    """Return a name-specific 0..1 similarity score."""

    return text_similarity(normalize_name(left), normalize_name(right))


def exact_or_zero(left: str | None, right: str | None) -> float:
    """Return 1.0 for equal non-empty normalized values, otherwise 0.0."""

    left_norm = normalize_whitespace(left)
    right_norm = normalize_whitespace(right)
    return 1.0 if left_norm and right_norm and left_norm == right_norm else 0.0


def postal_code_similarity(left: str | None, right: str | None) -> float:
    """Return exact-match postal code similarity."""

    return exact_or_zero(normalize_postal_code(left), normalize_postal_code(right))


def weighted_customer_similarity(
    *,
    name_score: float,
    date_of_birth_score: float,
    postal_code_score: float,
    address_score: float,
) -> float:
    """Combine v1 fuzzy matching features into a single 0..1 score."""

    weights = {
        "name": 0.45,
        "date_of_birth": 0.25,
        "postal_code": 0.15,
        "address": 0.15,
    }
    score = (
        name_score * weights["name"]
        + date_of_birth_score * weights["date_of_birth"]
        + postal_code_score * weights["postal_code"]
        + address_score * weights["address"]
    )
    return round(max(0.0, min(1.0, score)), 4)


def classify_match(score: float,
    *, 
    match_threshold: float = 0.92,
    review_threshold: float = 0.75
) -> str:
    """Classify a pair based on similarity score."""

    if score >= match_threshold:
        return "MATCH"
    if score >= review_threshold:
        return "REVIEW_REQUIRED"
    return "NO_MATCH"

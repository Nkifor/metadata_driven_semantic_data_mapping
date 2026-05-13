"""Semantic mapping helpers for source columns and business concepts."""

from dataclasses import dataclass

from mapping_engine.similarity import text_similarity


@dataclass(frozen=True)
class SemanticCandidate:
    """Candidate mapping between a source column and a semantic concept."""

    source_system: str
    source_column: str
    semantic_concept: str
    score: float
    decision: str


def score_column_to_concept(source_column: str, semantic_concept: str) -> float:
    """Score a source column name against one semantic concept name."""

    normalized_source = source_column.replace("_", " ")
    normalized_concept = semantic_concept.replace("_", " ")
    return round(text_similarity(normalized_source, normalized_concept), 4)


def classify_semantic_score(score: float) -> str:
    """Classify semantic mapping confidence."""

    if score >= 0.9:
        return "AUTO_APPROVE"
    if score >= 0.7:
        return "REVIEW"
    return "REJECT"

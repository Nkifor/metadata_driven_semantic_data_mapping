from mapping_engine.similarity import classify_match, name_similarity, weighted_customer_similarity


def test_name_similarity_handles_accents() -> None:
    assert name_similarity("Maria Zielińska", "Maria Zielinska") > 0.95


def test_weighted_customer_similarity_bounds_score() -> None:
    score = weighted_customer_similarity(
        name_score=1.0,
        date_of_birth_score=1.0,
        postal_code_score=1.0,
        address_score=1.0,
    )
    assert score == 1.0


def test_classify_match() -> None:
    assert classify_match(0.95) == "MATCH"
    assert classify_match(0.8) == "REVIEW_REQUIRED"
    assert classify_match(0.4) == "NO_MATCH"

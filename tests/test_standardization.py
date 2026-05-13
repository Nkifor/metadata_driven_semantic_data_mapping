from mapping_engine.standardization import (
    normalize_email,
    normalize_name,
    normalize_phone,
    normalize_postal_code,
    normalize_whitespace,
)


def test_normalize_whitespace_collapses_spaces() -> None:
    assert normalize_whitespace("  Anna   Kowalska  ") == "Anna Kowalska"
    assert normalize_whitespace("   ") is None


def test_normalize_email_lowercases() -> None:
    assert normalize_email("  ANNA.KOWALSKA@Example.COM ") == "anna.kowalska@example.com"


def test_normalize_phone_adds_default_polish_country_code() -> None:
    assert normalize_phone("501 100 200") == "+48501100200"
    assert normalize_phone("+48 501 100 200") == "+48501100200"


def test_normalize_name_strips_accents_and_uppercases() -> None:
    assert normalize_name("  Maria  Zielińska ") == "MARIA ZIELINSKA"


def test_normalize_postal_code_removes_spaces() -> None:
    assert normalize_postal_code("00 001") == "00001"

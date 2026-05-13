from mapping_engine.masking import (
    build_customer_pseudo_id,
    hash_email,
    hash_identifier,
    mask_email_for_display,
)


def test_hash_identifier_is_deterministic_and_salted() -> None:
    assert hash_identifier("ABC", salt="s1") == hash_identifier(" abc ", salt="s1")
    assert hash_identifier("ABC", salt="s1") != hash_identifier("ABC", salt="s2")


def test_hash_email_normalizes_before_hashing() -> None:
    assert hash_email("ANNA@example.com", salt="s") == hash_email("anna@example.com", salt="s")


def test_build_customer_pseudo_id_uses_first_available_identifier() -> None:
    pseudo_id = build_customer_pseudo_id(None, "anna@example.com", salt="s")
    assert pseudo_id is not None
    assert pseudo_id.startswith("cust_")


def test_mask_email_for_display() -> None:
    assert mask_email_for_display("anna@example.com") == "an***@example.com"

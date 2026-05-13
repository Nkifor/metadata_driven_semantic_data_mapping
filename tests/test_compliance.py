from mapping_engine.compliance import SuppressionReason, evaluate_processing_eligibility


def test_allows_processing_when_consented_and_not_blocked() -> None:
    result = evaluate_processing_eligibility(
        analytics_consent=True,
        marketing_consent=True,
        has_deletion_request=False,
        retention_expired=False,
        has_valid_identifier=True,
    )

    assert result.can_process_for_analytics is True
    assert result.can_process_for_marketing is True
    assert result.suppression_reasons == ()


def test_blocks_analytics_without_analytics_consent() -> None:
    result = evaluate_processing_eligibility(
        analytics_consent=False,
        marketing_consent=True,
        has_deletion_request=False,
        retention_expired=False,
        has_valid_identifier=True,
    )

    assert result.can_process_for_analytics is False
    assert result.can_process_for_marketing is True
    assert SuppressionReason.NO_ANALYTICS_CONSENT.value in result.suppression_reasons


def test_global_blockers_block_all_processing() -> None:
    result = evaluate_processing_eligibility(
        analytics_consent=True,
        marketing_consent=True,
        has_deletion_request=True,
        retention_expired=False,
        has_valid_identifier=True,
    )

    assert result.can_process_for_analytics is False
    assert result.can_process_for_marketing is False
    assert result.suppression_reason == SuppressionReason.DELETION_REQUESTED.value

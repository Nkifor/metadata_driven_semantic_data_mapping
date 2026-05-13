"""Compliance eligibility rules for GDPR-aware processing."""

from dataclasses import dataclass
from enum import StrEnum


class SuppressionReason(StrEnum):
    """Reasons why a source record cannot be processed for a use case."""

    NO_ANALYTICS_CONSENT = "NO_ANALYTICS_CONSENT"
    NO_MARKETING_CONSENT = "NO_MARKETING_CONSENT"
    DELETION_REQUESTED = "DELETION_REQUESTED"
    RETENTION_EXPIRED = "RETENTION_EXPIRED"
    INVALID_IDENTIFIER = "INVALID_IDENTIFIER"


@dataclass(frozen=True)
class ProcessingEligibility:
    """Decision object written later to compliance.processing_eligibility."""

    can_process_for_analytics: bool
    can_process_for_marketing: bool
    has_deletion_request: bool
    retention_expired: bool
    suppression_reasons: tuple[str, ...]

    @property
    def suppression_reason(self) -> str | None:
        """Compatibility property for v1 tables that keep one reason column."""

        return self.suppression_reasons[0] if self.suppression_reasons else None


def evaluate_processing_eligibility(
    *,
    analytics_consent: bool,
    marketing_consent: bool,
    has_deletion_request: bool,
    retention_expired: bool,
    has_valid_identifier: bool,
) -> ProcessingEligibility:
    """Evaluate record-level processing eligibility.

    Deletion requests, retention expiry, and invalid identifiers are global blockers. Missing
    consent blocks the corresponding processing purpose.
    """

    reasons: list[str] = []

    if has_deletion_request:
        reasons.append(SuppressionReason.DELETION_REQUESTED.value)
    if retention_expired:
        reasons.append(SuppressionReason.RETENTION_EXPIRED.value)
    if not has_valid_identifier:
        reasons.append(SuppressionReason.INVALID_IDENTIFIER.value)
    if not analytics_consent:
        reasons.append(SuppressionReason.NO_ANALYTICS_CONSENT.value)
    if not marketing_consent:
        reasons.append(SuppressionReason.NO_MARKETING_CONSENT.value)

    global_blocked = has_deletion_request or retention_expired or not has_valid_identifier

    return ProcessingEligibility(
        can_process_for_analytics=analytics_consent and not global_blocked,
        can_process_for_marketing=marketing_consent and not global_blocked,
        has_deletion_request=has_deletion_request,
        retention_expired=retention_expired,
        suppression_reasons=tuple(reasons),
    )

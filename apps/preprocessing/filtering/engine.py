from __future__ import annotations

from dataclasses import asdict, replace
from typing import Protocol, Sequence, TypeVar

from apps.preprocessing.language import LanguageAnnotatedDocument

from .anomaly import check_author_burst, check_velocity
from .config import ContentFilterConfig, DEFAULT_CONTENT_FILTER_CONFIG
from .schema import FilterStatus, FilteredDocument


def filter_content(
    document: LanguageAnnotatedDocument,
    config: ContentFilterConfig = DEFAULT_CONTENT_FILTER_CONFIG,
) -> FilteredDocument:
    text = " ".join(document.text.lower().split())
    reasons: list[str] = []
    status = FilterStatus.PASS
    quality_weight = 1.0

    if _is_explicit_spam(text, config):
        reasons.append("spam_signature")
        status = FilterStatus.DROP
        quality_weight = 0.0
    elif _is_short_noise(text, config):
        reasons.append("short_noise")
        status = FilterStatus.DROP
        quality_weight = 0.0
    elif _is_complaint_like(text, config) and not document.is_official:
        reasons.append("complaint_like")
        status = FilterStatus.REVIEW
        quality_weight = config.review_weight

    if status is not FilterStatus.PASS and not reasons:
        reasons.append("manual_review")

    return FilteredDocument(
        **asdict(document),
        filter_status=status,
        filter_reasons=tuple(reasons),
        quality_weight=quality_weight,
    )


class _AnomalyAwareDocument(Protocol):
    doc_id: str
    filter_reasons: tuple[str, ...]
    filter_status: FilterStatus
    quality_weight: float
    anomaly_flags: tuple[str, ...]
    anomaly_confidence: float


TAnomalyAwareDocument = TypeVar("TAnomalyAwareDocument", bound=_AnomalyAwareDocument)


ANOMALY_TYPE_ORDER = (
    "coordinated_flood",
    "near_duplicate_flood",
    "author_burst",
    "bot_timing",
)
DROP_FILTER_REASONS = frozenset({"spam_signature", "short_noise"})


def apply_anomaly_detection(
    documents: Sequence[TAnomalyAwareDocument],
    config: ContentFilterConfig = DEFAULT_CONTENT_FILTER_CONFIG,
) -> list[TAnomalyAwareDocument]:
    if not documents:
        return []

    flags = check_velocity(
        list(documents),
        window_minutes=config.velocity_window_minutes,
        velocity_threshold=config.velocity_threshold,
        near_velocity_threshold=config.near_velocity_threshold,
    ) + check_author_burst(
        list(documents),
        window_minutes=config.author_burst_window_minutes,
        author_burst_threshold=config.author_burst_threshold,
    )

    flags_by_doc: dict[str, list[str]] = {}
    confidence_by_doc: dict[str, float] = {}
    for flag in flags:
        flags_by_doc.setdefault(flag.doc_id, []).append(flag.anomaly_type)
        confidence_by_doc[flag.doc_id] = max(
            confidence_by_doc.get(flag.doc_id, 0.0),
            flag.confidence,
        )

    updated_documents: list[TAnomalyAwareDocument] = []
    for document in documents:
        base_status, base_quality_weight = derive_filter_baseline(
            filter_reasons=document.filter_reasons,
            config=config,
        )
        anomaly_types = _ordered_anomaly_types(flags_by_doc.get(document.doc_id, ()))
        anomaly_confidence = confidence_by_doc.get(document.doc_id, 0.0)
        status = base_status
        quality_weight = base_quality_weight
        if anomaly_types and base_status is not FilterStatus.DROP:
            status = FilterStatus.REVIEW
            quality_weight = max(0.0, base_quality_weight * (1.0 - anomaly_confidence))

        updated_documents.append(
            replace(
                document,
                filter_status=status,
                quality_weight=quality_weight,
                anomaly_flags=anomaly_types,
                anomaly_confidence=anomaly_confidence,
            ),
        )

    return updated_documents


def derive_filter_baseline(
    *,
    filter_reasons: Sequence[str],
    config: ContentFilterConfig = DEFAULT_CONTENT_FILTER_CONFIG,
) -> tuple[FilterStatus, float]:
    reason_set = {str(reason) for reason in filter_reasons}
    if reason_set & DROP_FILTER_REASONS:
        return FilterStatus.DROP, 0.0
    if reason_set:
        return FilterStatus.REVIEW, config.review_weight
    return FilterStatus.PASS, 1.0


def _is_explicit_spam(text: str, config: ContentFilterConfig) -> bool:
    if not text:
        return True

    for signature in config.spam_signatures:
        if signature in text:
            return True

    return any(pattern in text for pattern in config.ad_hashtag_patterns)


def _is_short_noise(text: str, config: ContentFilterConfig) -> bool:
    if not text:
        return True

    if len(text) >= config.min_text_length:
        return False

    if any(marker in text for marker in config.relevant_short_markers):
        return False

    return text in config.noise_markers or len(text.split()) <= 2


def _is_complaint_like(text: str, config: ContentFilterConfig) -> bool:
    if not text:
        return False

    if len(text) < config.min_meaningful_length:
        return False

    return any(marker in text for marker in config.complaint_markers)


def _ordered_anomaly_types(anomaly_types: Sequence[str]) -> tuple[str, ...]:
    seen = {str(anomaly_type) for anomaly_type in anomaly_types if str(anomaly_type)}
    ordered = [anomaly_type for anomaly_type in ANOMALY_TYPE_ORDER if anomaly_type in seen]
    ordered.extend(
        sorted(
            anomaly_type
            for anomaly_type in seen
            if anomaly_type not in ANOMALY_TYPE_ORDER
        ),
    )
    return tuple(ordered)

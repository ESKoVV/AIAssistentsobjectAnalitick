from __future__ import annotations

from dataclasses import asdict

from apps.preprocessing.language import LanguageAnnotatedDocument

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

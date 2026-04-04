from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, Sequence


BOT_TIMING_MAX_INTERVAL_SECONDS = 30


class _AnomalyDocument(Protocol):
    doc_id: str
    created_at: datetime
    author_id: str
    text_sha256: str
    duplicate_group_id: str
    near_duplicate_flag: bool


@dataclass(frozen=True, slots=True)
class AnomalyFlag:
    doc_id: str
    anomaly_type: str
    group_size: int
    window_start: datetime
    window_end: datetime
    confidence: float


def check_velocity(
    documents: list[_AnomalyDocument] | tuple[_AnomalyDocument, ...],
    window_minutes: int = 30,
    *,
    velocity_threshold: int = 20,
    near_velocity_threshold: int = 15,
) -> list[AnomalyFlag]:
    if not documents:
        return []

    window = timedelta(minutes=window_minutes)
    grouped_by_sha = _group_by(documents, lambda document: document.text_sha256)
    grouped_by_near_duplicate = _group_by(
        [document for document in documents if document.near_duplicate_flag],
        lambda document: document.duplicate_group_id,
    )

    exact_flags = _collect_window_flags(
        grouped_by_sha,
        window=window,
        threshold=velocity_threshold,
        anomaly_type="coordinated_flood",
        confidence_factory=lambda group_size: _scaled_confidence(
            group_size=group_size,
            threshold=velocity_threshold,
        ),
    )
    near_flags = _collect_window_flags(
        grouped_by_near_duplicate,
        window=window,
        threshold=near_velocity_threshold,
        anomaly_type="near_duplicate_flood",
        confidence_factory=lambda group_size: _scaled_confidence(
            group_size=group_size,
            threshold=near_velocity_threshold,
        ),
    )

    return _merge_flags(exact_flags + near_flags)


def check_author_burst(
    documents: list[_AnomalyDocument] | tuple[_AnomalyDocument, ...],
    window_minutes: int = 60,
    *,
    author_burst_threshold: int = 10,
) -> list[AnomalyFlag]:
    if not documents:
        return []

    window = timedelta(minutes=window_minutes)
    grouped_by_author = _group_by(documents, lambda document: document.author_id)
    burst_flags = _collect_window_flags(
        grouped_by_author,
        window=window,
        threshold=author_burst_threshold,
        anomaly_type="author_burst",
        confidence_factory=lambda group_size: _scaled_confidence(
            group_size=group_size,
            threshold=author_burst_threshold,
        ),
    )
    bot_timing_flags = _collect_window_flags(
        grouped_by_author,
        window=window,
        threshold=author_burst_threshold,
        anomaly_type="bot_timing",
        confidence_factory=lambda group_size: 0.95,
        predicate=_all_intervals_below_threshold,
    )

    return _merge_flags(burst_flags + bot_timing_flags)


def _group_by(
    documents: Sequence[_AnomalyDocument],
    key_factory,
) -> dict[str, list[_AnomalyDocument]]:  # type: ignore[no-untyped-def]
    grouped: dict[str, list[_AnomalyDocument]] = defaultdict(list)
    for document in documents:
        grouped[str(key_factory(document))].append(document)
    return grouped


def _collect_window_flags(
    grouped_documents: dict[str, list[_AnomalyDocument]],
    *,
    window: timedelta,
    threshold: int,
    anomaly_type: str,
    confidence_factory,
    predicate=None,
) -> list[AnomalyFlag]:  # type: ignore[no-untyped-def]
    flags: list[AnomalyFlag] = []
    for documents in grouped_documents.values():
        ordered_documents = sorted(
            documents,
            key=lambda document: (document.created_at, document.doc_id),
        )
        left = 0
        for right, _ in enumerate(ordered_documents):
            while ordered_documents[right].created_at - ordered_documents[left].created_at > window:
                left += 1
            window_documents = ordered_documents[left : right + 1]
            group_size = len(window_documents)
            if group_size < threshold:
                continue
            if predicate is not None and not predicate(window_documents):
                continue
            window_start = window_documents[0].created_at
            window_end = window_documents[-1].created_at
            confidence = float(confidence_factory(group_size))
            for document in window_documents:
                flags.append(
                    AnomalyFlag(
                        doc_id=document.doc_id,
                        anomaly_type=anomaly_type,
                        group_size=group_size,
                        window_start=window_start,
                        window_end=window_end,
                        confidence=confidence,
                    ),
                )
    return flags


def _all_intervals_below_threshold(documents: Sequence[_AnomalyDocument]) -> bool:
    if len(documents) < 2:
        return False

    ordered_documents = sorted(
        documents,
        key=lambda document: (document.created_at, document.doc_id),
    )
    return all(
        (
            later.created_at - earlier.created_at
        ).total_seconds()
        < BOT_TIMING_MAX_INTERVAL_SECONDS
        for earlier, later in zip(ordered_documents, ordered_documents[1:])
    )


def _scaled_confidence(*, group_size: int, threshold: int) -> float:
    baseline = 0.8
    if threshold <= 0:
        return 1.0
    scaled = baseline + 0.2 * float(group_size - threshold) / float(threshold)
    return max(0.0, min(1.0, scaled))


def _merge_flags(flags: Sequence[AnomalyFlag]) -> list[AnomalyFlag]:
    best_flags: dict[tuple[str, str], AnomalyFlag] = {}
    for flag in flags:
        key = (flag.doc_id, flag.anomaly_type)
        current = best_flags.get(key)
        if current is None or _is_stronger(flag, current):
            best_flags[key] = flag
    return sorted(
        best_flags.values(),
        key=lambda flag: (flag.window_start, flag.window_end, flag.doc_id, flag.anomaly_type),
    )


def _is_stronger(left: AnomalyFlag, right: AnomalyFlag) -> bool:
    return (
        left.group_size,
        left.confidence,
        left.window_end,
        left.window_start,
        left.doc_id,
        left.anomaly_type,
    ) > (
        right.group_size,
        right.confidence,
        right.window_end,
        right.window_start,
        right.doc_id,
        right.anomaly_type,
    )

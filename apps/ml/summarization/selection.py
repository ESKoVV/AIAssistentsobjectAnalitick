from __future__ import annotations

import math
from collections import Counter
from typing import Sequence

from apps.ml.clustering.schema import Cluster

from .schema import SummarizationDocumentRecord


def select_representative_docs(
    cluster: Cluster,
    documents: Sequence[SummarizationDocumentRecord],
    *,
    max_docs: int = 40,
    max_tokens: int = 6000,
    max_docs_per_author: int = 3,
    max_doc_chars: int = 500,
) -> list[SummarizationDocumentRecord]:
    if max_docs <= 0 or max_tokens <= 0 or not documents:
        return []

    cluster_doc_ids = set(cluster.doc_ids)
    cluster_docs = [
        document
        for document in documents
        if not cluster_doc_ids or document.doc_id in cluster_doc_ids
    ]
    if not cluster_docs:
        return []

    sorted_docs = sorted(
        cluster_docs,
        key=lambda document: _dot_product(document.embedding, cluster.centroid),
        reverse=True,
    )

    selected: list[SummarizationDocumentRecord] = []
    author_counts: Counter[str] = Counter()
    total_tokens = 0

    for document in sorted_docs:
        if author_counts[document.author_id] >= max_docs_per_author:
            continue

        prompt_text = truncate_prompt_text(document.text, max_chars=max_doc_chars)
        prompt_tokens = estimate_tokens(prompt_text)
        if selected and total_tokens + prompt_tokens > max_tokens:
            break
        if not selected and prompt_tokens > max_tokens:
            selected.append(document)
            break

        selected.append(document)
        author_counts[document.author_id] += 1
        total_tokens += prompt_tokens
        if len(selected) >= max_docs:
            break

    return selected


def render_selected_texts(
    documents: Sequence[SummarizationDocumentRecord],
    *,
    max_doc_chars: int = 500,
) -> str:
    blocks = [
        f"[{document.source_type.value}, {document.created_at.date().isoformat()}] "
        f"{truncate_prompt_text(document.text, max_chars=max_doc_chars)}"
        for document in documents
    ]
    return "\n---\n".join(blocks)


def estimate_tokens(text: str) -> int:
    normalized = text.strip()
    if not normalized:
        return 0
    return max(1, math.ceil(len(normalized) / 4))


def truncate_prompt_text(text: str, *, max_chars: int = 500) -> str:
    normalized = text.strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[:max_chars].rstrip()


def _dot_product(left: Sequence[float], right: Sequence[float]) -> float:
    return float(sum(float(a) * float(b) for a, b in zip(left, right)))

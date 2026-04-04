from __future__ import annotations

from tests.helpers import build_cluster, build_summarization_document_record

from apps.ml.summarization.selection import select_representative_docs


def test_representative_selection_orders_by_centroid_similarity_and_limits_author_share() -> None:
    cluster = build_cluster(
        doc_ids=["doc-1", "doc-2", "doc-3", "doc-4", "doc-5"],
        centroid=[1.0, 0.0],
    )
    documents = [
        build_summarization_document_record(doc_id="doc-1", author_id="author-a", embedding=[1.0, 0.0]),
        build_summarization_document_record(doc_id="doc-2", author_id="author-a", embedding=[0.99, 0.01]),
        build_summarization_document_record(doc_id="doc-3", author_id="author-a", embedding=[0.98, 0.02]),
        build_summarization_document_record(doc_id="doc-4", author_id="author-a", embedding=[0.97, 0.03]),
        build_summarization_document_record(doc_id="doc-5", author_id="author-b", embedding=[0.96, 0.04]),
    ]

    selected = select_representative_docs(
        cluster,
        documents,
        max_docs=10,
        max_tokens=6000,
        max_docs_per_author=3,
    )

    assert [document.doc_id for document in selected] == ["doc-1", "doc-2", "doc-3", "doc-5"]


def test_representative_selection_respects_prompt_token_budget() -> None:
    cluster = build_cluster(doc_ids=["doc-1", "doc-2"], centroid=[1.0, 0.0])
    documents = [
        build_summarization_document_record(
            doc_id="doc-1",
            text=" ".join(["вода"] * 120),
            embedding=[1.0, 0.0],
        ),
        build_summarization_document_record(
            doc_id="doc-2",
            text="короткое сообщение о подаче воды",
            embedding=[0.99, 0.01],
        ),
    ]

    selected = select_representative_docs(
        cluster,
        documents,
        max_docs=10,
        max_tokens=40,
        max_docs_per_author=3,
    )

    assert [document.doc_id for document in selected] == ["doc-1"]

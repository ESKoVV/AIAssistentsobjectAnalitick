from __future__ import annotations

import math

import pytest

from apps.ml.embeddings.config import EmbeddingServiceConfig
from apps.ml.embeddings.inference import EmbeddingPipeline
from apps.ml.embeddings.preparation import chunk_token_windows, prepare_document, prepare_text
from apps.ml.embeddings.service import EmbeddingBatchService
from apps.ml.embeddings.spool import SQLiteEmbeddingSpool
from apps.ml.embeddings.storage import InMemoryEmbeddingCache, InMemoryEmbeddingRepository
from tests.helpers import build_enriched_document


class FakeTokenizer:
    def __init__(self) -> None:
        self._token_to_id: dict[str, int] = {}
        self._id_to_token: dict[int, str] = {}
        self._next_id = 1

    def encode(self, text: str, *, add_special_tokens: bool = False) -> list[int]:
        del add_special_tokens
        token_ids: list[int] = []
        for token in text.split():
            if token not in self._token_to_id:
                self._token_to_id[token] = self._next_id
                self._id_to_token[self._next_id] = token
                self._next_id += 1
            token_ids.append(self._token_to_id[token])
        return token_ids

    def decode(self, token_ids: list[int], *, skip_special_tokens: bool = True) -> str:
        del skip_special_tokens
        return " ".join(self._id_to_token[token_id] for token_id in token_ids)


class FakeEmbeddingBackend:
    def __init__(self, mapping: dict[str, list[float]]) -> None:
        self._mapping = mapping

    def encode(self, texts: list[str], *, batch_size: int) -> list[list[float]]:
        assert batch_size > 0
        return [list(self._mapping[text]) for text in texts]


class ExplodingCache:
    def cache_embeddings(self, documents):  # type: ignore[no-untyped-def]
        raise RuntimeError("redis is down")


class FailingRepository:
    def ensure_model_compatibility(self, *, model_name: str, model_version: str) -> None:
        return None

    def upsert_embeddings(self, documents):  # type: ignore[no-untyped-def]
        raise ConnectionError("postgres is down")


def test_prepare_text_adds_passage_prefix_and_source_context() -> None:
    document = build_enriched_document("Смотрите https://example.test @city_admin 🚒")

    prepared = prepare_text(document)

    assert prepared.startswith("passage: [vk_post] ")
    assert "https://example.test" not in prepared
    assert "@city_admin" not in prepared
    assert "🚒" not in prepared


def test_chunk_token_windows_and_prepare_document_track_token_count_and_truncation() -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two three four five six seven eight")

    prepared = prepare_document(document, tokenizer, max_tokens=4, overlap=1)
    direct_windows = chunk_token_windows(prepared.token_ids, max_tokens=4, overlap=1)

    assert prepared.token_count == len(prepared.token_ids)
    assert prepared.truncated is True
    assert len(prepared.chunks) == len(direct_windows) >= 2


def test_pipeline_mean_pools_chunks_and_normalizes_final_vector() -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two three four five six")
    prepared = prepare_document(document, tokenizer, max_tokens=4, overlap=0)
    mapping = {
        prepared.chunks[0]: [2.0, 0.0],
        prepared.chunks[1]: [0.0, 2.0],
    }
    pipeline = EmbeddingPipeline(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="hash-1",
            batch_size=2,
            max_tokens=4,
            chunk_overlap=0,
            embedding_dimension=2,
        ),
        tokenizer=tokenizer,
        backend=FakeEmbeddingBackend(mapping),
    )

    result = pipeline.embed_documents([document])
    embedding = result.documents[0].embedding

    assert embedding[0] == pytest.approx(1 / math.sqrt(2))
    assert embedding[1] == pytest.approx(1 / math.sqrt(2))
    assert result.metrics.raw_norm_mean == pytest.approx(math.sqrt(2))
    assert result.documents[0].truncated is True


def test_model_compatibility_guard_fails_for_mismatched_versions() -> None:
    repository = InMemoryEmbeddingRepository()
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two")
    prepared = prepare_document(document, tokenizer, max_tokens=16, overlap=0)
    existing_document = EmbeddingPipeline(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="old-hash",
            embedding_dimension=2,
        ),
        tokenizer=tokenizer,
        backend=FakeEmbeddingBackend({prepared.chunks[0]: [1.0, 0.0]}),
    ).embed_documents([document]).documents[0]
    repository.upsert_embeddings([existing_document])

    with pytest.raises(RuntimeError, match="different model version"):
        repository.ensure_model_compatibility(
            model_name="intfloat/multilingual-e5-large",
            model_version="new-hash",
        )


def test_cache_failures_do_not_fail_batch(tmp_path) -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two")
    prepared = prepare_document(document, tokenizer, max_tokens=16, overlap=0)
    pipeline = EmbeddingPipeline(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="hash-1",
            embedding_dimension=2,
        ),
        tokenizer=tokenizer,
        backend=FakeEmbeddingBackend({prepared.chunks[0]: [3.0, 4.0]}),
    )
    service = EmbeddingBatchService(
        pipeline=pipeline,
        repository=InMemoryEmbeddingRepository(),
        cache=ExplodingCache(),
        spool=SQLiteEmbeddingSpool(str(tmp_path / "spool.sqlite3")),
    )

    result = service.process_batch([document])

    assert result.buffered is False
    assert len(result.documents) == 1


def test_repository_failures_buffer_documents_locally(tmp_path) -> None:
    tokenizer = FakeTokenizer()
    document = build_enriched_document("one two")
    prepared = prepare_document(document, tokenizer, max_tokens=16, overlap=0)
    spool = SQLiteEmbeddingSpool(str(tmp_path / "spool.sqlite3"))
    pipeline = EmbeddingPipeline(
        config=EmbeddingServiceConfig(
            model_name="intfloat/multilingual-e5-large",
            model_version="hash-1",
            embedding_dimension=2,
        ),
        tokenizer=tokenizer,
        backend=FakeEmbeddingBackend({prepared.chunks[0]: [3.0, 4.0]}),
    )
    service = EmbeddingBatchService(
        pipeline=pipeline,
        repository=FailingRepository(),
        cache=InMemoryEmbeddingCache(),
        spool=spool,
    )

    result = service.process_batch([document])

    assert result.buffered is True
    assert spool.size() == 1

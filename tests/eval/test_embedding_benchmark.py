from __future__ import annotations

import os
import time

import pytest

from apps.ml.embeddings import EmbeddingServiceConfig, TransformerEmbeddingBackend
from apps.ml.embeddings.inference import EmbeddingPipeline
from tests.helpers import build_enriched_document


@pytest.mark.skipif(
    os.getenv("RUN_EMBEDDINGS_BENCHMARK") != "1",
    reason="Set RUN_EMBEDDINGS_BENCHMARK=1 to run the GPU smoke benchmark",
)
def test_gpu_batch_latency_smoke() -> None:
    torch = pytest.importorskip("torch")
    pytest.importorskip("transformers")
    if not torch.cuda.is_available():
        pytest.skip("CUDA GPU is not available")

    model_name = os.getenv("EMBEDDINGS_MODEL_NAME", "intfloat/multilingual-e5-large")
    model_version = os.getenv("EMBEDDINGS_MODEL_VERSION")
    if not model_version:
        pytest.skip("EMBEDDINGS_MODEL_VERSION must pin the checkpoint hash for the smoke benchmark")

    backend = TransformerEmbeddingBackend(
        model_name=model_name,
        model_version=model_version,
        device="cuda",
    )
    pipeline = EmbeddingPipeline(
        config=EmbeddingServiceConfig(
            model_name=model_name,
            model_version=model_version,
            device="cuda",
            batch_size=32,
            embedding_dimension=1024,
        ),
        tokenizer=backend.tokenizer,
        backend=backend,
    )
    documents = [
        build_enriched_document(f"Жалоба номер {index}: нет горячей воды во дворе")
        for index in range(32)
    ]

    started_at = time.perf_counter()
    pipeline.embed_documents(documents)
    latency_s = time.perf_counter() - started_at

    assert latency_s < 2.0

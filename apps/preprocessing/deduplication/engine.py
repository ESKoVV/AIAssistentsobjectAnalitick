from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import asdict, dataclass

from apps.preprocessing.cleaning import CleanedDocument

from .schema import DeduplicatedDocument


@dataclass(frozen=True, slots=True)
class DeduplicationConfig:
    shingle_size: int = 5
    num_perm: int = 64
    bands: int = 16
    near_duplicate_threshold: float = 0.72


DEFAULT_DEDUPLICATION_CONFIG = DeduplicationConfig()


def deduplicate_documents(
    documents: list[CleanedDocument] | tuple[CleanedDocument, ...],
    *,
    config: DeduplicationConfig = DEFAULT_DEDUPLICATION_CONFIG,
) -> list[DeduplicatedDocument]:
    if not documents:
        return []

    _validate_config(config)

    text_hashes = [_compute_text_sha256(document.normalized_text) for document in documents]
    shingles = [_build_shingles(document.normalized_text, config.shingle_size) for document in documents]
    signatures = [_compute_minhash_signature(doc_shingles, config.num_perm) for doc_shingles in shingles]

    clusters = _UnionFind(len(documents))
    near_duplicate_indices: set[int] = set()

    for duplicate_indices in _group_exact_duplicates(text_hashes).values():
        first_index = duplicate_indices[0]
        for duplicate_index in duplicate_indices[1:]:
            clusters.union(first_index, duplicate_index)

    for left_index, right_index in _collect_lsh_candidate_pairs(signatures, config.bands):
        if text_hashes[left_index] == text_hashes[right_index]:
            continue

        similarity = _jaccard_similarity(shingles[left_index], shingles[right_index])
        if similarity >= config.near_duplicate_threshold:
            clusters.union(left_index, right_index)
            near_duplicate_indices.add(left_index)
            near_duplicate_indices.add(right_index)

    grouped_indices = _group_cluster_indices(clusters, len(documents))
    cluster_metadata = _build_cluster_metadata(documents, grouped_indices, near_duplicate_indices)

    deduplicated_documents: list[DeduplicatedDocument] = []
    for index, document in enumerate(documents):
        metadata = cluster_metadata[clusters.find(index)]
        deduplicated_documents.append(
            DeduplicatedDocument(
                **asdict(document),
                text_sha256=text_hashes[index],
                duplicate_group_id=metadata["duplicate_group_id"],
                near_duplicate_flag=metadata["near_duplicate_flag"],
                duplicate_cluster_size=metadata["duplicate_cluster_size"],
                canonical_doc_id=metadata["canonical_doc_id"],
            ),
        )

    return deduplicated_documents


def _validate_config(config: DeduplicationConfig) -> None:
    if config.shingle_size <= 0:
        raise ValueError("shingle_size must be positive")
    if config.num_perm <= 0:
        raise ValueError("num_perm must be positive")
    if config.bands <= 0 or config.num_perm % config.bands != 0:
        raise ValueError("bands must evenly divide num_perm")
    if not 0.0 <= config.near_duplicate_threshold <= 1.0:
        raise ValueError("near_duplicate_threshold must be between 0 and 1")


def _compute_text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _build_shingles(text: str, shingle_size: int) -> set[str]:
    normalized_text = " ".join(text.casefold().split())
    if not normalized_text:
        return {""}
    if len(normalized_text) <= shingle_size:
        return {normalized_text}
    return {
        normalized_text[index : index + shingle_size]
        for index in range(len(normalized_text) - shingle_size + 1)
    }


def _compute_minhash_signature(shingles: set[str], num_perm: int) -> tuple[int, ...]:
    return tuple(
        min(_hash_shingle_with_seed(shingle, seed) for shingle in shingles)
        for seed in range(num_perm)
    )


def _hash_shingle_with_seed(shingle: str, seed: int) -> int:
    payload = f"{seed}:{shingle}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def _group_exact_duplicates(text_hashes: list[str]) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, text_hash in enumerate(text_hashes):
        groups[text_hash].append(index)
    return groups


def _collect_lsh_candidate_pairs(
    signatures: list[tuple[int, ...]],
    bands: int,
) -> set[tuple[int, int]]:
    band_size = len(signatures[0]) // bands
    buckets: dict[tuple[int, tuple[int, ...]], list[int]] = defaultdict(list)
    candidate_pairs: set[tuple[int, int]] = set()

    for index, signature in enumerate(signatures):
        for band_index in range(bands):
            start = band_index * band_size
            end = start + band_size
            bucket_key = (band_index, signature[start:end])
            bucket = buckets[bucket_key]
            for other_index in bucket:
                candidate_pairs.add((other_index, index))
            bucket.append(index)

    return candidate_pairs


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def _group_cluster_indices(clusters: "_UnionFind", size: int) -> dict[int, list[int]]:
    grouped_indices: dict[int, list[int]] = defaultdict(list)
    for index in range(size):
        grouped_indices[clusters.find(index)].append(index)
    return grouped_indices


def _build_cluster_metadata(
    documents: list[CleanedDocument] | tuple[CleanedDocument, ...],
    grouped_indices: dict[int, list[int]],
    near_duplicate_indices: set[int],
) -> dict[int, dict[str, str | bool | int]]:
    metadata: dict[int, dict[str, str | bool | int]] = {}

    for root_index, cluster_indices in grouped_indices.items():
        canonical_index = min(cluster_indices)
        canonical_doc_id = documents[canonical_index].doc_id
        metadata[root_index] = {
            "duplicate_group_id": f"dup:{canonical_doc_id}",
            "near_duplicate_flag": any(index in near_duplicate_indices for index in cluster_indices),
            "duplicate_cluster_size": len(cluster_indices),
            "canonical_doc_id": canonical_doc_id,
        }

    return metadata


class _UnionFind:
    def __init__(self, size: int) -> None:
        self._parent = list(range(size))
        self._rank = [0] * size

    def find(self, index: int) -> int:
        if self._parent[index] != index:
            self._parent[index] = self.find(self._parent[index])
        return self._parent[index]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return

        if self._rank[left_root] < self._rank[right_root]:
            self._parent[left_root] = right_root
            return
        if self._rank[left_root] > self._rank[right_root]:
            self._parent[right_root] = left_root
            return

        self._parent[right_root] = left_root
        self._rank[left_root] += 1

from __future__ import annotations

import os
from functools import lru_cache

from .schema import TaxonomyCategory, TaxonomyConfig
from .storage import load_taxonomy_payload


DEFAULT_TAXONOMY_CONFIG_PATH = "configs/taxonomy.yaml"


@lru_cache(maxsize=4)
def load_taxonomy_config(path: str | None = None) -> TaxonomyConfig:
    config_path = path or os.getenv("CLASSIFICATION_TAXONOMY_PATH", DEFAULT_TAXONOMY_CONFIG_PATH)
    payload = load_taxonomy_payload(config_path)
    raw_categories = payload.get("categories")
    if not isinstance(raw_categories, dict) or not raw_categories:
        raise ValueError("taxonomy config must define a non-empty 'categories' mapping")

    categories: dict[str, TaxonomyCategory] = {}
    for key, value in raw_categories.items():
        if not isinstance(value, dict):
            raise ValueError(f"taxonomy category '{key}' must be a mapping")
        label = str(value.get("label", "")).strip()
        if not label:
            raise ValueError(f"taxonomy category '{key}' must define a non-empty label")
        raw_keywords = value.get("keywords", ())
        if not isinstance(raw_keywords, (list, tuple)):
            raise ValueError(f"taxonomy category '{key}' keywords must be a sequence")
        categories[str(key)] = TaxonomyCategory(
            key=str(key),
            label=label,
            keywords=tuple(str(item).strip().lower() for item in raw_keywords if str(item).strip()),
        )

    if "other" not in categories:
        raise ValueError("taxonomy config must define the 'other' category")

    return TaxonomyConfig(categories=categories)

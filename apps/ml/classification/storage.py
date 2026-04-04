from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_taxonomy_payload(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"taxonomy config not found: {path}")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("taxonomy config root must be a mapping")
    return dict(payload)

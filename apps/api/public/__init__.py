from __future__ import annotations

from typing import Any

__all__ = ["app", "create_app"]


def create_app(*args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
    from .server import create_app as _create_app

    return _create_app(*args, **kwargs)


def __getattr__(name: str) -> Any:
    if name == "app":
        from .server import app

        return app
    raise AttributeError(name)

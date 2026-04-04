from __future__ import annotations

import asyncio
import os

from .run_clustering_full import _main as run_once


async def _loop() -> None:
    interval_seconds = int(os.getenv("CLUSTERING_FULL_INTERVAL_SECONDS", str(6 * 60 * 60)))
    if interval_seconds <= 0:
        raise RuntimeError("CLUSTERING_FULL_INTERVAL_SECONDS must be positive")

    while True:
        await run_once()
        await asyncio.sleep(interval_seconds)


def main() -> None:
    asyncio.run(_loop())


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import datetime, timezone

from apps.ml.clustering import ClusteringServiceConfig, ClustersUpdatedEvent, serialize_payload
from apps.orchestration.schedulers.clustering_jobs import build_default_clustering_service

from .kafka_runtime import create_producer


async def _main() -> None:
    config = ClusteringServiceConfig.from_env()
    if not config.postgres_dsn:
        raise RuntimeError("CLUSTERING_POSTGRES_DSN must be configured")
    bootstrap_servers = os.getenv("CLUSTERING_KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        raise RuntimeError("CLUSTERING_KAFKA_BOOTSTRAP_SERVERS must be configured")

    producer = await create_producer(bootstrap_servers=bootstrap_servers)
    try:
        service = build_default_clustering_service(config)
        result = service.run_online_cycle()
        changed_cluster_ids = sorted({cluster.cluster_id for cluster in result.updated_clusters if not cluster.noise})
        if changed_cluster_ids:
            now = datetime.now(timezone.utc)
            event = ClustersUpdatedEvent(
                run_at=now,
                period_start=now,
                period_end=now,
                changed_cluster_ids=changed_cluster_ids,
                mode="online_update",
            )
            await producer.send_and_wait(
                config.updated_topic,
                json.dumps(
                    serialize_payload(event),
                    ensure_ascii=False,
                ).encode("utf-8"),
            )
    finally:
        with contextlib.suppress(Exception):
            await producer.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()

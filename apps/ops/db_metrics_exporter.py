from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


def create_app() -> FastAPI:
    app = FastAPI(title="Regional Analytics DB Metrics Exporter")

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise RuntimeError("DATABASE_URL must be configured")
        snapshot = collect_metrics(database_url)
        return render_prometheus(snapshot)

    return app


def collect_metrics(database_url: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    now = datetime.now(timezone.utc)
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            metrics["pipeline_row_count_raw_messages"] = _count(cur, "raw_messages")
            metrics["pipeline_row_count_normalized_messages"] = _count(cur, "normalized_messages")
            metrics["pipeline_row_count_embeddings"] = _count(cur, "embeddings")
            metrics["pipeline_row_count_clusters"] = _count(cur, "clusters")
            metrics["pipeline_row_count_cluster_descriptions"] = _count(cur, "cluster_descriptions")
            metrics["pipeline_row_count_rankings"] = _count(cur, "rankings")

            metrics["pipeline_freshness_seconds_embeddings"] = _freshness_seconds(
                now,
                _max_timestamp(cur, "SELECT MAX(embedded_at) FROM embeddings"),
            )
            metrics["pipeline_freshness_seconds_cluster_descriptions"] = _freshness_seconds(
                now,
                _max_timestamp(cur, "SELECT MAX(generated_at) FROM cluster_descriptions"),
            )
            metrics["pipeline_freshness_seconds_rankings"] = _freshness_seconds(
                now,
                _max_timestamp(cur, "SELECT MAX(computed_at) FROM rankings"),
            )
        conn.commit()
    return metrics


def render_prometheus(metrics: dict[str, float]) -> str:
    lines = [
        "# HELP pipeline_row_count Number of rows in canonical pipeline tables.",
        "# TYPE pipeline_row_count gauge",
        f'pipeline_row_count{{table="raw_messages"}} {metrics["pipeline_row_count_raw_messages"]}',
        f'pipeline_row_count{{table="normalized_messages"}} {metrics["pipeline_row_count_normalized_messages"]}',
        f'pipeline_row_count{{table="embeddings"}} {metrics["pipeline_row_count_embeddings"]}',
        f'pipeline_row_count{{table="clusters"}} {metrics["pipeline_row_count_clusters"]}',
        f'pipeline_row_count{{table="cluster_descriptions"}} {metrics["pipeline_row_count_cluster_descriptions"]}',
        f'pipeline_row_count{{table="rankings"}} {metrics["pipeline_row_count_rankings"]}',
        "# HELP pipeline_freshness_seconds Age of the freshest successful downstream record.",
        "# TYPE pipeline_freshness_seconds gauge",
        f'pipeline_freshness_seconds{{stage="embeddings"}} {metrics["pipeline_freshness_seconds_embeddings"]}',
        (
            "pipeline_freshness_seconds"
            f'{{stage="cluster_descriptions"}} {metrics["pipeline_freshness_seconds_cluster_descriptions"]}'
        ),
        f'pipeline_freshness_seconds{{stage="rankings"}} {metrics["pipeline_freshness_seconds_rankings"]}',
    ]
    return "\n".join(lines) + "\n"


def _count(cur: psycopg.Cursor, table_name: str) -> float:
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=%s)",
        (table_name,),
    )
    exists = bool(cur.fetchone()[0])
    if not exists:
        return 0.0
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return float(cur.fetchone()[0] or 0)


def _max_timestamp(cur: psycopg.Cursor, query: str) -> datetime | None:
    try:
        cur.execute(query)
    except psycopg.Error:
        return None
    value = cur.fetchone()[0]
    return value if isinstance(value, datetime) else None


def _freshness_seconds(now: datetime, value: datetime | None) -> float:
    if value is None:
        return float(10**9)
    return max(0.0, (now - value.astimezone(timezone.utc)).total_seconds())


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run(
        "apps.ops.db_metrics_exporter:app",
        host=os.getenv("OPS_METRICS_HOST", "0.0.0.0"),
        port=int(os.getenv("OPS_METRICS_PORT", "9108")),
        reload=False,
    )


if __name__ == "__main__":
    main()

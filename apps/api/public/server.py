import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
HOST = os.getenv("API_HOST", "0.0.0.0")
PORT = int(os.getenv("API_PORT", "8000"))


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def _parse_iso_date(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc)


def fetch_documents(filters: dict[str, Any]) -> dict[str, Any]:
    page = max(1, int(filters.get("page") or 1))
    limit = min(100, max(1, int(filters.get("limit") or 20)))

    conditions: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": (page - 1) * limit}

    if filters.get("region"):
        conditions.append("region_hint = %(region)s")
        params["region"] = filters["region"]

    date_from = _parse_iso_date(filters.get("date_from"))
    if date_from:
        conditions.append("created_at >= %(date_from)s")
        params["date_from"] = date_from

    date_to = _parse_iso_date(filters.get("date_to"))
    if date_to:
        conditions.append("created_at <= %(date_to)s")
        params["date_to"] = date_to

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
    SELECT
      doc_id, source_type, source_id, parent_id, text, media_type,
      created_at, collected_at, author_id, is_official,
      reach, likes, reposts, comments_count,
      region_hint, geo_lat, geo_lon, raw_payload
    FROM normalized_documents
    {where_clause}
    ORDER BY created_at DESC
    LIMIT %(limit)s OFFSET %(offset)s;
    """

    count_query = f"SELECT COUNT(*) FROM normalized_documents {where_clause};"

    with psycopg.connect(DATABASE_URL, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            items = cur.fetchall()

            cur.execute(count_query, params)
            total = cur.fetchone()["count"]

    return {"items": items, "total": total, "page": page, "limit": limit}


def fetch_document_by_id(doc_id: str) -> dict[str, Any] | None:
    with psycopg.connect(DATABASE_URL, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  doc_id, source_type, source_id, parent_id, text, media_type,
                  created_at, collected_at, author_id, is_official,
                  reach, likes, reposts, comments_count,
                  region_hint, geo_lat, geo_lon, raw_payload
                FROM normalized_documents
                WHERE doc_id = %(doc_id)s
                LIMIT 1;
                """,
                {"doc_id": doc_id},
            )
            return cur.fetchone()


def fetch_regions() -> list[str]:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT region_hint
                FROM normalized_documents
                WHERE region_hint IS NOT NULL AND region_hint <> ''
                ORDER BY region_hint;
                """
            )
            return [row[0] for row in cur.fetchall()]


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if not DATABASE_URL:
            self._send_json({"detail": "DATABASE_URL is not set"}, status=500)
            return

        parsed = urlparse(self.path)
        path = parsed.path
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        try:
            if path == "/api/documents":
                self._send_json(fetch_documents(params))
                return

            if path.startswith("/api/documents/"):
                doc_id = path.replace("/api/documents/", "", 1)
                if not doc_id:
                    self._send_json({"detail": "doc_id is required"}, status=400)
                    return
                item = fetch_document_by_id(doc_id)
                if item is None:
                    self._send_json({"detail": "Document not found"}, status=404)
                    return
                self._send_json(item)
                return

            if path == "/api/regions":
                self._send_json({"items": fetch_regions()})
                return

            self._send_json({"detail": "Not Found"}, status=404)
        except ValueError as exc:
            self._send_json({"detail": f"Bad request: {exc}"}, status=400)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"detail": f"Internal server error: {exc}"}, status=500)


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"API server running on http://{HOST}:{PORT}")
    server.serve_forever()

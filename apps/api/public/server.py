from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from apps.api.schemas.top import (
    APIError,
    ClusterDetailResponse,
    ClusterDocumentsQueryParams,
    ClusterDocumentsResponse,
    HealthResponse,
    HistoryQueryParams,
    HistoryResponse,
    TimelineResponse,
    TopQueryParams,
    TopResponse,
)

from .auth import AccessLevel, JWTAuthenticator, build_auth_dependency, require_access
from .cache import TopCache
from .config import APIConfig
from .errors import APIException
from .metrics import APIMetricEvent, MetricsRegistry, hash_params
from .repository import PublicAPIRepository
from .service import TopAPIService


logger = logging.getLogger(__name__)


def create_app(config: APIConfig | None = None) -> FastAPI:
    config = config or APIConfig.from_env()
    repository = PublicAPIRepository(
        config.database_url or "",
        documents_table=config.documents_table,
    )
    cache = TopCache(config.redis_dsn, ttl_seconds=config.cache_ttl_seconds)
    service = TopAPIService(repository=repository, cache=cache, config=config)
    metrics = MetricsRegistry()
    authenticator = JWTAuthenticator(config.auth)
    current_user_dependency = build_auth_dependency(authenticator)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        app.state.config = config
        app.state.repository = repository
        app.state.cache = cache
        app.state.service = service
        app.state.metrics = metrics
        invalidation_task = None
        if config.kafka_bootstrap_servers:
            invalidation_task = asyncio.create_task(_run_cache_invalidation_loop(app))
        try:
            yield
        finally:
            if invalidation_task is not None:
                invalidation_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await invalidation_task

    app = FastAPI(
        title="Regional Analytics Public API",
        version=config.api_version,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(config.cors_allow_origins),
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    @app.middleware("http")
    async def request_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = str(uuid4())
        started_at = time.perf_counter()
        request.state.request_id = request_id
        request.state.cache_hit = False
        request.state.user_id = None

        try:
            response = await call_next(request)
        except APIException as exc:
            response = _error_response(
                request=request,
                status_code=exc.status_code,
                error_code=exc.error_code,
                message=exc.message,
            )
        except RequestValidationError as exc:
            response = _error_response(
                request=request,
                status_code=400,
                error_code="invalid_request",
                message=str(exc),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled API error", exc_info=exc)
            response = _error_response(
                request=request,
                status_code=500,
                error_code="internal_error",
                message="Внутренняя ошибка сервиса",
            )

        elapsed_ms = (time.perf_counter() - started_at) * 1000
        response.headers["X-API-Version"] = config.api_version
        response.headers["X-Request-ID"] = request_id
        metrics.record(
            APIMetricEvent(
                endpoint=request.url.path,
                method=request.method,
                status_code=response.status_code,
                response_time_ms=elapsed_ms,
                cache_hit=bool(getattr(request.state, "cache_hit", False)),
                user_id=getattr(request.state, "user_id", None),
                params_hash=hash_params(dict(request.query_params)),
            ),
        )
        logger.info(
            "api_request request_id=%s user_id=%s endpoint=%s params=%s response_time_ms=%.2f status_code=%s cache_hit=%s",
            request_id,
            getattr(request.state, "user_id", None),
            request.url.path,
            dict(request.query_params),
            elapsed_ms,
            response.status_code,
            bool(getattr(request.state, "cache_hit", False)),
        )
        return response

    router = APIRouter(
        prefix="/api/v1",
        dependencies=[Depends(current_user_dependency)],
    )

    @router.get("/top", response_model=TopResponse)
    async def get_top(
        request: Request,
        params: TopQueryParams = Depends(),
        _=Depends(require_access(AccessLevel.VIEWER)),
    ) -> TopResponse:
        response, cache_hit = service.get_top(params)
        request.state.cache_hit = cache_hit
        return response

    @router.get("/top/{cluster_id}", response_model=ClusterDetailResponse)
    async def get_cluster_detail(
        cluster_id: str,
        _=Depends(require_access(AccessLevel.ANALYST)),
    ) -> ClusterDetailResponse:
        return service.get_cluster_detail(cluster_id)

    @router.get("/top/{cluster_id}/documents", response_model=ClusterDocumentsResponse)
    async def get_cluster_documents(
        cluster_id: str,
        params: ClusterDocumentsQueryParams = Depends(),
        _=Depends(require_access(AccessLevel.ANALYST)),
    ) -> ClusterDocumentsResponse:
        return service.get_cluster_documents(cluster_id, params)

    @router.get("/top/{cluster_id}/timeline", response_model=TimelineResponse)
    async def get_cluster_timeline(
        cluster_id: str,
        _=Depends(require_access(AccessLevel.ANALYST)),
    ) -> TimelineResponse:
        return service.get_cluster_timeline(cluster_id)

    @router.get("/history", response_model=HistoryResponse)
    async def get_history(
        params: HistoryQueryParams = Depends(),
        _=Depends(require_access(AccessLevel.ADMIN)),
    ) -> HistoryResponse:
        return service.get_history(params)

    @router.get("/health", response_model=HealthResponse)
    async def get_health(
        _=Depends(require_access(AccessLevel.ADMIN)),
    ) -> HealthResponse:
        return service.get_health()

    app.include_router(router)

    @app.get("/metrics", response_class=PlainTextResponse)
    async def get_metrics(_=Depends(current_user_dependency), __=Depends(require_access(AccessLevel.ADMIN))):  # type: ignore[no-untyped-def]
        return app.state.metrics.render_prometheus()

    return app


def _error_response(
    *,
    request: Request,
    status_code: int,
    error_code: str,
    message: str,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", str(uuid4()))
    payload = APIError(
        error_code=error_code,
        message=message,
        request_id=request_id,
    )
    return JSONResponse(status_code=status_code, content=payload.model_dump())


async def _run_cache_invalidation_loop(app: FastAPI) -> None:
    config: APIConfig = app.state.config
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        logger.warning("aiokafka is not installed; Kafka invalidation loop is disabled")
        return

    consumer = AIOKafkaConsumer(
        config.rankings_updated_topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        group_id="public-api-cache-invalidator",
    )
    await consumer.start()
    try:
        async for _message in consumer:
            await asyncio.to_thread(app.state.service.invalidate_cache)
            await asyncio.to_thread(app.state.service.warm_cache)
    finally:
        await consumer.stop()


try:
    app = create_app()
except Exception as exc:  # noqa: BLE001
    logger.warning("API app bootstrap deferred: %s", exc)
    app = FastAPI(
        title="Regional Analytics Public API",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )


if __name__ == "__main__":
    import uvicorn

    runtime_config = APIConfig.from_env()
    uvicorn.run(
        create_app(runtime_config),
        host=runtime_config.host,
        port=runtime_config.port,
        reload=False,
    )

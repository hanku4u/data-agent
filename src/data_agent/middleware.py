"""Error handling and request logging middleware."""

from __future__ import annotations

import time
import uuid

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .exceptions import (
    DataAgentError,
    SourceNotFoundError,
    SourceValidationError,
    FetchError,
    ChartError,
    ConfigError,
)
from .log import get_logger

logger = get_logger(__name__)

# Map exception types to HTTP status codes
_EXCEPTION_STATUS_MAP: dict[type[DataAgentError], int] = {
    SourceNotFoundError: 404,
    SourceValidationError: 400,
    FetchError: 500,
    ChartError: 400,
    ConfigError: 500,
}


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Add request_id, log request/response with duration."""

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id

        start = time.perf_counter()
        logger.info(
            "request_started",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        response = await call_next(request)

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        response.headers["X-Request-ID"] = request_id

        logger.info(
            "request_completed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )

        return response


async def data_agent_exception_handler(request: Request, exc: DataAgentError) -> JSONResponse:
    """Global exception handler for DataAgentError hierarchy."""
    status_code = _EXCEPTION_STATUS_MAP.get(type(exc), 500)
    request_id = getattr(request.state, "request_id", "unknown")

    logger.error(
        "unhandled_error",
        request_id=request_id,
        error_type=type(exc).__name__,
        error_message=exc.message,
        detail=exc.detail,
    )

    return JSONResponse(
        status_code=status_code,
        content={
            "error": type(exc).__name__,
            "message": exc.message,
            "detail": exc.detail,
            "request_id": request_id,
        },
    )

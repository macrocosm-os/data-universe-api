from __future__ import annotations
import time
import uuid
from typing import Callable, Awaitable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

import structlog

logger = structlog.get_logger(__name__)

# recognize common correlation headers if present
_REQUEST_ID_HEADERS = ("x-request-id", "x-correlation-id")

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    - Ensures every request has a request_id (honors inbound headers).
    - Binds request context (method/path/client_ip/ua) into structlog contextvars.
    - Emits a single structured access log with latency + status code.
    - Logs unhandled exceptions once and returns JSON 500 with request_id.
    - Always sets X-Request-ID on the response.
    """
    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        req_id = None
        for h in _REQUEST_ID_HEADERS:
            if h in request.headers:
                req_id = request.headers[h]
                break
        if not req_id:
            req_id = str(uuid.uuid4())

        structlog.contextvars.bind_contextvars(
            request_id=req_id,
            method=request.method,
            path=request.url.path,
            client_ip=(request.client.host if request.client else None),
            user_agent=request.headers.get("user-agent"),
        )

        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            # Log once with full context + traceback
            await logger.aexception("unhandled_exception")
            structlog.contextvars.clear_contextvars()
            return JSONResponse(
                {"detail": "Internal Server Error", "request_id": req_id},
                status_code=500,
                headers={"X-Request-ID": req_id},
            )

        # Access log

        duration_ms = round((time.perf_counter() - start) * 1000.0, 2)
        await logger.ainfo("http_request", status_code=response.status_code, duration_ms=duration_ms)

        # Propagate request id and clear context
        response.headers["X-Request-ID"] = req_id
        structlog.contextvars.clear_contextvars()
        return response

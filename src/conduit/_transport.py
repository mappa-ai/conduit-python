"""HTTP transport helpers for the Conduit Python SDK."""

from __future__ import annotations

import secrets
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlencode, urljoin
from uuid import uuid4

import httpx

from .errors import (
    ApiError,
    AuthError,
    ConduitError,
    InsufficientCreditsError,
    RateLimitError,
    RequestAbortedError,
    ValidationError,
)
from .errors import TimeoutError as ConduitTimeoutError
from .types import ErrorTelemetry, RequestTelemetry, ResponseTelemetry, TelemetryHooks

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


@dataclass(slots=True)
class TransportResponse:
    """Normalized HTTP response payload."""

    data: object
    status: int
    request_id: str | None
    headers: httpx.Headers


@dataclass(slots=True)
class StreamConnection:
    """Open streaming response metadata."""

    response: httpx.Response
    request_id: str


class Transport:
    """Authenticated transport with retry support."""

    def __init__(
        self,
        *,
        api_key: str,
        auth_header_name: str = "Mappa-Api-Key",
        base_url: str,
        timeout_ms: int,
        max_retries: int,
        user_agent: str | None = None,
        http_client: httpx.Client | None = None,
        telemetry: TelemetryHooks | None = None,
    ) -> None:
        self._api_key = api_key
        self._auth_header_name = auth_header_name
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._telemetry = telemetry
        self._timeout_ms = timeout_ms
        self._user_agent = user_agent
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            base_url=self._base_url,
            follow_redirects=False,
            timeout=timeout_ms / 1000,
        )

    @property
    def base_url(self) -> str:
        """Return the configured API base URL."""
        return self._base_url

    @property
    def max_retries(self) -> int:
        """Return the configured retry budget."""
        return self._max_retries

    @property
    def timeout_ms(self) -> int:
        """Return the default request timeout in milliseconds."""
        return self._timeout_ms

    def close(self) -> None:
        """Close the underlying HTTP client when owned by the SDK."""
        if self._owns_client:
            self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: object | None = None,
        data: Mapping[str, str] | None = None,
        files: object | None = None,
        query: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        request_id: str | None = None,
        idempotency_key: str | None = None,
        retryable: bool = False,
        timeout_ms: int | None = None,
    ) -> TransportResponse:
        """Issue an authenticated API request."""
        resolved_request_id = request_id or f"req_{uuid4().hex}"
        attempts = self._max_retries + 1 if retryable else 1

        for attempt in range(1, attempts + 1):
            started = time.monotonic()
            url = self._full_url(path, query)
            self._emit_request(method, resolved_request_id, url)
            try:
                response = self._client.request(
                    method,
                    path,
                    params=query,
                    json=json_body,
                    data=data,
                    files=cast("Any", files),
                    headers=self._headers(
                        request_id=resolved_request_id,
                        idempotency_key=idempotency_key,
                        headers=headers,
                    ),
                    timeout=(timeout_ms or self._timeout_ms) / 1000,
                )
            except httpx.TimeoutException as exc:
                error = ConduitTimeoutError(
                    f"Request timed out after {timeout_ms or self._timeout_ms}ms",
                    code="timeout",
                    request_id=resolved_request_id,
                    cause=exc,
                )
                self._emit_error(method, resolved_request_id, url, started, error)
                if attempt < attempts:
                    _sleep(_backoff_ms(attempt))
                    continue
                raise error from exc
            except httpx.HTTPError as exc:
                error = ConduitError(
                    "Request failed",
                    code="transport_error",
                    request_id=resolved_request_id,
                    cause=exc,
                )
                self._emit_error(method, resolved_request_id, url, started, error)
                if attempt < attempts:
                    _sleep(_backoff_ms(attempt))
                    continue
                raise error from exc

            server_request_id = response.headers.get(
                "x-request-id", resolved_request_id
            )
            self._emit_response(
                method,
                server_request_id,
                url,
                started,
                response.status_code,
            )
            if response.is_success:
                return TransportResponse(
                    data=_read_response_data(response),
                    status=response.status_code,
                    request_id=server_request_id,
                    headers=response.headers,
                )

            error = _coerce_api_error(response, server_request_id)
            self._emit_error(method, server_request_id, url, started, error)
            if attempt < attempts and _should_retry_error(error):
                _sleep(_retry_after_ms(error, attempt))
                continue
            raise error

        raise ConduitError("Unexpected transport exit", code="transport_error")

    @contextmanager
    def open_stream(
        self,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        request_id: str | None = None,
        last_event_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> Iterator[StreamConnection]:
        """Open an authenticated streaming request."""
        resolved_request_id = request_id or f"req_{uuid4().hex}"
        request_headers = self._headers(request_id=resolved_request_id, headers=headers)
        if last_event_id:
            request_headers["Last-Event-ID"] = last_event_id
        url = self._full_url(path, query)
        started = time.monotonic()
        self._emit_request("GET", resolved_request_id, url)
        try:
            with self._client.stream(
                "GET",
                path,
                params=query,
                headers=request_headers,
                timeout=(timeout_ms or self._timeout_ms) / 1000,
            ) as response:
                server_request_id = response.headers.get(
                    "x-request-id",
                    resolved_request_id,
                )
                self._emit_response(
                    "GET",
                    server_request_id,
                    url,
                    started,
                    response.status_code,
                )
                if not response.is_success:
                    error = _coerce_api_error(response, server_request_id)
                    self._emit_error("GET", server_request_id, url, started, error)
                    raise error
                yield StreamConnection(response=response, request_id=server_request_id)
        except httpx.TimeoutException as exc:
            error = ConduitTimeoutError(
                f"Request timed out after {timeout_ms or self._timeout_ms}ms",
                code="timeout",
                request_id=resolved_request_id,
                cause=exc,
            )
            self._emit_error("GET", resolved_request_id, url, started, error)
            raise error from exc
        except httpx.HTTPError as exc:
            error = ConduitError(
                "Request failed",
                code="transport_error",
                request_id=resolved_request_id,
                cause=exc,
            )
            self._emit_error("GET", resolved_request_id, url, started, error)
            raise error from exc

    def aborted(
        self, *, request_id: str | None = None, cause: BaseException | None = None
    ) -> RequestAbortedError:
        """Create a typed request-aborted error."""
        return RequestAbortedError(
            "Request aborted by caller",
            code="request_aborted",
            request_id=request_id,
            cause=cause,
        )

    def _headers(
        self,
        *,
        request_id: str,
        idempotency_key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        values = {
            self._auth_header_name: self._api_key,
            "X-Request-Id": request_id,
        }
        if self._user_agent:
            values["User-Agent"] = self._user_agent
        if idempotency_key:
            values["Idempotency-Key"] = idempotency_key
        if headers:
            values.update(headers)
        return values

    def _emit_request(self, method: str, request_id: str, url: str) -> None:
        if self._telemetry is None or self._telemetry.on_request is None:
            return
        self._telemetry.on_request(
            RequestTelemetry(method=method, request_id=request_id, url=url)
        )

    def _emit_response(
        self,
        method: str,
        request_id: str,
        url: str,
        started: float,
        status: int,
    ) -> None:
        if self._telemetry is None or self._telemetry.on_response is None:
            return
        self._telemetry.on_response(
            ResponseTelemetry(
                duration_ms=(time.monotonic() - started) * 1000,
                method=method,
                request_id=request_id,
                status=status,
                url=url,
            )
        )

    def _emit_error(
        self,
        method: str,
        request_id: str,
        url: str,
        started: float,
        error: BaseException,
    ) -> None:
        if self._telemetry is None or self._telemetry.on_error is None:
            return
        self._telemetry.on_error(
            ErrorTelemetry(
                duration_ms=(time.monotonic() - started) * 1000,
                error=error,
                method=method,
                request_id=request_id,
                url=url,
            )
        )

    def _full_url(self, path: str, query: Mapping[str, str] | None) -> str:
        base_url = urljoin(f"{self._base_url}/", path.lstrip("/"))
        if query is None:
            return base_url
        return f"{base_url}?{urlencode(query)}"


def _read_response_data(response: httpx.Response) -> object:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        return response.json()
    if response.content:
        return response.text
    return None


def _coerce_api_error(response: httpx.Response, request_id: str | None) -> ApiError:
    payload = _read_error_body(response)
    message, code, details = _read_error_fields(payload)

    if response.status_code in {401, 403}:
        return AuthError(
            message,
            status=response.status_code,
            code=code,
            request_id=request_id,
            details=details,
        )
    if response.status_code == 402:
        required, available = _read_credit_details(details)
        return InsufficientCreditsError(
            message,
            status=response.status_code,
            code=code,
            request_id=request_id,
            details=details,
            required=required,
            available=available,
        )
    if response.status_code == 422:
        return ValidationError(
            message,
            status=response.status_code,
            code=code,
            request_id=request_id,
            details=details,
        )
    if response.status_code == 429:
        return RateLimitError(
            message,
            status=response.status_code,
            code=code,
            request_id=request_id,
            details=details,
            retry_after_ms=_read_retry_after_ms(response.headers.get("retry-after")),
        )
    return ApiError(
        message,
        status=response.status_code,
        code=code,
        request_id=request_id,
        details=details,
    )


def _read_error_body(response: httpx.Response) -> object | None:
    try:
        return response.json()
    except ValueError:
        if response.text:
            return {"message": response.text}
        return None


def _read_error_fields(payload: object | None) -> tuple[str, str, object | None]:
    default_message = "Request failed"
    default_code = "api_error"
    if not isinstance(payload, dict):
        return default_message, default_code, payload

    payload_map = cast("dict[str, object]", payload)

    error = payload_map.get("error")
    if isinstance(error, dict):
        error_map = cast("dict[str, object]", error)
        code = error_map.get("code")
        message = error_map.get("message")
        return (
            message if isinstance(message, str) else default_message,
            code if isinstance(code, str) else default_code,
            error_map.get("details"),
        )

    code = payload_map.get("code")
    message = payload_map.get("message")
    return (
        message if isinstance(message, str) else default_message,
        code if isinstance(code, str) else default_code,
        payload_map,
    )


def _read_credit_details(details: object | None) -> tuple[int, int]:
    if not isinstance(details, dict):
        return 0, 0
    details_map = cast("dict[str, object]", details)
    required = details_map.get("required")
    available = details_map.get("available")
    return (
        int(required) if isinstance(required, int | float) else 0,
        int(available) if isinstance(available, int | float) else 0,
    )


def _read_retry_after_ms(value: str | None) -> int | None:
    if value is None:
        return None
    if value.isdigit():
        return int(value) * 1000
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = datetime.now(tz=UTC)
    return max(0, int((retry_at - now).total_seconds() * 1000))


def _should_retry_error(error: Exception) -> bool:
    if isinstance(error, RateLimitError):
        return True
    if isinstance(error, ApiError):
        return error.status >= 500
    return (
        error.code in {"timeout", "transport_error"}
        if isinstance(error, ConduitError)
        else False
    )


def _retry_after_ms(error: Exception, attempt: int) -> int:
    if isinstance(error, RateLimitError) and error.retry_after_ms is not None:
        return error.retry_after_ms
    return _backoff_ms(attempt)


def _backoff_ms(attempt: int) -> int:
    base = min(500 * (2**attempt), 4000)
    jitter = secrets.randbelow(max(base // 2, 1))
    return base + jitter


def _sleep(duration_ms: int) -> None:
    time.sleep(duration_ms / 1000)


__all__ = ["StreamConnection", "Transport", "TransportResponse"]

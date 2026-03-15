"""HTTP transport helpers for the Conduit Python SDK."""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import httpx

from .errors import (
    ApiError,
    AuthError,
    ConduitError,
    InsufficientCreditsError,
    RateLimitError,
    ValidationError,
)
from .errors import (
    TimeoutError as ConduitTimeoutError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(slots=True)
class TransportResponse:
    """Normalized HTTP response payload."""

    data: object
    status: int
    request_id: str | None
    headers: httpx.Headers


class Transport:
    """Small authenticated transport with retry support."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        timeout_ms: int,
        max_retries: int,
        user_agent: str | None = None,
    ) -> None:
        """Initialize the transport."""
        self._api_key = api_key
        self._timeout_ms = timeout_ms
        self._max_retries = max_retries
        self._user_agent = user_agent
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            follow_redirects=False,
            timeout=timeout_ms / 1000,
        )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: object | None = None,
        data: Mapping[str, str] | None = None,
        files: Mapping[str, tuple[str, bytes, str | None]] | None = None,
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
            try:
                response = self._client.request(
                    method,
                    path,
                    params=query,
                    json=json_body,
                    data=data,
                    files=files,
                    headers=self._headers(
                        request_id=resolved_request_id,
                        idempotency_key=idempotency_key,
                        headers=headers,
                    ),
                    timeout=(timeout_ms or self._timeout_ms) / 1000,
                )
                server_request_id = response.headers.get(
                    "x-request-id", resolved_request_id
                )
                if response.is_success:
                    return TransportResponse(
                        data=_read_response_data(response),
                        status=response.status_code,
                        request_id=server_request_id,
                        headers=response.headers,
                    )

                error = _coerce_api_error(response, server_request_id)
                if attempt < attempts and _should_retry_error(error):
                    _sleep(_retry_after_ms(error, attempt))
                    continue
                raise error
            except httpx.TimeoutException as exc:
                error = ConduitTimeoutError(
                    f"Request timed out after {timeout_ms or self._timeout_ms}ms",
                    code="timeout",
                    request_id=resolved_request_id,
                    cause=exc,
                )
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
                if attempt < attempts:
                    _sleep(_backoff_ms(attempt))
                    continue
                raise error from exc

        raise ConduitError("Unexpected transport exit", code="transport_error")

    def _headers(
        self,
        *,
        request_id: str,
        idempotency_key: str | None,
        headers: Mapping[str, str] | None,
    ) -> dict[str, str]:
        values = {
            "Mappa-Api-Key": self._api_key,
            "X-Request-Id": request_id,
        }
        if self._user_agent:
            values["User-Agent"] = self._user_agent
        if idempotency_key:
            values["Idempotency-Key"] = idempotency_key
        if headers:
            values.update(headers)
        return values


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
    return False


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


__all__ = ["Transport", "TransportResponse"]

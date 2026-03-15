"""Transport tests for the Conduit Python SDK."""

from __future__ import annotations

import httpx
import pytest

from conduit._transport import Transport
from conduit.errors import InsufficientCreditsError


def test_transport_retries_rate_limits() -> None:
    """Retry a 429 response and return the later success payload."""
    attempts = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        attempts[0] += 1
        if attempts[0] == 1:
            return httpx.Response(
                429,
                headers={"retry-after": "0", "x-request-id": "req_rate_limit"},
                json={"error": {"code": "rate_limited", "message": "Slow down"}},
                request=request,
            )
        return httpx.Response(
            200,
            headers={"x-request-id": "req_success"},
            json={"ok": True},
            request=request,
        )

    transport = Transport(
        api_key="sk_test",
        base_url="http://testserver",
        timeout_ms=1000,
        max_retries=2,
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://testserver"
        ),
    )

    try:
        response = transport.request("GET", "/test", retryable=True)
    finally:
        transport.close()

    if attempts[0] != 2:
        raise AssertionError("Expected one retry after the initial 429 response")
    if response.data != {"ok": True}:
        raise AssertionError("Expected the successful retry payload")


def test_transport_maps_insufficient_credits_error() -> None:
    """Expose required and available credits from 402 responses."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            402,
            headers={"x-request-id": "req_credits"},
            json={
                "error": {
                    "code": "insufficient_credits",
                    "message": "Not enough credits",
                    "details": {"available": 2, "required": 5},
                }
            },
            request=request,
        )

    transport = Transport(
        api_key="sk_test",
        base_url="http://testserver",
        timeout_ms=1000,
        max_retries=0,
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://testserver"
        ),
    )

    try:
        with pytest.raises(InsufficientCreditsError) as exc_info:
            transport.request("GET", "/test")
    finally:
        transport.close()

    error = exc_info.value
    if error.required != 5 or error.available != 2:
        raise AssertionError("Expected required and available credits to be exposed")

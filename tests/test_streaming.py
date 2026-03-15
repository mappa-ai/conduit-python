"""Streaming tests for the Conduit Python SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

import httpx

from conduit._transport import Transport
from conduit.streaming import iter_job_events

if TYPE_CHECKING:
    from collections.abc import Iterator


class EventStream(httpx.SyncByteStream):
    """Small sync byte stream for SSE tests."""

    def __init__(self, chunks: list[bytes]) -> None:
        """Initialize the test byte stream."""
        self._chunks = chunks

    @override
    def __iter__(self) -> Iterator[bytes]:
        """Yield the configured byte chunks."""
        return iter(self._chunks)

    @override
    def close(self) -> None:
        """Close the in-memory byte stream."""
        return


def test_streaming_reconnects_with_last_event_id() -> None:
    """Reconnect to the SSE endpoint and resume from the last event ID."""
    attempts = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        attempts[0] += 1
        if attempts[0] == 1:
            payload = (
                b"id: 1\n"
                b"event: status\n"
                b'data: {"job":{"id":"job_1","type":"report.generate",'
                b'"status":"running","createdAt":"2026-03-15T00:00:00Z",'
                b'"updatedAt":"2026-03-15T00:00:01Z"}}\n\n'
            )
            return httpx.Response(
                200,
                headers={
                    "content-type": "text/event-stream",
                    "x-request-id": "req_stream_1",
                },
                request=request,
                stream=EventStream([payload]),
            )

        last_event_id = request.headers.get("Last-Event-ID")
        if last_event_id != "1":
            raise AssertionError("Expected reconnect with Last-Event-ID=1")
        payload = (
            b"id: 2\n"
            b"event: terminal\n"
            b'data: {"job":{"id":"job_1","type":"report.generate",'
            b'"status":"succeeded","createdAt":"2026-03-15T00:00:00Z",'
            b'"updatedAt":"2026-03-15T00:00:02Z","reportId":"rep_1"}}\n\n'
        )
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "x-request-id": "req_stream_2",
            },
            request=request,
            stream=EventStream([payload]),
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
        events = list(iter_job_events(transport, "job_1", timeout_ms=3000))
    finally:
        transport.close()

    if [event.type for event in events] != ["status", "terminal"]:
        raise AssertionError("Expected status event followed by terminal event")
    if attempts[0] != 2:
        raise AssertionError("Expected one reconnect before terminal delivery")

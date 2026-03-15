"""Streaming helpers for Conduit job lifecycle events."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from urllib.parse import quote
from uuid import uuid4

from .errors import ApiError, ConduitError, StreamError
from .errors import TimeoutError as ConduitTimeoutError
from .models import JobEvent, parse_job

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from ._transport import Transport
    from .types import CancelSignal

DEFAULT_STREAM_SESSION_MS = 300000
DEFAULT_STREAM_TIMEOUT_MS = 300000
STREAM_MAX_RETRIES = 5


@dataclass(slots=True)
class RawSSEEvent:
    """Minimal parsed SSE frame."""

    data: str
    event: str
    event_id: str | None = None


def iter_job_events(
    transport: Transport,
    job_id: str,
    *,
    timeout_ms: int | None = None,
    signal: CancelSignal | None = None,
    on_event: Callable[[JobEvent], None] | None = None,
) -> Iterator[JobEvent]:
    """Yield lifecycle events for a job via SSE with reconnection."""
    deadline = None
    if timeout_ms is not None:
        deadline = time.monotonic() + (timeout_ms / 1000)
    last_event_id: str | None = None
    retry_count = 0

    while True:
        _check_signal(signal, transport)
        if deadline is not None and time.monotonic() >= deadline:
            raise ConduitTimeoutError(
                f"Timed out waiting for job {job_id} after {timeout_ms}ms",
                code="timeout",
            )
        request_id = f"req_{uuid4().hex}"
        try:
            for event, next_last_event_id in _stream_once(
                transport,
                job_id,
                deadline=deadline,
                last_event_id=last_event_id,
                request_id=request_id,
                signal=signal,
                on_event=on_event,
            ):
                last_event_id = next_last_event_id
                retry_count = 0
                yield event
                if event.type == "terminal":
                    return
        except ConduitTimeoutError:
            raise
        except ConduitError as exc:
            if not _should_retry_stream_error(exc) or retry_count >= STREAM_MAX_RETRIES:
                raise StreamError(
                    f"Failed to stream job {job_id}",
                    job_id=job_id,
                    last_event_id=last_event_id,
                    retry_count=retry_count,
                    request_id=request_id,
                    cause=exc,
                ) from exc
        retry_count += 1
        if retry_count > STREAM_MAX_RETRIES:
            raise StreamError(
                f"Failed to stream job {job_id} after {retry_count} retries",
                job_id=job_id,
                last_event_id=last_event_id,
                retry_count=retry_count,
            )
        _sleep(_backoff_ms(retry_count), signal, transport, request_id=request_id)


def _stream_once(
    transport: Transport,
    job_id: str,
    *,
    deadline: float | None,
    last_event_id: str | None,
    request_id: str,
    signal: CancelSignal | None,
    on_event: Callable[[JobEvent], None] | None,
) -> Iterator[tuple[JobEvent, str | None]]:
    session_timeout_ms = _session_timeout_ms(deadline)
    path = f"/v1/jobs/{quote(job_id, safe='')}/stream"
    query = {"timeout": str(session_timeout_ms)}
    with transport.open_stream(
        path,
        query=query,
        request_id=request_id,
        last_event_id=last_event_id,
        timeout_ms=session_timeout_ms + 5000,
    ) as connection:
        current_last_event_id = last_event_id
        for raw_event in _iter_sse_events(connection.response.iter_lines()):
            _check_signal(signal, transport, request_id=connection.request_id)
            if raw_event.event_id is not None:
                current_last_event_id = raw_event.event_id
            parsed_event = _parse_job_event(raw_event)
            if parsed_event is None:
                continue
            if on_event is not None:
                on_event(parsed_event)
            yield parsed_event, current_last_event_id


def _iter_sse_events(lines: Iterator[str]) -> Iterator[RawSSEEvent]:
    event_name = "message"
    event_id: str | None = None
    data_lines: list[str] = []

    for line in lines:
        if line == "":
            if data_lines:
                yield RawSSEEvent(
                    data="\n".join(data_lines),
                    event=event_name,
                    event_id=event_id,
                )
            event_name = "message"
            event_id = None
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "event":
            event_name = value or "message"
            continue
        if field == "id":
            event_id = value or None
            continue
        if field == "data":
            data_lines.append(value)

    if data_lines:
        yield RawSSEEvent(
            data="\n".join(data_lines), event=event_name, event_id=event_id
        )


def _parse_job_event(raw_event: RawSSEEvent) -> JobEvent | None:
    if raw_event.event == "heartbeat":
        return None
    payload = _json_object(raw_event.data)
    if raw_event.event == "error":
        message = payload.get("message")
        code = payload.get("code")
        raise ConduitError(
            message if isinstance(message, str) else "Unknown SSE error",
            code=code if isinstance(code, str) else "stream_error",
        )
    if raw_event.event not in {"status", "stage", "terminal"}:
        return None
    job_data = payload.get("job")
    job = parse_job(job_data)
    if raw_event.event == "stage":
        stage = payload.get("stage")
        progress = payload.get("progress")
        return JobEvent(
            type="stage",
            job=job,
            stage=stage if isinstance(stage, str) else job.stage,
            progress=float(progress)
            if isinstance(progress, int | float)
            else job.progress,
        )
    return JobEvent(type=raw_event.event, job=job)


def _json_object(raw_data: str) -> Mapping[str, object]:
    try:
        value = json.loads(raw_data)
    except json.JSONDecodeError as exc:
        raise ConduitError(
            "Invalid stream payload",
            code="invalid_response",
            cause=exc,
        ) from exc
    if isinstance(value, dict):
        return cast("Mapping[str, object]", value)
    raise ConduitError("Invalid stream payload", code="invalid_response")


def _should_retry_stream_error(error: ConduitError) -> bool:
    if isinstance(error, ApiError):
        return error.status >= 500 or error.status == 429
    return error.code in {"timeout", "transport_error"}


def _session_timeout_ms(deadline: float | None) -> int:
    if deadline is None:
        return DEFAULT_STREAM_SESSION_MS
    remaining_ms = int((deadline - time.monotonic()) * 1000)
    if remaining_ms <= 0:
        return 1
    return min(remaining_ms, DEFAULT_STREAM_SESSION_MS)


def _check_signal(
    signal: CancelSignal | None,
    transport: Transport,
    *,
    request_id: str | None = None,
) -> None:
    if signal is not None and signal.is_set():
        raise transport.aborted(request_id=request_id)


def _backoff_ms(retry_count: int) -> int:
    return min(500 * (2**retry_count), 4000)


def _sleep(
    duration_ms: int,
    signal: CancelSignal | None,
    transport: Transport,
    *,
    request_id: str | None = None,
) -> None:
    started = time.monotonic()
    while (time.monotonic() - started) * 1000 < duration_ms:
        _check_signal(signal, transport, request_id=request_id)
        time.sleep(0.05)


__all__ = ["DEFAULT_STREAM_TIMEOUT_MS", "iter_job_events"]

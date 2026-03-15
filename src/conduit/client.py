"""Public client surface for the Conduit Python SDK."""

from __future__ import annotations

import hashlib
import hmac
import mimetypes
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Self, cast
from urllib.parse import quote, urlparse
from uuid import uuid4

import httpx

from ._transport import Transport
from .errors import (
    ConduitError,
    InitializationError,
    InvalidSourceError,
    JobCanceledError,
    JobFailedError,
    RemoteFetchError,
    RemoteFetchTimeoutError,
    RemoteFetchTooLargeError,
    StreamError,
    WebhookVerificationError,
)
from .errors import (
    TimeoutError as ConduitTimeoutError,
)
from .models import (
    Entity,
    FileDeleteReceipt,
    Job,
    JobEvent,
    ListEntitiesResponse,
    ListFilesResponse,
    MatchingAnalysisResponse,
    MediaFile,
    MediaObject,
    Report,
    RetentionLockResult,
    WebhookEvent,
    parse_delete_receipt,
    parse_entity,
    parse_job,
    parse_job_receipt,
    parse_list_entities,
    parse_list_files,
    parse_matching,
    parse_media_file,
    parse_media_object,
    parse_report,
    parse_retention_lock,
    parse_webhook_event,
)

DEFAULT_MAX_SOURCE_BYTES = 5 * 1024 * 1024 * 1024
DEFAULT_POLL_INTERVAL_MS = 3000
LABEL_SUFFIX_REGEX = re.compile(r"(\.[^.]+)+$")
TERMINAL_JOB_STATUSES = {"succeeded", "failed", "canceled"}
VALID_REPORT_TEMPLATES = {"general_report", "sales_playbook"}


@dataclass(slots=True)
class ReportRunHandle:
    """Secondary synchronous controls for a report job."""

    job_id: str
    _jobs: _JobsResource
    _reports: _ReportsResource

    def stream(
        self,
        *,
        timeout_ms: int | None = None,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> Iterator[JobEvent]:
        """Yield job events until the job reaches a terminal state."""
        return self._jobs.stream(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            poll_interval_ms=poll_interval_ms,
        )

    def wait(
        self,
        *,
        timeout_ms: int = 300000,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> Report:
        """Wait for the completed report resource."""
        job = self._jobs.wait(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            poll_interval_ms=poll_interval_ms,
        )
        if job.report_id is None:
            raise ConduitError(
                f"Job {self.job_id} succeeded but no report_id was returned",
                code="invalid_response",
            )
        return self._reports.get(job.report_id)

    def cancel(self) -> Job:
        """Request cancellation for the report job."""
        return self._jobs.cancel(self.job_id)

    def job(self) -> Job:
        """Fetch the latest job state."""
        return self._jobs.get(self.job_id)

    def report(self) -> Report | None:
        """Fetch the completed report if it already exists."""
        return self._reports.get_by_job(self.job_id)


@dataclass(slots=True)
class MatchingRunHandle:
    """Secondary synchronous controls for a matching job."""

    job_id: str
    _jobs: _JobsResource
    _matching: _MatchingResource

    def stream(
        self,
        *,
        timeout_ms: int | None = None,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> Iterator[JobEvent]:
        """Yield job events until the job reaches a terminal state."""
        return self._jobs.stream(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            poll_interval_ms=poll_interval_ms,
        )

    def wait(
        self,
        *,
        timeout_ms: int = 300000,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> MatchingAnalysisResponse:
        """Wait for the completed matching resource."""
        job = self._jobs.wait(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            poll_interval_ms=poll_interval_ms,
        )
        if job.matching_id is None:
            raise ConduitError(
                f"Job {self.job_id} succeeded but no matching_id was returned",
                code="invalid_response",
            )
        return self._matching.get(job.matching_id)

    def cancel(self) -> Job:
        """Request cancellation for the matching job."""
        return self._jobs.cancel(self.job_id)

    def job(self) -> Job:
        """Fetch the latest job state."""
        return self._jobs.get(self.job_id)

    def matching(self) -> MatchingAnalysisResponse | None:
        """Fetch the completed matching resource if it already exists."""
        return self._matching.get_by_job(self.job_id)


@dataclass(slots=True)
class ReportJobReceipt:
    """Receipt returned from report creation."""

    job_id: str
    status: str
    handle: ReportRunHandle
    media_id: str | None = None
    stage: str | None = None
    estimated_wait_sec: float | None = None


@dataclass(slots=True)
class MatchingJobReceipt:
    """Receipt returned from matching creation."""

    job_id: str
    status: str
    handle: MatchingRunHandle
    stage: str | None = None
    estimated_wait_sec: float | None = None


@dataclass(slots=True)
class _UploadMaterialization:
    payload: bytes
    filename: str
    label: str
    content_type: str | None


class _JobsResource:
    def __init__(self, transport: Transport, poll_interval_ms: int) -> None:
        self._transport = transport
        self._poll_interval_ms = poll_interval_ms

    def get(self, job_id: str, *, request_id: str | None = None) -> Job:
        response = self._transport.request(
            "GET",
            f"/v1/jobs/{_path_segment(job_id, 'job_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_job(response.data)

    def cancel(
        self,
        job_id: str,
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> Job:
        response = self._transport.request(
            "POST",
            f"/v1/jobs/{_path_segment(job_id, 'job_id')}/cancel",
            idempotency_key=idempotency_key,
            request_id=request_id,
            retryable=True,
        )
        return parse_job(response.data)

    def wait(
        self,
        job_id: str,
        *,
        timeout_ms: int = 300000,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> Job:
        for event in self.stream(
            job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            poll_interval_ms=poll_interval_ms,
        ):
            if event.type != "terminal":
                continue
            if event.job.status == "succeeded":
                return event.job
            if event.job.status == "failed":
                message = event.job.error.message if event.job.error else "Job failed"
                raise JobFailedError(
                    job_id,
                    message,
                    code=event.job.error.code if event.job.error else "job_failed",
                    request_id=event.job.request_id,
                )
            raise JobCanceledError(
                job_id,
                "Job canceled",
                request_id=event.job.request_id,
            )
        raise ConduitTimeoutError(
            f"Timed out waiting for job {job_id} after {timeout_ms}ms",
            code="timeout",
        )

    def stream(
        self,
        job_id: str,
        *,
        timeout_ms: int | None = None,
        on_event: Callable[[JobEvent], None] | None = None,
        poll_interval_ms: int | None = None,
    ) -> Iterator[JobEvent]:
        deadline = (
            None if timeout_ms is None else time.monotonic() + (timeout_ms / 1000)
        )
        interval_ms = poll_interval_ms or self._poll_interval_ms
        last_status: str | None = None
        last_stage: str | None = None

        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise ConduitTimeoutError(
                    f"Timed out waiting for job {job_id} after {timeout_ms}ms",
                    code="timeout",
                )
            try:
                job = self.get(job_id)
            except ConduitError as exc:
                raise StreamError(
                    f"Failed to fetch status for job {job_id}",
                    job_id=job_id,
                    cause=exc,
                ) from exc

            if job.status != last_status:
                event = JobEvent(type="status", job=job)
                if on_event is not None:
                    on_event(event)
                yield event
                last_status = job.status

            current_stage = job.stage
            if current_stage != last_stage and current_stage is not None:
                event = JobEvent(
                    type="stage",
                    job=job,
                    stage=current_stage,
                    progress=job.progress,
                )
                if on_event is not None:
                    on_event(event)
                yield event
                last_stage = current_stage

            if job.status in TERMINAL_JOB_STATUSES:
                event = JobEvent(type="terminal", job=job)
                if on_event is not None:
                    on_event(event)
                yield event
                return

            time.sleep(interval_ms / 1000)


class _EntitiesResource:
    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def get(self, entity_id: str, *, request_id: str | None = None) -> Entity:
        response = self._transport.request(
            "GET",
            f"/v1/entities/{_path_segment(entity_id, 'entity_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_entity(response.data)

    def list(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        request_id: str | None = None,
    ) -> ListEntitiesResponse:
        query = {
            "limit": str(limit or 20),
        }
        if cursor:
            query["cursor"] = cursor
        response = self._transport.request(
            "GET",
            "/v1/entities",
            query=query,
            request_id=request_id,
            retryable=True,
        )
        return parse_list_entities(response.data)

    def update(
        self,
        entity_id: str,
        body: Mapping[str, object],
        *,
        request_id: str | None = None,
    ) -> Entity:
        payload = {"label": body.get("label")}
        response = self._transport.request(
            "PATCH",
            f"/v1/entities/{_path_segment(entity_id, 'entity_id')}",
            json_body=payload,
            request_id=request_id,
            retryable=True,
        )
        return parse_entity(response.data)


class _MediaResource:
    def __init__(
        self,
        transport: Transport,
        *,
        timeout_ms: int,
        max_source_bytes: int,
    ) -> None:
        self._transport = transport
        self._timeout_ms = timeout_ms
        self._max_source_bytes = max_source_bytes

    def upload(
        self,
        *,
        file: bytes | bytearray | memoryview | BinaryIO | None = None,
        url: str | None = None,
        path: str | None = None,
        label: str | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> MediaObject:
        materialized = self._materialize_source(
            file=file,
            url=url,
            path=path,
            label=label,
        )
        response = self._transport.request(
            "POST",
            "/v1/files",
            data={"label": materialized.label},
            files={
                "file": (
                    materialized.filename,
                    materialized.payload,
                    materialized.content_type,
                )
            },
            idempotency_key=idempotency_key,
            request_id=request_id,
            retryable=True,
        )
        return parse_media_object(response.data)

    def get(self, media_id: str, *, request_id: str | None = None) -> MediaFile:
        response = self._transport.request(
            "GET",
            f"/v1/files/{_path_segment(media_id, 'media_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_media_file(response.data)

    def list(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        include_deleted: bool = False,
        request_id: str | None = None,
    ) -> ListFilesResponse:
        query = {
            "includeDeleted": str(include_deleted).lower(),
            "limit": str(limit or 20),
        }
        if cursor:
            query["cursor"] = cursor
        response = self._transport.request(
            "GET",
            "/v1/files",
            query=query,
            request_id=request_id,
            retryable=True,
        )
        return parse_list_files(response.data)

    def delete(
        self,
        media_id: str,
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> FileDeleteReceipt:
        response = self._transport.request(
            "DELETE",
            f"/v1/files/{_path_segment(media_id, 'media_id')}",
            idempotency_key=idempotency_key,
            request_id=request_id,
            retryable=True,
        )
        return parse_delete_receipt(response.data)

    def set_retention_lock(
        self,
        media_id: str,
        *,
        locked: bool,
        request_id: str | None = None,
    ) -> RetentionLockResult:
        response = self._transport.request(
            "PATCH",
            f"/v1/files/{_path_segment(media_id, 'media_id')}/retention",
            json_body={"lock": locked},
            request_id=request_id,
            retryable=True,
        )
        return parse_retention_lock(response.data)

    def resolve_report_source(
        self,
        source: Mapping[str, object],
        *,
        idempotency_key: str | None,
        request_id: str | None,
    ) -> str:
        keys = [key for key in ("mediaId", "file", "url", "path") if key in source]
        if len(keys) != 1:
            raise InvalidSourceError(
                "source must include exactly one of mediaId, file, url, or path",
                code="invalid_source",
            )
        if "mediaId" in source:
            return _string_value(source.get("mediaId"), "source.mediaId")
        return self.upload(
            file=cast(
                "bytes | bytearray | memoryview | BinaryIO | None", source.get("file")
            ),
            url=cast("str | None", source.get("url")),
            path=cast("str | None", source.get("path")),
            label=cast("str | None", source.get("label")),
            idempotency_key=idempotency_key,
            request_id=request_id,
        ).media_id

    def _materialize_source(
        self,
        *,
        file: bytes | bytearray | memoryview | BinaryIO | None,
        url: str | None,
        path: str | None,
        label: str | None,
    ) -> _UploadMaterialization:
        provided = [
            key
            for key, value in (("file", file), ("url", url), ("path", path))
            if value is not None
        ]
        if len(provided) != 1:
            raise InvalidSourceError(
                "upload() must include exactly one of file, url, or path",
                code="invalid_source",
            )
        if path is not None:
            return self._materialize_path(path, label)
        if url is not None:
            return self._materialize_url(url, label)
        if file is None:
            raise InvalidSourceError("file is required", code="invalid_source")
        return self._materialize_file(file, label)

    def _materialize_path(
        self, raw_path: str, label: str | None
    ) -> _UploadMaterialization:
        path = Path(_string_value(raw_path, "source.path"))
        if not path.exists():
            raise InvalidSourceError(f"File not found: {path}", code="invalid_source")
        if path.is_dir():
            raise InvalidSourceError(
                f"Path is a directory: {path}", code="invalid_source"
            )
        size = path.stat().st_size
        if size > self._max_source_bytes:
            raise InvalidSourceError(
                "source.path exceeds upload size limit", code="source_too_large"
            )
        payload = path.read_bytes()
        filename = path.name
        return _UploadMaterialization(
            payload=payload,
            filename=filename,
            label=_resolve_label(label, filename, fallback="upload"),
            content_type=mimetypes.guess_type(filename)[0],
        )

    def _materialize_file(
        self,
        raw_file: bytes | bytearray | memoryview | BinaryIO,
        label: str | None,
    ) -> _UploadMaterialization:
        payload, filename = _read_binary_like(raw_file)
        if len(payload) > self._max_source_bytes:
            raise InvalidSourceError(
                "source.file exceeds upload size limit", code="source_too_large"
            )
        return _UploadMaterialization(
            payload=payload,
            filename=filename,
            label=_resolve_label(label, filename, fallback="upload"),
            content_type=mimetypes.guess_type(filename)[0],
        )

    def _materialize_url(
        self, raw_url: str, label: str | None
    ) -> _UploadMaterialization:
        url = _validate_http_url(raw_url, "source.url")
        try:
            with (
                httpx.Client(
                    follow_redirects=True,
                    max_redirects=5,
                    timeout=self._timeout_ms / 1000,
                ) as client,
                client.stream("GET", url) as response,
            ):
                if not response.is_success:
                    raise RemoteFetchError(
                        f"Remote fetch failed with status {response.status_code}",
                        code="remote_fetch_failed",
                        status=response.status_code,
                        url=url,
                    )
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > self._max_source_bytes:
                    raise RemoteFetchTooLargeError(
                        "source.url exceeds upload size limit",
                        code="source_too_large",
                        url=url,
                        status=response.status_code,
                    )
                chunks = bytearray()
                for chunk in response.iter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > self._max_source_bytes:
                        raise RemoteFetchTooLargeError(
                            "source.url exceeds upload size limit",
                            code="source_too_large",
                            url=url,
                            status=response.status_code,
                        )
                final_filename = _filename_from_url(str(response.url))
                content_type = response.headers.get("content-type")
        except httpx.TimeoutException as exc:
            raise RemoteFetchTimeoutError(
                "Remote fetch timed out",
                code="remote_fetch_timeout",
                url=url,
                cause=exc,
            ) from exc
        except httpx.TooManyRedirects as exc:
            raise RemoteFetchError(
                "Remote fetch exceeded redirect limit",
                code="remote_fetch_redirects_exhausted",
                url=url,
                cause=exc,
            ) from exc
        except httpx.HTTPError as exc:
            raise RemoteFetchError(
                "Remote fetch failed",
                code="remote_fetch_failed",
                url=url,
                cause=exc,
            ) from exc

        return _UploadMaterialization(
            payload=bytes(chunks),
            filename=final_filename,
            label=_resolve_label(label, final_filename, fallback="remote"),
            content_type=_strip_content_type(content_type),
        )


class _ReportsResource:
    def __init__(
        self,
        transport: Transport,
        jobs: _JobsResource,
        media: _MediaResource,
    ) -> None:
        self._transport = transport
        self._jobs = jobs
        self._media = media

    def create(
        self,
        *,
        source: Mapping[str, object],
        output: Mapping[str, object],
        target: Mapping[str, object],
        webhook: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> ReportJobReceipt:
        media_id = self._media.resolve_report_source(
            source,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )
        body = {
            "media": {"mediaId": media_id},
            "output": _normalize_report_output(output),
            "target": _normalize_target_selector(target),
        }
        normalized_webhook = _normalize_webhook(webhook)
        if normalized_webhook is not None:
            body["webhook"] = normalized_webhook
        response = self._transport.request(
            "POST",
            "/v1/reports/jobs",
            json_body=body,
            idempotency_key=idempotency_key or _random_id("idem"),
            request_id=request_id,
            retryable=True,
        )
        job_id, status, stage, estimated_wait_sec = parse_job_receipt(response.data)
        return ReportJobReceipt(
            job_id=job_id,
            status=status,
            stage=stage,
            estimated_wait_sec=estimated_wait_sec,
            media_id=media_id,
            handle=ReportRunHandle(job_id=job_id, _jobs=self._jobs, _reports=self),
        )

    def get(self, report_id: str, *, request_id: str | None = None) -> Report:
        response = self._transport.request(
            "GET",
            f"/v1/reports/{_path_segment(report_id, 'report_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_report(response.data)

    def get_by_job(
        self, job_id: str, *, request_id: str | None = None
    ) -> Report | None:
        response = self._transport.request(
            "GET",
            f"/v1/reports/by-job/{_path_segment(job_id, 'job_id')}",
            request_id=request_id,
            retryable=True,
        )
        if response.data is None:
            return None
        return parse_report(response.data)


class _MatchingResource:
    def __init__(self, transport: Transport, jobs: _JobsResource) -> None:
        self._transport = transport
        self._jobs = jobs

    def create(
        self,
        *,
        context: str,
        target: Mapping[str, object],
        group: Sequence[Mapping[str, object]],
        webhook: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> MatchingJobReceipt:
        body = {
            "context": _normalize_matching_context(context),
            "target": _normalize_matching_subject(target),
            "group": [_normalize_matching_subject(item) for item in group],
        }
        _ensure_unique_direct_entity_ids(
            cast("Mapping[str, object]", body["target"]),
            cast("list[Mapping[str, object]]", body["group"]),
        )
        normalized_webhook = _normalize_webhook(webhook)
        if normalized_webhook is not None:
            body["webhook"] = normalized_webhook
        response = self._transport.request(
            "POST",
            "/v1/matching/jobs",
            json_body=body,
            idempotency_key=idempotency_key or _random_id("idem"),
            request_id=request_id,
            retryable=True,
        )
        job_id, status, stage, estimated_wait_sec = parse_job_receipt(response.data)
        return MatchingJobReceipt(
            job_id=job_id,
            status=status,
            stage=stage,
            estimated_wait_sec=estimated_wait_sec,
            handle=MatchingRunHandle(job_id=job_id, _jobs=self._jobs, _matching=self),
        )

    def get(
        self, matching_id: str, *, request_id: str | None = None
    ) -> MatchingAnalysisResponse:
        response = self._transport.request(
            "GET",
            f"/v1/matching/{_path_segment(matching_id, 'matching_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_matching(response.data)

    def get_by_job(
        self,
        job_id: str,
        *,
        request_id: str | None = None,
    ) -> MatchingAnalysisResponse | None:
        response = self._transport.request(
            "GET",
            f"/v1/matching/by-job/{_path_segment(job_id, 'job_id')}",
            request_id=request_id,
            retryable=True,
        )
        if response.data is None:
            return None
        return parse_matching(response.data)


class _WebhooksResource:
    def verify_signature(
        self,
        payload: str,
        headers: Mapping[str, str | Sequence[str] | None],
        secret: str,
        *,
        tolerance_sec: int = 300,
    ) -> bool:
        signature_header = _get_header(headers, "conduit-signature")
        if signature_header is None:
            raise WebhookVerificationError(
                "Missing conduit-signature header",
                code="webhook_signature_missing",
            )
        timestamp, signature = _parse_signature_header(signature_header)
        now = int(time.time())
        if abs(now - timestamp) > tolerance_sec:
            raise WebhookVerificationError(
                "Signature timestamp outside tolerance",
                code="webhook_signature_stale",
            )
        expected = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.{payload}".encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise WebhookVerificationError(
                "Invalid signature",
                code="webhook_signature_invalid",
            )
        return True

    def parse_event(self, payload: str) -> WebhookEvent:
        try:
            value = httpx.Response(200, text=payload).json()
        except ValueError as exc:
            raise ConduitError(
                "Invalid webhook payload: invalid JSON",
                code="invalid_webhook_payload",
                cause=exc,
            ) from exc
        event = parse_webhook_event(value)
        _validate_iso8601(event.created_at, "created_at")
        _validate_iso8601(event.timestamp, "timestamp")
        if event.type == "report.completed":
            _validate_completed_data(event.data, resource_key="reportId")
        elif event.type == "report.failed":
            _validate_failed_data(event.data)
        elif event.type == "matching.completed":
            _validate_completed_data(event.data, resource_key="matchingId")
        elif event.type == "matching.failed":
            _validate_failed_data(event.data)
        return event


class _PrimitivesResource:
    def __init__(
        self,
        *,
        entities: _EntitiesResource,
        media: _MediaResource,
        jobs: _JobsResource,
    ) -> None:
        self.entities = entities
        self.media = media
        self.jobs = jobs


class Conduit:
    """Official Python client for the Conduit API."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.mappa.ai",
        timeout_ms: int = 30000,
        max_retries: int = 2,
        max_source_bytes: int = DEFAULT_MAX_SOURCE_BYTES,
        user_agent: str | None = None,
    ) -> None:
        """Initialize a Conduit client."""
        if not api_key:
            raise InitializationError("api_key is required", code="config_error")
        _validate_base_url(base_url)
        self._transport = Transport(
            api_key=api_key,
            base_url=base_url,
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            user_agent=user_agent,
        )
        jobs = _JobsResource(self._transport, DEFAULT_POLL_INTERVAL_MS)
        media = _MediaResource(
            self._transport,
            timeout_ms=timeout_ms,
            max_source_bytes=max_source_bytes,
        )
        self.reports = _ReportsResource(self._transport, jobs, media)
        self.matching = _MatchingResource(self._transport, jobs)
        self.webhooks = _WebhooksResource()
        self.primitives = _PrimitivesResource(
            entities=_EntitiesResource(self._transport),
            media=media,
            jobs=jobs,
        )

    def close(self) -> None:
        """Close the underlying transport."""
        self._transport.close()

    def __enter__(self) -> Self:
        """Enter a context manager scope."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Exit a context manager scope."""
        self.close()


def _validate_base_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise InitializationError("base_url must be a valid URL", code="config_error")


def _validate_http_url(value: str, name: str) -> str:
    normalized = _string_value(value, name)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise InvalidSourceError(
            f"{name} must be an http or https URL",
            code="invalid_source",
        )
    return normalized


def _path_segment(value: str, name: str) -> str:
    return quote(_string_value(value, name), safe="")


def _string_value(value: object, name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConduitError(f"{name} must be a non-empty string", code="invalid_request")


def _random_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _normalize_report_output(output: Mapping[str, object]) -> dict[str, object]:
    template = _string_value(output.get("template"), "output.template")
    if template not in VALID_REPORT_TEMPLATES:
        raise ConduitError(
            "output.template must be general_report or sales_playbook",
            code="invalid_request",
        )
    payload: dict[str, object] = {"template": template}
    template_params = output.get("templateParams")
    if template_params is not None:
        if not isinstance(template_params, Mapping):
            raise ConduitError(
                "output.templateParams must be an object", code="invalid_request"
            )
        payload["templateParams"] = dict(cast("Mapping[str, object]", template_params))
    return payload


def _normalize_target_selector(target: Mapping[str, object]) -> dict[str, object]:
    strategy = _string_value(target.get("strategy"), "target.strategy")
    on_miss = target.get("onMiss")
    payload: dict[str, object] = {"strategy": strategy}
    if on_miss is not None:
        if on_miss not in {"fallback_dominant", "error"}:
            raise ConduitError(
                "target.onMiss must be fallback_dominant or error",
                code="invalid_request",
            )
        payload["on_miss"] = on_miss

    if strategy == "dominant":
        return payload
    if strategy == "timerange":
        time_range = target.get("timeRange")
        if not isinstance(time_range, Mapping):
            raise ConduitError(
                "target.timeRange is required for timerange", code="invalid_request"
            )
        time_range_map = cast("Mapping[str, object]", time_range)
        start = time_range_map.get("startSeconds")
        end = time_range_map.get("endSeconds")
        if start is None and end is None:
            raise ConduitError(
                "target.timeRange must include startSeconds or endSeconds",
                code="invalid_request",
            )
        start_value = float(start) if isinstance(start, int | float) else None
        end_value = float(end) if isinstance(end, int | float) else None
        if (
            start_value is not None
            and end_value is not None
            and start_value >= end_value
        ):
            raise ConduitError(
                "target.timeRange.startSeconds must be less than endSeconds",
                code="invalid_request",
            )
        payload["timerange"] = {
            "start_seconds": start_value,
            "end_seconds": end_value,
        }
        return payload
    if strategy == "entity_id":
        payload["entity_id"] = _string_value(target.get("entityId"), "target.entityId")
        return payload
    if strategy == "magic_hint":
        payload["hint"] = _string_value(target.get("hint"), "target.hint")
        return payload
    raise ConduitError(
        "target.strategy must be dominant, timerange, entity_id, or magic_hint",
        code="invalid_request",
    )


def _normalize_matching_context(context: str) -> str:
    if context != "hiring_team_fit":
        raise ConduitError("context must be hiring_team_fit", code="invalid_request")
    return context


def _normalize_matching_subject(subject: Mapping[str, object]) -> dict[str, object]:
    if "entityId" in subject:
        return {
            "type": "entity_id",
            "entityId": _string_value(subject.get("entityId"), "subject.entityId"),
        }
    if "mediaId" in subject:
        selector = subject.get("selector")
        if not isinstance(selector, Mapping):
            raise ConduitError(
                "subject.selector is required for mediaId refs", code="invalid_request"
            )
        return {
            "type": "media_target",
            "mediaId": _string_value(subject.get("mediaId"), "subject.mediaId"),
            "selector": _normalize_target_selector(
                cast("Mapping[str, object]", selector)
            ),
        }
    raise ConduitError(
        "subject must include entityId or mediaId with selector",
        code="invalid_request",
    )


def _ensure_unique_direct_entity_ids(
    target: Mapping[str, object],
    group: Sequence[Mapping[str, object]],
) -> None:
    entity_ids: set[str] = set()
    for subject in [target, *group]:
        if subject.get("type") != "entity_id":
            continue
        entity_id = cast("str", subject.get("entityId"))
        if entity_id in entity_ids:
            raise ConduitError(
                "target and group must reference different direct entity IDs",
                code="invalid_request",
            )
        entity_ids.add(entity_id)


def _normalize_webhook(
    webhook: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if webhook is None:
        return None
    url = _validate_http_url(cast("str", webhook.get("url")), "webhook.url")
    payload: dict[str, object] = {"url": url}
    headers = webhook.get("headers")
    if headers is not None:
        if not isinstance(headers, Mapping):
            raise ConduitError(
                "webhook.headers must be an object", code="invalid_request"
            )
        payload["headers"] = {
            str(key): _string_value(value, f"webhook.headers[{key}]")
            for key, value in cast("Mapping[object, object]", headers).items()
        }
    return payload


def _read_binary_like(
    value: bytes | bytearray | memoryview | BinaryIO,
) -> tuple[bytes, str]:
    if isinstance(value, bytes):
        return value, "upload.bin"
    if isinstance(value, bytearray):
        return bytes(value), "upload.bin"
    if isinstance(value, memoryview):
        return value.tobytes(), "upload.bin"
    filename = Path(getattr(value, "name", "upload.bin")).name or "upload.bin"
    payload = value.read()
    return payload, filename


def _resolve_label(label: str | None, filename: str, *, fallback: str) -> str:
    if label is not None:
        return _normalize_label(label)
    normalized = _normalize_label(filename)
    if normalized:
        return normalized
    return fallback


def _normalize_label(value: str) -> str:
    cleaned = LABEL_SUFFIX_REGEX.sub("", value).strip()
    if cleaned:
        return cleaned
    raise InvalidSourceError("label is required", code="invalid_source")


def _filename_from_url(value: str) -> str:
    parsed = urlparse(value)
    name = Path(parsed.path).name
    return name or "remote.bin"


def _strip_content_type(value: str | None) -> str | None:
    if value is None:
        return None
    return value.split(";", maxsplit=1)[0].strip() or None


def _get_header(
    headers: Mapping[str, str | Sequence[str] | None], name: str
) -> str | None:
    for key, value in headers.items():
        if key.lower() != name.lower():
            continue
        if value is None:
            return None
        if isinstance(value, str):
            return value
        values = list(value)
        if len(values) != 1:
            raise WebhookVerificationError(
                f"Duplicate {name} header",
                code="webhook_signature_invalid",
            )
        return values[0]
    return None


def _parse_signature_header(value: str) -> tuple[int, str]:
    fields: dict[str, str] = {}
    for raw_part in value.split(","):
        key, separator, part_value = raw_part.partition("=")
        if separator != "=" or not key or not part_value or key in fields:
            raise WebhookVerificationError(
                "Malformed conduit-signature header",
                code="webhook_signature_invalid",
            )
        if key not in {"t", "v1"}:
            raise WebhookVerificationError(
                "Malformed conduit-signature header",
                code="webhook_signature_invalid",
            )
        fields[key] = part_value
    if "t" not in fields or "v1" not in fields:
        raise WebhookVerificationError(
            "Malformed conduit-signature header",
            code="webhook_signature_invalid",
        )
    try:
        timestamp = int(fields["t"])
    except ValueError as exc:
        raise WebhookVerificationError(
            "Invalid signature timestamp",
            code="webhook_signature_invalid",
        ) from exc
    return timestamp, fields["v1"]


def _validate_iso8601(value: str, name: str) -> None:
    try:
        _ = datetime.fromisoformat(value).astimezone(UTC)
    except ValueError as exc:
        raise ConduitError(
            f"Invalid webhook payload: {name} must be an ISO8601 string",
            code="invalid_webhook_payload",
            cause=exc,
        ) from exc


def _validate_completed_data(data: object, *, resource_key: str) -> None:
    if not isinstance(data, Mapping):
        raise ConduitError(
            "Invalid webhook payload: data must be an object",
            code="invalid_webhook_payload",
        )
    data_map = cast("Mapping[str, object]", data)
    _ = _string_value(data_map.get("jobId"), "webhook.data.jobId")
    _ = _string_value(data_map.get(resource_key), f"webhook.data.{resource_key}")
    status = data_map.get("status")
    if status != "succeeded":
        raise ConduitError(
            "Invalid webhook payload: status must be succeeded",
            code="invalid_webhook_payload",
        )


def _validate_failed_data(data: object) -> None:
    if not isinstance(data, Mapping):
        raise ConduitError(
            "Invalid webhook payload: data must be an object",
            code="invalid_webhook_payload",
        )
    data_map = cast("Mapping[str, object]", data)
    _ = _string_value(data_map.get("jobId"), "webhook.data.jobId")
    if data_map.get("status") != "failed":
        raise ConduitError(
            "Invalid webhook payload: status must be failed",
            code="invalid_webhook_payload",
        )
    error = data_map.get("error")
    if not isinstance(error, Mapping):
        raise ConduitError(
            "Invalid webhook payload: error must be an object",
            code="invalid_webhook_payload",
        )
    error_map = cast("Mapping[str, object]", error)
    _ = _string_value(error_map.get("code"), "webhook.data.error.code")
    _ = _string_value(error_map.get("message"), "webhook.data.error.message")


__all__ = [
    "Conduit",
    "MatchingJobReceipt",
    "MatchingRunHandle",
    "ReportJobReceipt",
    "ReportRunHandle",
]

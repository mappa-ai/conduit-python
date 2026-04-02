"""Resource implementations for the Conduit Python SDK."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from urllib.parse import quote, urlparse

from .errors import (
    ConduitError,
    JobCanceledError,
    JobFailedError,
    WebhookVerificationError,
)
from .errors import TimeoutError as ConduitTimeoutError
from .models import (
    Entity,
    FileDeleteReceipt,
    Job,
    JobEvent,
    ListEntitiesResponse,
    ListFilesResponse,
    Matching,
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
    parse_report,
    parse_retention_lock,
    parse_webhook_event,
)
from .streaming import DEFAULT_STREAM_TIMEOUT_MS, iter_job_events

if TYPE_CHECKING:
    from ._transport import Transport
    from .sources import SourceManager
    from .types import (
        BinaryLike,
        CancelSignal,
        HeaderMapping,
        MatchingContext,
        MatchingSubjectRef,
        ReportOutput,
        ReportSource,
        TargetSelector,
        WebhookConfig,
    )

VALID_REPORT_TEMPLATES = {"general_report", "sales_playbook"}


@dataclass(slots=True)
class ReportRunHandle:
    """Secondary synchronous controls for a report job."""

    job_id: str
    _jobs: JobsResource
    _reports: ReportsResource

    def stream(
        self,
        *,
        timeout_ms: int | None = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Iterator[JobEvent]:
        """Yield job events until the job reaches a terminal state."""
        return self._jobs.stream(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            signal=signal,
        )

    def wait(
        self,
        *,
        timeout_ms: int = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Report:
        """Wait for the completed report resource."""
        job = self._jobs.wait(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            signal=signal,
        )
        if job.report_id is None:
            raise ConduitError(
                f"Job {self.job_id} succeeded but no reportId was returned",
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
    _jobs: JobsResource
    _matching: MatchingResource

    def stream(
        self,
        *,
        timeout_ms: int | None = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Iterator[JobEvent]:
        """Yield job events until the job reaches a terminal state."""
        return self._jobs.stream(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            signal=signal,
        )

    def wait(
        self,
        *,
        timeout_ms: int = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Matching:
        """Wait for the completed matching resource."""
        job = self._jobs.wait(
            self.job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            signal=signal,
        )
        if job.matching_id is None:
            raise ConduitError(
                f"Job {self.job_id} succeeded but no matchingId was returned",
                code="invalid_response",
            )
        return self._matching.get(job.matching_id)

    def cancel(self) -> Job:
        """Request cancellation for the matching job."""
        return self._jobs.cancel(self.job_id)

    def job(self) -> Job:
        """Fetch the latest job state."""
        return self._jobs.get(self.job_id)

    def matching(self) -> Matching | None:
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


class JobsResource:
    """Stable job lifecycle resource."""

    def __init__(self, transport: Transport) -> None:
        """Initialize the jobs resource."""
        self._transport = transport

    def get(self, job_id: str, *, request_id: str | None = None) -> Job:
        """Fetch the latest state for a job."""
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
        """Request cancellation for a job."""
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
        timeout_ms: int = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Job:
        """Wait for a job to reach a terminal state."""
        for event in self.stream(
            job_id,
            timeout_ms=timeout_ms,
            on_event=on_event,
            signal=signal,
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
        timeout_ms: int | None = DEFAULT_STREAM_TIMEOUT_MS,
        on_event: Callable[[JobEvent], None] | None = None,
        signal: CancelSignal | None = None,
    ) -> Iterator[JobEvent]:
        """Yield lifecycle events for a job."""
        return iter_job_events(
            self._transport,
            job_id,
            timeout_ms=timeout_ms,
            signal=signal,
            on_event=on_event,
        )


class EntitiesResource:
    """Stable entity primitives resource."""

    def __init__(self, transport: Transport) -> None:
        """Initialize the entities resource."""
        self._transport = transport

    def get(self, entity_id: str, *, request_id: str | None = None) -> Entity:
        """Fetch an entity by ID."""
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
        """List entities."""
        query = {"limit": str(limit or 20)}
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
        """Update an entity label."""
        payload = {"label": body.get("label")}
        response = self._transport.request(
            "PATCH",
            f"/v1/entities/{_path_segment(entity_id, 'entity_id')}",
            json_body=payload,
            request_id=request_id,
            retryable=True,
        )
        return parse_entity(response.data)


class MediaResource:
    """Stable media primitives resource."""

    def __init__(self, source_manager: SourceManager, transport: Transport) -> None:
        """Initialize the media resource."""
        self._source_manager = source_manager
        self._transport = transport

    def upload(
        self,
        *,
        file: object | None = None,
        url: str | None = None,
        path: str | None = None,
        label: str | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> MediaObject:
        """Upload media."""
        return self._source_manager.upload(
            file=_binary_like(file),
            url=url,
            path=path,
            label=label,
            idempotency_key=idempotency_key,
            request_id=request_id,
            signal=signal,
        )

    def get(self, media_id: str, *, request_id: str | None = None) -> MediaFile:
        """Fetch a media resource."""
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
        """List media resources."""
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
        """Delete a media resource."""
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
        """Set media retention lock status."""
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
        source: ReportSource,
        *,
        idempotency_key: str | None,
        request_id: str | None,
        signal: CancelSignal | None,
    ) -> str:
        """Resolve a report source into a media ID."""
        return self._source_manager.resolve_report_source(
            source,
            idempotency_key=idempotency_key,
            request_id=request_id,
            signal=signal,
        )


class ReportsResource:
    """Stable reports resource."""

    def __init__(
        self,
        transport: Transport,
        jobs: JobsResource,
        media: MediaResource,
    ) -> None:
        """Initialize the reports resource."""
        self._transport = transport
        self._jobs = jobs
        self._media = media

    def create(
        self,
        *,
        source: ReportSource,
        output: ReportOutput,
        target: TargetSelector,
        webhook: WebhookConfig | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> ReportJobReceipt:
        """Create a report generation job."""
        media_id = self._media.resolve_report_source(
            source,
            idempotency_key=idempotency_key,
            request_id=request_id,
            signal=signal,
        )
        _check_signal(signal, self._transport, request_id=request_id)
        body: dict[str, object] = {
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
            idempotency_key=idempotency_key,
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
        """Fetch a completed report resource."""
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
        """Fetch a report by job ID."""
        response = self._transport.request(
            "GET",
            f"/v1/reports/by-job/{_path_segment(job_id, 'job_id')}",
            request_id=request_id,
            retryable=True,
        )
        if response.data is None:
            return None
        return parse_report(response.data)


class MatchingResource:
    """Stable matching resource."""

    def __init__(self, transport: Transport, jobs: JobsResource) -> None:
        """Initialize the matching resource."""
        self._transport = transport
        self._jobs = jobs

    def create(
        self,
        *,
        context: MatchingContext | str,
        target: MatchingSubjectRef,
        group: Sequence[MatchingSubjectRef],
        webhook: WebhookConfig | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> MatchingJobReceipt:
        """Create a matching job."""
        _check_signal(signal, self._transport, request_id=request_id)
        target_payload = _normalize_matching_subject(target)
        group_payload = [_normalize_matching_subject(item) for item in group]
        if not group_payload:
            raise ConduitError(
                "group must contain at least one subject",
                code="invalid_request",
            )
        _ensure_unique_direct_entity_ids(target_payload, group_payload)
        body: dict[str, object] = {
            "context": _normalize_matching_context(context),
            "target": target_payload,
            "group": group_payload,
        }
        normalized_webhook = _normalize_webhook(webhook)
        if normalized_webhook is not None:
            body["webhook"] = normalized_webhook
        response = self._transport.request(
            "POST",
            "/v1/matching/jobs",
            json_body=body,
            idempotency_key=idempotency_key,
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

    def get(self, matching_id: str, *, request_id: str | None = None) -> Matching:
        """Fetch a completed matching resource."""
        response = self._transport.request(
            "GET",
            f"/v1/matching/{_path_segment(matching_id, 'matching_id')}",
            request_id=request_id,
            retryable=True,
        )
        return parse_matching(response.data)

    def get_by_job(
        self, job_id: str, *, request_id: str | None = None
    ) -> Matching | None:
        """Fetch a matching resource by job ID."""
        response = self._transport.request(
            "GET",
            f"/v1/matching/by-job/{_path_segment(job_id, 'job_id')}",
            request_id=request_id,
            retryable=True,
        )
        if response.data is None:
            return None
        return parse_matching(response.data)


class WebhooksResource:
    """Webhook verification and parsing helpers."""

    def verify_signature(
        self,
        payload: str | bytes,
        headers: HeaderMapping,
        secret: str,
        *,
        tolerance_sec: int = 300,
    ) -> bool:
        """Verify a webhook signature."""
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
        payload_bytes = (
            payload if isinstance(payload, bytes) else payload.encode("utf-8")
        )
        expected = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.".encode() + payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise WebhookVerificationError(
                "Invalid signature",
                code="webhook_signature_invalid",
            )
        return True

    def parse_event(self, payload: str | bytes) -> WebhookEvent:
        """Parse a webhook payload."""
        raw_text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        try:
            value = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ConduitError(
                "Invalid webhook payload: invalid JSON",
                code="invalid_webhook_payload",
                cause=exc,
            ) from exc
        event = parse_webhook_event(value)
        _validate_iso8601(event.created_at, "createdAt")
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


class PrimitivesResource:
    """Stable advanced primitives surface."""

    def __init__(
        self,
        *,
        entities: EntitiesResource,
        media: MediaResource,
        jobs: JobsResource,
    ) -> None:
        """Initialize the primitives resource."""
        self.entities = entities
        self.media = media
        self.jobs = jobs


def _path_segment(value: str, name: str) -> str:
    return quote(_string_value(value, name), safe="")


def _string_value(value: object, name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConduitError(f"{name} must be a non-empty string", code="invalid_request")


def _normalize_report_output(output: ReportOutput) -> dict[str, object]:
    template = _string_value(_required_value(output, "template"), "output.template")
    if template not in VALID_REPORT_TEMPLATES:
        raise ConduitError(
            "output.template must be general_report or sales_playbook",
            code="invalid_request",
        )
    payload: dict[str, object] = {"template": template}
    template_params = _optional_value(output, "template_params", "templateParams")
    if template_params is not None:
        template_params_map = _mapping_value(
            template_params,
            "output.template_params",
        )
        payload["templateParams"] = {
            str(key): value for key, value in template_params_map.items()
        }
    return payload


def _normalize_target_selector(target: TargetSelector) -> dict[str, object]:
    strategy = _string_value(_required_value(target, "strategy"), "target.strategy")
    on_miss = _optional_value(target, "on_miss", "onMiss")
    payload: dict[str, object] = {"strategy": strategy}
    if on_miss is not None:
        if on_miss not in {"fallback_dominant", "error"}:
            raise ConduitError(
                "target.on_miss must be fallback_dominant or error",
                code="invalid_request",
            )
        payload["on_miss"] = on_miss
    if strategy == "dominant":
        return payload
    if strategy == "timerange":
        return _normalize_timerange_target(target, payload)
    if strategy == "entity_id":
        payload["entity_id"] = _string_value(
            _required_value(target, "entity_id", "entityId"),
            "target.entity_id",
        )
        return payload
    if strategy == "magic_hint":
        payload["hint"] = _string_value(_required_value(target, "hint"), "target.hint")
        return payload
    raise ConduitError(
        "target.strategy must be dominant, timerange, entity_id, or magic_hint",
        code="invalid_request",
    )


def _normalize_timerange_target(
    target: TargetSelector,
    payload: dict[str, object],
) -> dict[str, object]:
    time_range = _mapping_value(
        _required_value(target, "time_range", "timeRange"),
        "target.time_range",
    )
    start = _optional_value(time_range, "start_seconds", "startSeconds")
    end = _optional_value(time_range, "end_seconds", "endSeconds")
    if start is None and end is None:
        raise ConduitError(
            "target.time_range must include start_seconds or end_seconds",
            code="invalid_request",
        )
    start_value = _optional_number(start)
    end_value = _optional_number(end)
    if start_value is not None and end_value is not None and start_value >= end_value:
        raise ConduitError(
            "target.time_range.start_seconds must be less than end_seconds",
            code="invalid_request",
        )
    payload["timerange"] = {
        "start_seconds": start_value,
        "end_seconds": end_value,
    }
    return payload


def _normalize_matching_context(context: MatchingContext | str) -> str:
    normalized = _string_value(context, "context")
    if normalized != "behavioral_compatibility":
        raise ConduitError(
            "context must be behavioral_compatibility", code="invalid_request"
        )
    return normalized


def _normalize_matching_subject(
    subject: MatchingSubjectRef,
) -> dict[str, object]:
    entity_id = _optional_value(subject, "entity_id", "entityId")
    media_id = _optional_value(subject, "media_id", "mediaId")
    if entity_id is not None and media_id is not None:
        raise ConduitError(
            "subject must include either entity_id or media_id with selector",
            code="invalid_request",
        )
    if entity_id is not None:
        return {
            "entityId": _string_value(entity_id, "subject.entity_id"),
            "type": "entity_id",
        }
    if media_id is not None:
        selector = _mapping_value(
            _required_value(subject, "selector"),
            "subject.selector",
        )
        return {
            "mediaId": _string_value(media_id, "subject.media_id"),
            "selector": _normalize_target_selector(cast("TargetSelector", selector)),
            "type": "media_target",
        }
    raise ConduitError(
        "subject must include entity_id or media_id with selector",
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
        entity_id = subject.get("entityId")
        if not isinstance(entity_id, str):
            continue
        if entity_id in entity_ids:
            raise ConduitError(
                "target and group must reference different direct entity IDs",
                code="invalid_request",
            )
        entity_ids.add(entity_id)


def _normalize_webhook(
    webhook: WebhookConfig | None,
) -> dict[str, object] | None:
    if webhook is None:
        return None
    url = _validate_http_url(_required_value(webhook, "url"), "webhook.url")
    payload: dict[str, object] = {"url": url}
    headers = _optional_value(webhook, "headers")
    if headers is not None:
        headers_map = _mapping_value(headers, "webhook.headers")
        payload["headers"] = {
            str(key): _string_value(value, f"webhook.headers[{key}]")
            for key, value in headers_map.items()
        }
    return payload


def _validate_http_url(value: object, name: str) -> str:
    normalized = _string_value(value, name)
    result = urlparse(normalized)
    if result.scheme not in {"http", "https"} or not result.netloc:
        raise ConduitError(
            f"{name} must be an http or https URL", code="invalid_request"
        )
    return normalized


def _required_value(mapping: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        if key in mapping:
            return mapping[key]
    raise ConduitError(f"Missing required field: {keys[0]}", code="invalid_request")


def _optional_value(mapping: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _mapping_value(value: object, name: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise ConduitError(f"{name} must be an object", code="invalid_request")


def _optional_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise ConduitError("Expected number", code="invalid_request")


def _binary_like(
    value: object | None,
) -> BinaryLike | None:
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        return cast("BinaryLike", value)
    if hasattr(value, "read"):
        return cast("BinaryLike", value)
    raise ConduitError("file must be binary", code="invalid_request")


def _check_signal(
    signal: CancelSignal | None,
    transport: Transport,
    *,
    request_id: str | None,
) -> None:
    if signal is not None and signal.is_set():
        raise transport.aborted(request_id=request_id)


def _get_header(headers: HeaderMapping, name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() != name.lower():
            continue
        if value is None:
            return None
        if isinstance(value, str):
            return value
        header_values = list(value)
        if len(header_values) != 1:
            raise WebhookVerificationError(
                f"Duplicate {name} header",
                code="webhook_signature_invalid",
            )
        return header_values[0]
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
        datetime.fromisoformat(value).astimezone(UTC)
    except ValueError as exc:
        raise ConduitError(
            f"Invalid webhook payload: {name} must be an ISO8601 string",
            code="invalid_webhook_payload",
            cause=exc,
        ) from exc


def _validate_completed_data(data: object, *, resource_key: str) -> None:
    data_map = _mapping_value(data, "webhook.data")
    _string_value(data_map.get("jobId"), "webhook.data.jobId")
    _string_value(data_map.get(resource_key), f"webhook.data.{resource_key}")
    if data_map.get("status") != "succeeded":
        raise ConduitError(
            "Invalid webhook payload: status must be succeeded",
            code="invalid_webhook_payload",
        )


def _validate_failed_data(data: object) -> None:
    data_map = _mapping_value(data, "webhook.data")
    _string_value(data_map.get("jobId"), "webhook.data.jobId")
    if data_map.get("status") != "failed":
        raise ConduitError(
            "Invalid webhook payload: status must be failed",
            code="invalid_webhook_payload",
        )
    error_map = _mapping_value(data_map.get("error"), "webhook.data.error")
    _string_value(error_map.get("code"), "webhook.data.error.code")
    _string_value(error_map.get("message"), "webhook.data.error.message")


__all__ = [
    "EntitiesResource",
    "JobsResource",
    "MatchingJobReceipt",
    "MatchingResource",
    "MatchingRunHandle",
    "MediaResource",
    "PrimitivesResource",
    "ReportJobReceipt",
    "ReportRunHandle",
    "ReportsResource",
    "WebhooksResource",
]

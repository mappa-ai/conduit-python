"""Structured models and parsers for the Conduit Python SDK."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from .errors import ConduitError


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise ConduitError(f"Invalid {name}: expected object", code="invalid_response")


def _optional_mapping(value: object) -> Mapping[str, object] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise ConduitError(
        "Invalid response: expected object or null",
        code="invalid_response",
    )


def _string(value: object, name: str) -> str:
    if isinstance(value, str):
        return value
    raise ConduitError(f"Invalid {name}: expected string", code="invalid_response")


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise ConduitError(
        "Invalid response: expected string or null",
        code="invalid_response",
    )


def _float(value: object, name: str) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise ConduitError(f"Invalid {name}: expected number", code="invalid_response")


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise ConduitError(
        "Invalid response: expected number or null",
        code="invalid_response",
    )


def _optional_int(value: object) -> int | None:
    number = _optional_float(value)
    if number is None:
        return None
    return int(number)


def _bool(value: object, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ConduitError(f"Invalid {name}: expected boolean", code="invalid_response")


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    raise ConduitError(
        "Invalid response: expected boolean or null",
        code="invalid_response",
    )


def _dict(value: object | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        mapping = cast("Mapping[object, Any]", value)
        return {str(key): item for key, item in mapping.items()}
    raise ConduitError(
        "Invalid response: expected object or null",
        code="invalid_response",
    )


def _first_value(data: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in data:
            return data[key]
    return None


@dataclass(slots=True)
class JobErrorData:
    """Structured job failure payload."""

    code: str
    message: str
    details: object | None = None
    retryable: bool | None = None


@dataclass(slots=True)
class Usage:
    """Job usage metadata."""

    credits_used: float
    credits_net_used: float
    credits_discounted: float | None = None
    duration_ms: float | None = None
    model_version: str | None = None


@dataclass(slots=True)
class JobCreditReservation:
    """Credit reservation metadata for a job."""

    reserved_credits: float | None
    reservation_status: str | None


@dataclass(slots=True)
class Job:
    """Job state."""

    id: str
    type: str
    status: str
    created_at: str
    updated_at: str
    stage: str | None = None
    progress: float | None = None
    report_id: str | None = None
    matching_id: str | None = None
    usage: Usage | None = None
    credits: JobCreditReservation | None = None
    released_credits: float | None = None
    error: JobErrorData | None = None
    request_id: str | None = None


@dataclass(slots=True)
class JobEvent:
    """Observed job lifecycle event."""

    type: str
    job: Job
    stage: str | None = None
    progress: float | None = None


@dataclass(slots=True)
class ReportOutputData:
    """Rendered report output."""

    template: str
    markdown: str | None = None
    json: dict[str, Any] | None = None
    report_url: str | None = None


@dataclass(slots=True)
class Report:
    """Completed report resource."""

    id: str
    created_at: str
    output: ReportOutputData
    job_id: str | None = None
    label: str | None = None
    entity_id: str | None = None
    entity_label: str | None = None
    media_id: str | None = None


@dataclass(slots=True)
class MatchingResolvedSubject:
    """Resolved matching subject metadata."""

    source: dict[str, Any]
    entity_id: str | None = None
    resolved_label: str | None = None


@dataclass(slots=True)
class MatchingOutputData:
    """Rendered matching output."""

    markdown: str | None = None
    json: dict[str, Any] | None = None


@dataclass(slots=True)
class Matching:
    """Completed matching resource."""

    id: str
    created_at: str
    context: str
    output: MatchingOutputData
    job_id: str | None = None
    label: str | None = None
    target: MatchingResolvedSubject | None = None
    group: list[MatchingResolvedSubject] | None = None


MatchingAnalysisResponse = Matching


@dataclass(slots=True)
class MediaObject:
    """Uploaded media receipt."""

    media_id: str
    created_at: str
    content_type: str
    label: str
    size_bytes: int | None = None
    duration_seconds: float | None = None


@dataclass(slots=True)
class MediaRetention:
    """Media retention policy."""

    expires_at: str | None
    days_remaining: int | None
    locked: bool


@dataclass(slots=True)
class MediaFile:
    """Stored media resource."""

    media_id: str
    created_at: str
    content_type: str
    has_reports: bool
    label: str
    processing_status: str
    last_used_at: str | None
    retention: MediaRetention
    size_bytes: int | None = None
    duration_seconds: float | None = None


@dataclass(slots=True)
class MediaSpeaker:
    """Diarized speaker summary for one media file."""

    speaker_index: int
    speech_seconds: float
    transcript: str


@dataclass(slots=True)
class MediaSpeakers:
    """Diarized speakers detected in one media file."""

    media_id: str
    status: str
    duration_seconds: float | None
    speakers: list[MediaSpeaker]


@dataclass(slots=True)
class FileDeleteReceipt:
    """Media delete receipt."""

    media_id: str
    deleted: bool


@dataclass(slots=True)
class RetentionLockResult:
    """Media retention lock update receipt."""

    media_id: str
    retention_lock: bool
    message: str


@dataclass(slots=True)
class ListFilesResponse:
    """Paginated media list response."""

    files: list[MediaFile]
    has_more: bool
    next_cursor: str | None


@dataclass(slots=True)
class Entity:
    """Stable speaker identity."""

    id: str
    created_at: str
    label: str | None
    media_count: float
    last_seen_at: str | None


@dataclass(slots=True)
class ListEntitiesResponse:
    """Paginated entity list response."""

    entities: list[Entity]
    has_more: bool
    cursor: str | None


@dataclass(slots=True)
class WebhookEvent:
    """Verified webhook event envelope."""

    id: str
    type: str
    created_at: str
    timestamp: str
    data: object


def parse_job(value: object) -> Job:
    """Parse a job response payload."""
    data = _mapping(value, "job")
    usage_data = data.get("usage")
    credits_data = data.get("credits")
    error_data = data.get("error")

    usage = None
    if usage_data is not None:
        usage_map = _mapping(usage_data, "job.usage")
        usage = Usage(
            credits_used=_float(usage_map.get("creditsUsed"), "job.usage.creditsUsed"),
            credits_net_used=_float(
                usage_map.get("creditsNetUsed"),
                "job.usage.creditsNetUsed",
            ),
            credits_discounted=_optional_float(usage_map.get("creditsDiscounted")),
            duration_ms=_optional_float(usage_map.get("durationMs")),
            model_version=_optional_string(usage_map.get("modelVersion")),
        )

    credit_reservation = None
    if credits_data is not None:
        credits_map = _mapping(credits_data, "job.credits")
        credit_reservation = JobCreditReservation(
            reserved_credits=_optional_float(credits_map.get("reservedCredits")),
            reservation_status=_optional_string(credits_map.get("reservationStatus")),
        )

    error = None
    if error_data is not None:
        error_map = _mapping(error_data, "job.error")
        error = JobErrorData(
            code=_string(error_map.get("code"), "job.error.code"),
            message=_string(error_map.get("message"), "job.error.message"),
            details=error_map.get("details"),
            retryable=_optional_bool(error_map.get("retryable")),
        )

    return Job(
        id=_string(data.get("id"), "job.id"),
        type=_string(data.get("type"), "job.type"),
        status=_string(data.get("status"), "job.status"),
        created_at=_string(data.get("createdAt"), "job.createdAt"),
        updated_at=_string(data.get("updatedAt"), "job.updatedAt"),
        stage=_optional_string(data.get("stage")),
        progress=_optional_float(data.get("progress")),
        report_id=_optional_string(data.get("reportId")),
        matching_id=_optional_string(data.get("matchingId")),
        usage=usage,
        credits=credit_reservation,
        released_credits=_optional_float(data.get("releasedCredits")),
        error=error,
        request_id=_optional_string(data.get("requestId")),
    )


def parse_report(value: object) -> Report:
    """Parse a report response payload."""
    data = _mapping(value, "report")
    output_data = _mapping(data.get("output"), "report.output")
    entity_data = _optional_mapping(data.get("entity"))
    media_data = _optional_mapping(data.get("media"))

    entity_id = _optional_string(_first_value(data, "entityId"))
    if entity_id is None and entity_data is not None:
        entity_id = _string(entity_data.get("id"), "report.entity.id")

    entity_label = _optional_string(_first_value(data, "entityLabel"))
    if entity_label is None and entity_data is not None:
        entity_label = _optional_string(entity_data.get("label"))

    media_id = _optional_string(_first_value(data, "mediaId"))
    if media_id is None and media_data is not None:
        media_id = _optional_string(media_data.get("mediaId"))

    report_url = _optional_string(output_data.get("reportUrl"))
    if report_url is None and media_data is not None:
        report_url = _optional_string(media_data.get("url"))

    output_markdown = _optional_string(output_data.get("markdown"))
    if output_markdown is None:
        output_markdown = _optional_string(data.get("markdown"))

    output_json = _dict(output_data.get("json"))
    if output_json is None:
        output_json = _dict(data.get("json"))

    return Report(
        id=_string(data.get("id"), "report.id"),
        created_at=_string(data.get("createdAt"), "report.createdAt"),
        job_id=_optional_string(data.get("jobId")),
        label=_optional_string(data.get("label")),
        entity_id=entity_id,
        entity_label=entity_label,
        media_id=media_id,
        output=ReportOutputData(
            template=_string(output_data.get("template"), "report.output.template"),
            markdown=output_markdown,
            json=output_json,
            report_url=report_url,
        ),
    )


def parse_matching_subject(value: object) -> MatchingResolvedSubject:
    """Parse a resolved matching subject payload."""
    data = _mapping(value, "matching subject")
    source = _mapping(data.get("source"), "matching subject.source")
    return MatchingResolvedSubject(
        source={str(key): item for key, item in source.items()},
        entity_id=_optional_string(_first_value(data, "entityId", "entity_id")),
        resolved_label=_optional_string(
            _first_value(data, "resolvedLabel", "resolved_label")
        ),
    )


def parse_matching(value: object) -> Matching:
    """Parse a matching response payload."""
    data = _mapping(value, "matching")
    output_data = _optional_mapping(data.get("output")) or {}

    group_value = data.get("group")
    group = None
    if group_value is not None:
        if not isinstance(group_value, list):
            raise ConduitError("Invalid matching.group", code="invalid_response")
        group_items = cast("list[object]", group_value)
        group = [parse_matching_subject(item) for item in group_items]

    target_value = data.get("target")
    target = parse_matching_subject(target_value) if target_value is not None else None

    output_markdown = _optional_string(output_data.get("markdown"))
    if output_markdown is None:
        output_markdown = _optional_string(data.get("markdown"))

    output_json = _dict(output_data.get("json"))
    if output_json is None:
        output_json = _dict(data.get("json"))

    return Matching(
        id=_string(data.get("id"), "matching.id"),
        created_at=_string(data.get("createdAt"), "matching.createdAt"),
        context=_string(data.get("context"), "matching.context"),
        job_id=_optional_string(data.get("jobId")),
        label=_optional_string(data.get("label")),
        target=target,
        group=group,
        output=MatchingOutputData(
            markdown=output_markdown,
            json=output_json,
        ),
    )


def parse_media_object(value: object) -> MediaObject:
    """Parse a media upload receipt."""
    data = _mapping(value, "media")
    return MediaObject(
        media_id=_string(data.get("mediaId"), "media.mediaId"),
        created_at=_string(data.get("createdAt"), "media.createdAt"),
        content_type=_string(data.get("contentType"), "media.contentType"),
        label=_string(data.get("label"), "media.label"),
        size_bytes=_optional_int(data.get("sizeBytes")),
        duration_seconds=_optional_float(data.get("durationSeconds")),
    )


def parse_media_file(value: object) -> MediaFile:
    """Parse a media detail payload."""
    data = _mapping(value, "media file")
    retention_data = _mapping(data.get("retention"), "media.retention")
    return MediaFile(
        media_id=_string(data.get("mediaId"), "media.mediaId"),
        created_at=_string(data.get("createdAt"), "media.createdAt"),
        content_type=_string(data.get("contentType"), "media.contentType"),
        has_reports=_bool(data.get("hasReports"), "media.hasReports"),
        label=_string(data.get("label"), "media.label"),
        processing_status=_string(
            data.get("processingStatus"),
            "media.processingStatus",
        ),
        last_used_at=_optional_string(data.get("lastUsedAt")),
        retention=MediaRetention(
            expires_at=_optional_string(retention_data.get("expiresAt")),
            days_remaining=_optional_int(retention_data.get("daysRemaining")),
            locked=_bool(retention_data.get("locked"), "media.retention.locked"),
        ),
        size_bytes=_optional_int(data.get("sizeBytes")),
        duration_seconds=_optional_float(data.get("durationSeconds")),
    )


def parse_list_files(value: object) -> ListFilesResponse:
    """Parse a paginated media list."""
    data = _mapping(value, "files list")
    files = data.get("files")
    if not isinstance(files, list):
        raise ConduitError(
            "Invalid files list: expected files array",
            code="invalid_response",
        )
    file_items = cast("list[object]", files)
    return ListFilesResponse(
        files=[parse_media_file(item) for item in file_items],
        has_more=_bool(data.get("hasMore"), "files.hasMore"),
        next_cursor=_optional_string(data.get("nextCursor")),
    )


def parse_media_speakers(value: object) -> MediaSpeakers:
    """Parse diarized speaker summaries for one media file."""
    data = _mapping(value, "media speakers")
    speakers = data.get("speakers")
    if not isinstance(speakers, list):
        raise ConduitError(
            "Invalid media speakers: expected speakers array",
            code="invalid_response",
        )
    speaker_items = cast("list[object]", speakers)
    return MediaSpeakers(
        media_id=_string(data.get("mediaId"), "speakers.mediaId"),
        status=_string(data.get("status"), "speakers.status"),
        duration_seconds=_optional_float(data.get("durationSeconds")),
        speakers=[_parse_media_speaker(item) for item in speaker_items],
    )


def _parse_media_speaker(value: object) -> MediaSpeaker:
    data = _mapping(value, "media speaker")
    return MediaSpeaker(
        speaker_index=int(_float(data.get("speakerIndex"), "speaker.speakerIndex")),
        speech_seconds=_float(data.get("speechSeconds"), "speaker.speechSeconds"),
        transcript=_string(data.get("transcript"), "speaker.transcript"),
    )


def parse_delete_receipt(value: object) -> FileDeleteReceipt:
    """Parse a media delete receipt."""
    data = _mapping(value, "delete receipt")
    return FileDeleteReceipt(
        media_id=_string(data.get("mediaId"), "delete.mediaId"),
        deleted=_bool(data.get("deleted"), "delete.deleted"),
    )


def parse_retention_lock(value: object) -> RetentionLockResult:
    """Parse a retention lock update receipt."""
    data = _mapping(value, "retention result")
    return RetentionLockResult(
        media_id=_string(data.get("mediaId"), "retention.mediaId"),
        retention_lock=_bool(data.get("retentionLock"), "retention.retentionLock"),
        message=_string(data.get("message"), "retention.message"),
    )


def parse_entity(value: object) -> Entity:
    """Parse an entity payload."""
    data = _mapping(value, "entity")
    return Entity(
        id=_string(data.get("id"), "entity.id"),
        created_at=_string(data.get("createdAt"), "entity.createdAt"),
        label=_optional_string(data.get("label")),
        media_count=_float(data.get("mediaCount"), "entity.mediaCount"),
        last_seen_at=_optional_string(data.get("lastSeenAt")),
    )


def parse_list_entities(value: object) -> ListEntitiesResponse:
    """Parse a paginated entity list."""
    data = _mapping(value, "entities list")
    entities = data.get("entities")
    if not isinstance(entities, list):
        raise ConduitError(
            "Invalid entities list: expected entities array",
            code="invalid_response",
        )
    entity_items = cast("list[object]", entities)
    return ListEntitiesResponse(
        entities=[parse_entity(item) for item in entity_items],
        has_more=_bool(data.get("hasMore"), "entities.hasMore"),
        cursor=_optional_string(data.get("cursor")),
    )


def parse_webhook_event(value: object) -> WebhookEvent:
    """Parse a webhook event envelope."""
    data = _mapping(value, "webhook event")
    return WebhookEvent(
        id=_string(data.get("id"), "webhook.id"),
        type=_string(data.get("type"), "webhook.type"),
        created_at=_string(data.get("createdAt"), "webhook.createdAt"),
        timestamp=_string(data.get("timestamp"), "webhook.timestamp"),
        data=data.get("data"),
    )


def parse_job_receipt(value: object) -> tuple[str, str, str | None, float | None]:
    """Parse a job receipt payload."""
    data = _mapping(value, "job receipt")
    return (
        _string(data.get("jobId"), "jobReceipt.jobId"),
        _string(data.get("status"), "jobReceipt.status"),
        _optional_string(data.get("stage")),
        _optional_float(data.get("estimatedWaitSec")),
    )


__all__ = [
    "Entity",
    "FileDeleteReceipt",
    "Job",
    "JobCreditReservation",
    "JobErrorData",
    "JobEvent",
    "ListEntitiesResponse",
    "ListFilesResponse",
    "Matching",
    "MatchingAnalysisResponse",
    "MatchingOutputData",
    "MatchingResolvedSubject",
    "MediaFile",
    "MediaObject",
    "MediaRetention",
    "MediaSpeaker",
    "MediaSpeakers",
    "Report",
    "ReportOutputData",
    "RetentionLockResult",
    "Usage",
    "WebhookEvent",
    "parse_delete_receipt",
    "parse_entity",
    "parse_job",
    "parse_job_receipt",
    "parse_list_entities",
    "parse_list_files",
    "parse_matching",
    "parse_matching_subject",
    "parse_media_file",
    "parse_media_object",
    "parse_media_speakers",
    "parse_report",
    "parse_retention_lock",
    "parse_webhook_event",
]

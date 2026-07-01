"""Public request and telemetry types for the Conduit Python SDK."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, BinaryIO, Literal, NotRequired, Protocol, TypedDict

REPORT_TEMPLATE_GENERAL_REPORT = "general_report"
REPORT_TEMPLATE_SALES_PLAYBOOK = "sales_playbook"
MATCHING_CONTEXT_BEHAVIORAL_COMPATIBILITY = "behavioral_compatibility"

ReportTemplate = Literal["general_report", "sales_playbook"]
ReportLanguage = Literal["en", "es", "pt"]
MatchingContext = Literal["behavioral_compatibility"]
OnMiss = Literal["error", "fallback_dominant"]
TargetStrategy = Literal["dominant", "timerange", "entity_id", "magic_hint"]

BinaryLike = bytes | bytearray | memoryview | BinaryIO
HeaderValue = str | Sequence[str] | None
HeaderMapping = Mapping[str, HeaderValue]


class CancelSignal(Protocol):
    """Minimal cancellation protocol for sync SDK operations."""

    def is_set(self) -> bool:
        """Return whether the caller requested cancellation."""
        ...


@dataclass(slots=True)
class RequestTelemetry:
    """Outbound request telemetry event."""

    method: str
    request_id: str
    url: str


@dataclass(slots=True)
class ResponseTelemetry:
    """Inbound response telemetry event."""

    duration_ms: float
    method: str
    request_id: str
    status: int
    url: str


@dataclass(slots=True)
class ErrorTelemetry:
    """Transport or streaming error telemetry event."""

    duration_ms: float
    error: BaseException
    method: str
    request_id: str
    url: str


@dataclass(slots=True)
class TelemetryHooks:
    """Non-invasive SDK lifecycle hooks."""

    on_error: Callable[[ErrorTelemetry], None] | None = None
    on_request: Callable[[RequestTelemetry], None] | None = None
    on_response: Callable[[ResponseTelemetry], None] | None = None


class WebhookConfig(TypedDict, total=False):
    """Webhook delivery target configuration."""

    headers: dict[str, str]
    url: str


class ReportSourceMedia(TypedDict):
    """Reference pre-uploaded media for report creation."""

    media_id: str


class ReportSourceFile(TypedDict, total=False):
    """Upload bytes or a binary file handle for report creation."""

    file: BinaryLike
    label: str


class ReportSourceUrl(TypedDict, total=False):
    """Fetch a remote media URL before report creation."""

    label: str
    url: str


class ReportSourcePath(TypedDict, total=False):
    """Upload a local filesystem path for report creation."""

    label: str
    path: str


ReportSource = ReportSourceMedia | ReportSourceFile | ReportSourceUrl | ReportSourcePath


class ReportOutput(TypedDict):
    """Requested report rendering configuration."""

    template: ReportTemplate
    template_params: NotRequired[dict[str, Any]]


class TimeRange(TypedDict, total=False):
    """Target selection time range."""

    end_seconds: float
    start_seconds: float


class TargetDominant(TypedDict, total=False):
    """Select the dominant speaker."""

    on_miss: OnMiss
    strategy: Literal["dominant"]


class TargetTimeRange(TypedDict, total=False):
    """Select a speaker using a time range hint."""

    on_miss: OnMiss
    strategy: Literal["timerange"]
    time_range: TimeRange


class TargetEntityId(TypedDict, total=False):
    """Select a stable entity for report generation."""

    entity_id: str
    on_miss: OnMiss
    strategy: Literal["entity_id"]


class TargetMagicHint(TypedDict, total=False):
    """Select a speaker with a natural-language hint."""

    hint: str
    on_miss: OnMiss
    strategy: Literal["magic_hint"]


TargetSelector = TargetDominant | TargetTimeRange | TargetEntityId | TargetMagicHint


class MatchingEntityRef(TypedDict):
    """Reference a stable entity in matching."""

    entity_id: str


class MatchingMediaRef(TypedDict):
    """Reference a media target in matching."""

    media_id: str
    selector: TargetSelector


MatchingSubjectRef = MatchingEntityRef | MatchingMediaRef


__all__ = [
    "MATCHING_CONTEXT_BEHAVIORAL_COMPATIBILITY",
    "REPORT_TEMPLATE_GENERAL_REPORT",
    "REPORT_TEMPLATE_SALES_PLAYBOOK",
    "BinaryLike",
    "CancelSignal",
    "ErrorTelemetry",
    "HeaderMapping",
    "HeaderValue",
    "MatchingContext",
    "MatchingEntityRef",
    "MatchingMediaRef",
    "MatchingSubjectRef",
    "OnMiss",
    "ReportLanguage",
    "ReportOutput",
    "ReportSource",
    "ReportSourceFile",
    "ReportSourceMedia",
    "ReportSourcePath",
    "ReportSourceUrl",
    "ReportTemplate",
    "RequestTelemetry",
    "ResponseTelemetry",
    "TargetDominant",
    "TargetEntityId",
    "TargetMagicHint",
    "TargetSelector",
    "TargetStrategy",
    "TargetTimeRange",
    "TelemetryHooks",
    "TimeRange",
    "WebhookConfig",
]

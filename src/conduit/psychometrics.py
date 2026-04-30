"""Public psychometrics resource for the Conduit Python SDK."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict, cast
from urllib.parse import quote

from .errors import ConduitError, InvalidSourceError

if TYPE_CHECKING:
    from ._transport import Transport
    from .sources import SourceManager
    from .types import BinaryLike, CancelSignal


PsychometricsTargetStrategy = Literal["dominant", "magic_hint"]


class PsychometricsSourceFile(TypedDict, total=False):
    """Upload bytes or a binary file handle for psychometrics analysis."""

    file: BinaryLike
    label: str


class PsychometricsSourceUrl(TypedDict, total=False):
    """Fetch a remote media URL before psychometrics analysis."""

    label: str
    url: str


class PsychometricsSourcePath(TypedDict, total=False):
    """Upload a local filesystem path for psychometrics analysis."""

    label: str
    path: str


PsychometricsSource = (
    PsychometricsSourceFile | PsychometricsSourceUrl | PsychometricsSourcePath
)


class PsychometricsTargetDominant(TypedDict):
    """Select the dominant speaker for psychometrics analysis."""

    strategy: Literal["dominant"]


class PsychometricsTargetMagicHint(TypedDict):
    """Select a speaker using a natural-language hint."""

    hint: str
    strategy: Literal["magic_hint"]


PsychometricsTargetSelector = (
    PsychometricsTargetDominant | PsychometricsTargetMagicHint
)


@dataclass(slots=True)
class PsychometricsConfidence:
    """Confidence metadata returned by the psychometrics endpoint."""

    overall: float
    source: Literal["signal_heuristic"]


@dataclass(slots=True)
class PsychometricsQuality:
    """Quality metadata returned by the psychometrics endpoint."""

    segment_count: int
    signal: Literal["low", "medium", "high"]
    source_audio_duration_seconds: float
    speaker_coverage_ratio: float
    target_audio_duration_seconds: float
    target_utterance_count: int


@dataclass(slots=True)
class PsychometricsSelectedSpeaker:
    """Resolved speaker selection for the psychometrics analysis."""

    speaker_index: int
    strategy: PsychometricsTargetStrategy


@dataclass(slots=True)
class PsychometricsModelInfo:
    """Model metadata returned by the psychometrics endpoint."""

    metadata: dict[str, str]
    version: str | None


@dataclass(slots=True)
class PsychometricsResult:
    """Completed psychometrics analysis."""

    analysis_id: str
    confidence: PsychometricsConfidence
    created_at: str
    expires_at: str
    model: PsychometricsModelInfo
    psychometrics: dict[str, float]
    quality: PsychometricsQuality
    selected_speaker: PsychometricsSelectedSpeaker


class PsychometricsResource:
    """Stable sync psychometrics workflow."""

    def __init__(self, source_manager: SourceManager, transport: Transport) -> None:
        """Initialize the psychometrics resource."""
        self._source_manager = source_manager
        self._transport = transport

    def create(
        self,
        *,
        source: PsychometricsSource,
        target: PsychometricsTargetSelector,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> PsychometricsResult:
        """Create a psychometrics analysis and return the completed result."""
        _check_signal(signal, self._transport, request_id=request_id)
        source_map = _normalize_source_mapping(source)
        materialized = self._source_manager.materialize_upload_source(
            file=_binary_like_value(source_map.get("file")),
            url=_optional_request_string(source_map.get("url")),
            path=_optional_request_string(source_map.get("path")),
            label=_optional_request_string(source_map.get("label")),
        )
        try:
            target_map = _normalize_target_mapping(target)
            data = {"strategy": _target_strategy(target_map)}
            hint = _target_hint(target_map)
            if hint is not None:
                data["hint"] = hint
            response = self._transport.request(
                "POST",
                "/v2/psychometrics",
                data=data,
                files={
                    "file": (
                        materialized.filename,
                        materialized.file_value,
                        materialized.content_type,
                    )
                },
                idempotency_key=idempotency_key,
                request_id=request_id,
                retryable=False,
            )
        finally:
            materialized.close()
        return parse_psychometrics_result(response.data)

    def get(
        self,
        analysis_id: str,
        *,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> PsychometricsResult:
        """Fetch a previously completed psychometrics analysis by ID."""
        _check_signal(signal, self._transport, request_id=request_id)
        response = self._transport.request(
            "GET",
            "/v2/psychometrics/"
            + quote(_string_value(analysis_id, "analysis_id"), safe=""),
            request_id=request_id,
            retryable=True,
        )
        return parse_psychometrics_result(response.data)


def parse_psychometrics_result(value: object) -> PsychometricsResult:
    """Parse a completed psychometrics response payload."""
    data = _mapping(value, "psychometrics result")
    confidence_data = _mapping(data.get("confidence"), "psychometrics.confidence")
    quality_data = _mapping(data.get("quality"), "psychometrics.quality")
    model_data = _mapping(data.get("model"), "psychometrics.model")
    selected_speaker_data = _mapping(
        data.get("selectedSpeaker"),
        "psychometrics.selectedSpeaker",
    )
    metadata_data = _mapping(model_data.get("metadata"), "psychometrics.model.metadata")
    return PsychometricsResult(
        analysis_id=_string_value(data.get("analysisId"), "analysisId"),
        confidence=PsychometricsConfidence(
            overall=_float_value(confidence_data.get("overall"), "confidence.overall"),
            source=_signal_source(confidence_data.get("source"), "confidence.source"),
        ),
        created_at=_string_value(data.get("createdAt"), "createdAt"),
        expires_at=_string_value(data.get("expiresAt"), "expiresAt"),
        model=PsychometricsModelInfo(
            metadata=_string_dict(metadata_data, "model.metadata"),
            version=_optional_string(model_data.get("version")),
        ),
        psychometrics=_float_dict(data.get("psychometrics"), "psychometrics"),
        quality=PsychometricsQuality(
            segment_count=_int_value(
                quality_data.get("segmentCount"),
                "quality.segmentCount",
            ),
            signal=_quality_signal(quality_data.get("signal"), "quality.signal"),
            source_audio_duration_seconds=_float_value(
                quality_data.get("sourceAudioDurationSeconds"),
                "quality.sourceAudioDurationSeconds",
            ),
            speaker_coverage_ratio=_float_value(
                quality_data.get("speakerCoverageRatio"),
                "quality.speakerCoverageRatio",
            ),
            target_audio_duration_seconds=_float_value(
                quality_data.get("targetAudioDurationSeconds"),
                "quality.targetAudioDurationSeconds",
            ),
            target_utterance_count=_int_value(
                quality_data.get("targetUtteranceCount"),
                "quality.targetUtteranceCount",
            ),
        ),
        selected_speaker=PsychometricsSelectedSpeaker(
            speaker_index=_int_value(
                selected_speaker_data.get("speakerIndex"),
                "selectedSpeaker.speakerIndex",
            ),
            strategy=_target_strategy_literal(
                selected_speaker_data.get("strategy"),
                "selectedSpeaker.strategy",
            ),
        ),
    )


def _binary_like_value(value: object | None) -> BinaryLike | None:
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        return cast("BinaryLike", value)
    if hasattr(value, "read"):
        return cast("BinaryLike", value)
    raise ConduitError("source.file must be binary", code="invalid_request")


def _check_signal(
    signal: CancelSignal | None,
    transport: Transport,
    *,
    request_id: str | None,
) -> None:
    if signal is not None and signal.is_set():
        raise transport.aborted(request_id=request_id)


def _float_dict(value: object, name: str) -> dict[str, float]:
    mapping = _mapping(value, name)
    normalized: dict[str, float] = {}
    for key, item in mapping.items():
        normalized[str(key)] = _float_value(item, f"{name}[{key}]")
    return normalized


def _float_value(value: object, name: str) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    raise ConduitError(f"Invalid {name}: expected number", code="invalid_response")


def _int_value(value: object, name: str) -> int:
    number = _float_value(value, name)
    return int(number)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise ConduitError(f"Invalid {name}: expected object", code="invalid_response")


def _normalize_source_mapping(source: PsychometricsSource) -> dict[str, object]:
    normalized = {str(key): value for key, value in source.items()}
    keys = [key for key in ("file", "url", "path") if key in normalized]
    if len(keys) == 1:
        return normalized
    raise InvalidSourceError(
        "source must include exactly one of file, url, or path",
        code="invalid_source",
    )


def _normalize_target_mapping(
    target: PsychometricsTargetSelector,
) -> dict[str, object]:
    normalized = {str(key): value for key, value in target.items()}
    strategy = _request_target_strategy(normalized)
    if strategy == "dominant":
        return normalized
    hint = normalized.get("hint")
    if not isinstance(hint, str) or not hint.strip():
        raise ConduitError(
            "target.hint is required for magic_hint",
            code="invalid_request",
        )
    return normalized


def _optional_request_string(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise ConduitError(
        "request field must be a string when provided",
        code="invalid_request",
    )


def _optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise ConduitError(
        "Invalid response: expected string or null",
        code="invalid_response",
    )


def _quality_signal(
    value: object,
    name: str,
) -> Literal["low", "medium", "high"]:
    if value in {"low", "medium", "high"}:
        return cast('Literal["low", "medium", "high"]', value)
    raise ConduitError(
        f"Invalid {name}: expected low, medium, or high",
        code="invalid_response",
    )


def _signal_source(value: object, name: str) -> Literal["signal_heuristic"]:
    if value == "signal_heuristic":
        return "signal_heuristic"
    raise ConduitError(
        f"Invalid {name}: expected signal_heuristic",
        code="invalid_response",
    )


def _string_dict(value: object, name: str) -> dict[str, str]:
    mapping = _mapping(value, name)
    normalized: dict[str, str] = {}
    for key, item in mapping.items():
        normalized[str(key)] = _string_value(item, f"{name}[{key}]")
    return normalized


def _string_value(value: object, name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConduitError(
        f"Invalid {name}: expected non-empty string",
        code="invalid_response",
    )


def _target_hint(target: Mapping[str, object]) -> str | None:
    hint = target.get("hint")
    if hint is None:
        return None
    if isinstance(hint, str) and hint.strip():
        return hint.strip()
    raise ConduitError(
        "target.hint is required for magic_hint",
        code="invalid_request",
    )


def _request_target_strategy(
    target: Mapping[str, object],
) -> PsychometricsTargetStrategy:
    strategy = target.get("strategy")
    if strategy in {"dominant", "magic_hint"}:
        return cast("PsychometricsTargetStrategy", strategy)
    raise ConduitError(
        "target.strategy must be dominant or magic_hint",
        code="invalid_request",
    )


def _target_strategy(target: Mapping[str, object]) -> PsychometricsTargetStrategy:
    strategy = target.get("strategy")
    return _target_strategy_literal(strategy, "target.strategy")


def _target_strategy_literal(
    value: object,
    name: str,
) -> PsychometricsTargetStrategy:
    if value in {"dominant", "magic_hint"}:
        return cast("PsychometricsTargetStrategy", value)
    raise ConduitError(
        f"Invalid {name}: expected dominant or magic_hint",
        code="invalid_response",
    )


__all__ = [
    "PsychometricsConfidence",
    "PsychometricsModelInfo",
    "PsychometricsQuality",
    "PsychometricsResource",
    "PsychometricsResult",
    "PsychometricsSelectedSpeaker",
    "PsychometricsSource",
    "PsychometricsSourceFile",
    "PsychometricsSourcePath",
    "PsychometricsSourceUrl",
    "PsychometricsTargetDominant",
    "PsychometricsTargetMagicHint",
    "PsychometricsTargetSelector",
    "PsychometricsTargetStrategy",
    "parse_psychometrics_result",
]

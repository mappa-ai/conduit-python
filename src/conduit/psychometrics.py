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
class PsychometricsResult:
    """Completed psychometrics analysis."""

    analysis_id: str
    created_at: str
    expires_at: str
    psychometrics: dict[str, float]


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
    return PsychometricsResult(
        analysis_id=_string_value(data.get("analysisId"), "analysisId"),
        created_at=_string_value(data.get("createdAt"), "createdAt"),
        expires_at=_string_value(data.get("expiresAt"), "expiresAt"),
        psychometrics=_float_dict(data.get("psychometrics"), "psychometrics"),
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
    "PsychometricsResource",
    "PsychometricsResult",
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

"""Internal-only client surface for behavior-map exports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self, cast
from urllib.parse import quote, urlparse

from ._transport import Transport
from .errors import ConduitError, InitializationError

if TYPE_CHECKING:
    import httpx

    from .types import TelemetryHooks


@dataclass(slots=True)
class BehaviorMapExport:
    """Latest persisted prompt-facing behavior map for an entity."""

    behavior_map: dict[str, Any]
    created_at: str
    entity_id: str
    map_id: str
    model_checkpoint: str


class InternalBehaviorMapsResource:
    """Read-only internal behavior-map resource."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def get(
        self, entity_id: str, *, request_id: str | None = None
    ) -> BehaviorMapExport:
        """Fetch the latest persisted behavior map for an entity."""
        if not entity_id:
            raise ConduitError(
                "entity_id must be a non-empty string", code="invalid_request"
            )

        response = self._transport.request(
            "GET",
            f"/internal/behavior-maps/{quote(entity_id, safe='')}",
            request_id=request_id,
            retryable=True,
        )
        return _parse_behavior_map_export(response.data)


class InternalConduit:
    """Internal-only Conduit client for trusted backend integrations."""

    def __init__(
        self,
        *,
        internal_api_key: str,
        base_url: str = "https://api.mappa.ai",
        timeout_ms: int = 300000,
        max_retries: int = 2,
        user_agent: str | None = None,
        http_client: httpx.Client | None = None,
        telemetry: TelemetryHooks | None = None,
    ) -> None:
        """Initialize an internal Conduit client."""
        if not internal_api_key:
            raise InitializationError(
                "internal_api_key is required", code="config_error"
            )
        _validate_base_url(base_url)
        self._transport = Transport(
            api_key=internal_api_key,
            auth_header_name="x-internal-key",
            base_url=base_url,
            timeout_ms=timeout_ms,
            max_retries=max_retries,
            user_agent=user_agent,
            http_client=http_client,
            telemetry=telemetry,
        )
        self.behavior_maps = InternalBehaviorMapsResource(self._transport)

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


def _parse_behavior_map_export(payload: object) -> BehaviorMapExport:
    data = _mapping(payload, "behavior map export")
    behavior_map = _mapping(data.get("behaviorMap"), "behaviorMap")
    return BehaviorMapExport(
        behavior_map=dict(cast("dict[str, Any]", behavior_map)),
        created_at=_non_empty_string(data.get("createdAt"), "createdAt"),
        entity_id=_non_empty_string(data.get("entityId"), "entityId"),
        map_id=_non_empty_string(data.get("mapId"), "mapId"),
        model_checkpoint=_non_empty_string(
            data.get("modelCheckpoint"), "modelCheckpoint"
        ),
    )


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise ConduitError(f"Invalid {name}: expected object", code="invalid_response")


def _non_empty_string(value: object, name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise ConduitError(
        f"Invalid {name}: expected non-empty string", code="invalid_response"
    )


__all__ = ["BehaviorMapExport", "InternalConduit"]

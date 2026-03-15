"""Public client surface for the Conduit Python SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self
from urllib.parse import urlparse

from ._transport import Transport
from .errors import InitializationError
from .resources import (
    EntitiesResource,
    JobsResource,
    MatchingJobReceipt,
    MatchingResource,
    MatchingRunHandle,
    MediaResource,
    PrimitivesResource,
    ReportJobReceipt,
    ReportRunHandle,
    ReportsResource,
    WebhooksResource,
)
from .sources import DEFAULT_MAX_SOURCE_BYTES, SourceManager

if TYPE_CHECKING:
    import httpx

    from .types import TelemetryHooks


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
        http_client: httpx.Client | None = None,
        telemetry: TelemetryHooks | None = None,
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
            http_client=http_client,
            telemetry=telemetry,
        )
        jobs = JobsResource(self._transport)
        source_manager = SourceManager(
            self._transport,
            timeout_ms=timeout_ms,
            max_source_bytes=max_source_bytes,
        )
        media = MediaResource(source_manager, self._transport)
        self.reports = ReportsResource(self._transport, jobs, media)
        self.matching = MatchingResource(self._transport, jobs)
        self.webhooks = WebhooksResource()
        self.primitives = PrimitivesResource(
            entities=EntitiesResource(self._transport),
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


__all__ = [
    "Conduit",
    "MatchingJobReceipt",
    "MatchingRunHandle",
    "ReportJobReceipt",
    "ReportRunHandle",
]

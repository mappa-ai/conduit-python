"""Error types for the Conduit Python SDK."""

from __future__ import annotations

from dataclasses import dataclass


class ConduitError(Exception):
    """Base SDK error."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "conduit_error",
        request_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.__cause__ = cause


class InitializationError(ConduitError):
    """Raised when the client is configured incorrectly."""


class UnsupportedRuntimeError(ConduitError):
    """Raised when a runtime capability is unavailable."""


class WebhookVerificationError(ConduitError):
    """Raised when webhook signature verification fails."""


class SourceError(ConduitError):
    """Base error for source materialization and upload failures."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "source_error",
        request_id: str | None = None,
        cause: BaseException | None = None,
        url: str | None = None,
        status: int | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(
            message,
            code=code,
            request_id=request_id,
            cause=cause,
        )
        self.url = url
        self.status = status


class InvalidSourceError(SourceError):
    """Raised when a source payload is invalid before any network call."""


class RemoteFetchError(SourceError):
    """Raised when remote source fetching fails."""


class RemoteFetchTimeoutError(RemoteFetchError):
    """Raised when remote source fetching times out."""


class RemoteFetchTooLargeError(RemoteFetchError):
    """Raised when a remote source exceeds the configured size limit."""


class ApiError(ConduitError):
    """Base HTTP API error."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str = "api_error",
        request_id: str | None = None,
        details: object | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(message, code=code, request_id=request_id)
        self.status = status
        self.details = details


class AuthError(ApiError):
    """Raised for 401 and 403 responses."""


class ValidationError(ApiError):
    """Raised for 422 responses."""


class RateLimitError(ApiError):
    """Raised for 429 responses."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str = "rate_limited",
        request_id: str | None = None,
        details: object | None = None,
        retry_after_ms: int | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(
            message,
            status=status,
            code=code,
            request_id=request_id,
            details=details,
        )
        self.retry_after_ms = retry_after_ms


class InsufficientCreditsError(ApiError):
    """Raised for 402 insufficient credits responses."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str = "insufficient_credits",
        request_id: str | None = None,
        details: object | None = None,
        required: int = 0,
        available: int = 0,
    ) -> None:
        """Initialize the error."""
        super().__init__(
            message,
            status=status,
            code=code,
            request_id=request_id,
            details=details,
        )
        self.required = required
        self.available = available


class JobFailedError(ConduitError):
    """Raised when a job reaches a failed terminal state."""

    def __init__(
        self,
        job_id: str,
        message: str,
        *,
        code: str = "job_failed",
        request_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(message, code=code, request_id=request_id, cause=cause)
        self.job_id = job_id


class JobCanceledError(ConduitError):
    """Raised when a job reaches a canceled terminal state."""

    def __init__(
        self,
        job_id: str,
        message: str,
        *,
        code: str = "job_canceled",
        request_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(message, code=code, request_id=request_id, cause=cause)
        self.job_id = job_id


class TimeoutError(ConduitError):
    """Raised when the SDK deadline expires."""


class RequestAbortedError(ConduitError):
    """Raised when the caller aborts a request."""


@dataclass(slots=True)
class StreamContext:
    """Debug metadata for stream failures."""

    job_id: str | None = None
    last_event_id: str | None = None
    retry_count: int = 0


class StreamError(ConduitError):
    """Raised when job event streaming fails."""

    def __init__(
        self,
        message: str,
        *,
        job_id: str | None = None,
        last_event_id: str | None = None,
        retry_count: int = 0,
        request_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        """Initialize the error."""
        super().__init__(
            message, code="stream_error", request_id=request_id, cause=cause
        )
        self.job_id = job_id
        self.last_event_id = last_event_id
        self.retry_count = retry_count


__all__ = [
    "ApiError",
    "AuthError",
    "ConduitError",
    "InitializationError",
    "InsufficientCreditsError",
    "InvalidSourceError",
    "JobCanceledError",
    "JobFailedError",
    "RateLimitError",
    "RemoteFetchError",
    "RemoteFetchTimeoutError",
    "RemoteFetchTooLargeError",
    "RequestAbortedError",
    "SourceError",
    "StreamContext",
    "StreamError",
    "TimeoutError",
    "UnsupportedRuntimeError",
    "ValidationError",
    "WebhookVerificationError",
]

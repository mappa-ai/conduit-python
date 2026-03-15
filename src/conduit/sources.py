"""Source validation, materialization, and upload helpers."""

from __future__ import annotations

import mimetypes
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, cast
from urllib.parse import urlparse

import httpx

from .errors import (
    ConduitError,
    InvalidSourceError,
    RemoteFetchError,
    RemoteFetchTimeoutError,
    RemoteFetchTooLargeError,
)
from .models import MediaObject, parse_media_object

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._transport import Transport
    from .types import BinaryLike, CancelSignal, ReportSource

DEFAULT_MAX_SOURCE_BYTES = 5 * 1024 * 1024 * 1024


@dataclass(slots=True)
class UploadMaterialization:
    """Prepared upload payload and metadata."""

    content_type: str | None
    filename: str
    file_value: object
    label: str
    closer: Callable[[], None] | None = None

    def close(self) -> None:
        """Release any temporary resources created during materialization."""
        if self.closer is None:
            return
        self.closer()


class LimitedBinaryReader:
    """Binary reader wrapper that enforces a maximum byte budget."""

    def __init__(self, wrapped: BinaryIO, *, limit_bytes: int) -> None:
        self._limit_bytes = limit_bytes
        self._read_bytes = 0
        self._wrapped = wrapped

    def read(self, size: int = -1) -> bytes:
        chunk = self._wrapped.read(size)
        self._read_bytes += len(chunk)
        if self._read_bytes > self._limit_bytes:
            raise InvalidSourceError(
                "source.file exceeds upload size limit",
                code="source_too_large",
            )
        return chunk

    def __getattr__(self, name: str) -> object:
        return getattr(self._wrapped, name)


class SourceManager:
    """Resolve report sources and perform media uploads."""

    def __init__(
        self,
        transport: Transport,
        *,
        timeout_ms: int,
        max_source_bytes: int,
    ) -> None:
        """Initialize the source manager."""
        self._max_source_bytes = max_source_bytes
        self._timeout_ms = timeout_ms
        self._transport = transport

    def upload(
        self,
        *,
        file: BinaryLike | None = None,
        url: str | None = None,
        path: str | None = None,
        label: str | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        signal: CancelSignal | None = None,
    ) -> MediaObject:
        """Upload media from a supported source."""
        _check_signal(signal, self._transport, request_id=request_id)
        materialized = self.materialize_upload_source(
            file=file,
            url=url,
            path=path,
            label=label,
        )
        try:
            _check_signal(signal, self._transport, request_id=request_id)
            response = self._transport.request(
                "POST",
                "/v1/files",
                data={"label": materialized.label},
                files={
                    "file": (
                        materialized.filename,
                        materialized.file_value,
                        materialized.content_type,
                    )
                },
                idempotency_key=idempotency_key,
                request_id=request_id,
                retryable=True,
            )
        finally:
            materialized.close()
        return parse_media_object(response.data)

    def resolve_report_source(
        self,
        source: ReportSource,
        *,
        idempotency_key: str | None,
        request_id: str | None,
        signal: CancelSignal | None,
    ) -> str:
        """Resolve a report source into a media ID."""
        source_map = _normalize_source_mapping(source)
        keys = [key for key in ("media_id", "file", "url", "path") if key in source_map]
        if len(keys) != 1:
            raise InvalidSourceError(
                "source must include exactly one of media_id, file, url, or path",
                code="invalid_source",
            )
        if "media_id" in source_map:
            return _string_value(source_map.get("media_id"), "source.media_id")
        return self.upload(
            file=_binary_like_value(source_map.get("file")),
            url=_optional_str(source_map.get("url")),
            path=_optional_str(source_map.get("path")),
            label=_optional_str(source_map.get("label")),
            idempotency_key=idempotency_key,
            request_id=request_id,
            signal=signal,
        ).media_id

    def materialize_upload_source(
        self,
        *,
        file: BinaryLike | None,
        url: str | None,
        path: str | None,
        label: str | None,
    ) -> UploadMaterialization:
        """Prepare a supported upload source."""
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
    ) -> UploadMaterialization:
        path = Path(_string_value(raw_path, "source.path"))
        if not path.exists():
            raise InvalidSourceError(f"File not found: {path}", code="invalid_source")
        if path.is_dir():
            raise InvalidSourceError(
                f"Path is a directory: {path}",
                code="invalid_source",
            )
        size = path.stat().st_size
        if size > self._max_source_bytes:
            raise InvalidSourceError(
                "source.path exceeds upload size limit",
                code="source_too_large",
            )
        handle = path.open("rb")
        filename = path.name
        return UploadMaterialization(
            content_type=mimetypes.guess_type(filename)[0],
            filename=filename,
            file_value=handle,
            label=_resolve_label(label, filename, fallback="upload"),
            closer=handle.close,
        )

    def _materialize_file(
        self,
        raw_file: BinaryLike,
        label: str | None,
    ) -> UploadMaterialization:
        if isinstance(raw_file, bytes):
            filename = "upload.bin"
            return UploadMaterialization(
                content_type=mimetypes.guess_type(filename)[0],
                filename=filename,
                file_value=self._bounded_bytes(raw_file, "source.file"),
                label=_resolve_label(label, filename, fallback="upload"),
            )
        if isinstance(raw_file, bytearray):
            return self._materialize_file(bytes(raw_file), label)
        if isinstance(raw_file, memoryview):
            return self._materialize_file(raw_file.tobytes(), label)

        filename = Path(getattr(raw_file, "name", "upload.bin")).name or "upload.bin"
        size = _stream_size(raw_file)
        if size is not None and size > self._max_source_bytes:
            raise InvalidSourceError(
                "source.file exceeds upload size limit",
                code="source_too_large",
            )
        wrapped = LimitedBinaryReader(raw_file, limit_bytes=self._max_source_bytes)
        return UploadMaterialization(
            content_type=mimetypes.guess_type(filename)[0],
            filename=filename,
            file_value=wrapped,
            label=_resolve_label(label, filename, fallback="upload"),
        )

    def _materialize_url(
        self, raw_url: str, label: str | None
    ) -> UploadMaterialization:
        url = _validate_http_url(raw_url, "source.url")
        temporary_file = _temporary_file()
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
                    _raise_remote_fetch_failed(url, response.status_code)
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > self._max_source_bytes:
                    _raise_remote_fetch_too_large(url, response.status_code)
                total_bytes = 0
                for chunk in response.iter_bytes():
                    total_bytes += len(chunk)
                    if total_bytes > self._max_source_bytes:
                        _raise_remote_fetch_too_large(url, response.status_code)
                    temporary_file.write(chunk)
                temporary_file.seek(0)
                final_filename = _filename_from_url(str(response.url))
                content_type = _strip_content_type(response.headers.get("content-type"))
        except httpx.TimeoutException as exc:
            temporary_file.close()
            raise RemoteFetchTimeoutError(
                "Remote fetch timed out",
                code="remote_fetch_timeout",
                url=url,
                cause=exc,
            ) from exc
        except httpx.TooManyRedirects as exc:
            temporary_file.close()
            raise RemoteFetchError(
                "Remote fetch exceeded redirect limit",
                code="remote_fetch_redirects_exhausted",
                url=url,
                cause=exc,
            ) from exc
        except httpx.HTTPError as exc:
            temporary_file.close()
            raise RemoteFetchError(
                "Remote fetch failed",
                code="remote_fetch_failed",
                url=url,
                cause=exc,
            ) from exc
        except Exception:
            temporary_file.close()
            raise

        return UploadMaterialization(
            content_type=content_type,
            filename=final_filename,
            file_value=temporary_file,
            label=_resolve_label(label, final_filename, fallback="remote"),
            closer=temporary_file.close,
        )

    def _bounded_bytes(self, payload: bytes, name: str) -> bytes:
        if len(payload) > self._max_source_bytes:
            raise InvalidSourceError(
                f"{name} exceeds upload size limit",
                code="source_too_large",
            )
        return payload


def _binary_like_value(value: object | None) -> BinaryLike | None:
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        return cast("BinaryLike", value)
    if hasattr(value, "read"):
        return cast("BinaryIO", value)
    raise ConduitError("source.file must be binary", code="invalid_request")


def _normalize_source_mapping(source: ReportSource) -> dict[str, object]:
    normalized = {str(key): value for key, value in source.items()}
    for legacy_key, new_key in {
        "mediaId": "media_id",
        "templateParams": "template_params",
        "timeRange": "time_range",
    }.items():
        if legacy_key in normalized and new_key not in normalized:
            normalized[new_key] = normalized[legacy_key]
    return normalized


def _optional_str(value: object | None) -> str | None:
    return value if isinstance(value, str) else None


def _string_value(value: object, name: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ConduitError(f"{name} must be a non-empty string", code="invalid_request")


def _validate_http_url(value: str, name: str) -> str:
    normalized = _string_value(value, name)
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise InvalidSourceError(
            f"{name} must be an http or https URL",
            code="invalid_source",
        )
    return normalized


def _resolve_label(label: str | None, filename: str, *, fallback: str) -> str:
    if label is not None:
        return _normalize_label(label)
    normalized = _normalize_label(filename)
    if normalized:
        return normalized
    return fallback


def _normalize_label(value: str) -> str:
    cleaned = value.strip()
    stem = Path(cleaned).stem.strip()
    if stem:
        return stem
    raise InvalidSourceError("label is required", code="invalid_source")


def _filename_from_url(value: str) -> str:
    parsed = urlparse(value)
    name = Path(parsed.path).name
    return name or "remote.bin"


def _strip_content_type(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.split(";", maxsplit=1)[0].strip()
    return stripped or None


def _stream_size(stream: BinaryIO) -> int | None:
    if not stream.seekable():
        return None
    current_position = stream.tell()
    stream.seek(0, 2)
    end_position = stream.tell()
    stream.seek(current_position)
    return end_position - current_position


def _check_signal(
    signal: CancelSignal | None,
    transport: Transport,
    *,
    request_id: str | None,
) -> None:
    if signal is not None and signal.is_set():
        raise transport.aborted(request_id=request_id)


def _temporary_file() -> BinaryIO:
    return tempfile.TemporaryFile()


def _raise_remote_fetch_failed(url: str, status: int) -> None:
    raise RemoteFetchError(
        f"Remote fetch failed with status {status}",
        code="remote_fetch_failed",
        status=status,
        url=url,
    )


def _raise_remote_fetch_too_large(url: str, status: int) -> None:
    raise RemoteFetchTooLargeError(
        "source.url exceeds upload size limit",
        code="source_too_large",
        status=status,
        url=url,
    )


__all__ = ["DEFAULT_MAX_SOURCE_BYTES", "SourceManager", "UploadMaterialization"]

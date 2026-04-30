"""Psychometrics tests for the Conduit Python SDK."""

from __future__ import annotations

import httpx
import pytest

from conduit import Conduit, ConduitError, InvalidSourceError

EXAMPLE_PSYCHOMETRICS = {
    "analysisId": "analysis_123",
    "confidence": {"overall": 0.81, "source": "signal_heuristic"},
    "createdAt": "2026-04-20T12:00:00.000Z",
    "expiresAt": "2026-04-27T12:00:00.000Z",
    "model": {
        "metadata": {"checkpoint": "checkpoint_v2"},
        "version": "checkpoint_v2",
    },
    "psychometrics": {"agreeableness": 0.42, "conscientiousness": 0.77},
    "quality": {
        "segmentCount": 4,
        "signal": "high",
        "sourceAudioDurationSeconds": 91,
        "speakerCoverageRatio": 0.64,
        "targetAudioDurationSeconds": 58,
        "targetUtteranceCount": 12,
    },
    "selectedSpeaker": {"speakerIndex": 1, "strategy": "magic_hint"},
}


def test_psychometrics_create_posts_multipart_and_parses_result() -> None:
    """Create a psychometrics analysis from a file-like source."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or request.url.path != "/v2/psychometrics":
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        if request.headers.get("Mappa-Api-Key") != "sk_test":
            raise AssertionError("Expected public API key auth")
        if request.headers.get("Idempotency-Key") != "idem_123":
            raise AssertionError("Expected idempotency key on create request")
        body = request.content.decode("utf-8", errors="ignore")
        if 'name="strategy"' not in body or "magic_hint" not in body:
            raise AssertionError("Expected multipart strategy field")
        if 'name="hint"' not in body or "the candidate" not in body:
            raise AssertionError("Expected multipart hint field")
        if 'name="file"; filename="upload.bin"' not in body:
            raise AssertionError("Expected multipart file field")
        return httpx.Response(
            200,
            headers={"x-request-id": "req_psy_123"},
            json=EXAMPLE_PSYCHOMETRICS,
            request=request,
        )

    conduit = Conduit(
        api_key="sk_test",
        base_url="http://testserver",
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://testserver"
        ),
    )

    try:
        result = conduit.psychometrics.create(
            source={"file": b"audio-bytes"},
            target={"strategy": "magic_hint", "hint": "the candidate"},
            idempotency_key="idem_123",
        )
    finally:
        conduit.close()

    if result.analysis_id != "analysis_123":
        raise AssertionError("Expected analysis_id to parse correctly")
    if result.selected_speaker.strategy != "magic_hint":
        raise AssertionError("Expected selected speaker strategy to parse correctly")


def test_psychometrics_get_fetches_analysis() -> None:
    """Fetch a completed psychometrics analysis by ID."""

    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.method != "GET"
            or request.url.path != "/v2/psychometrics/analysis_123"
        ):
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        return httpx.Response(
            200,
            headers={"x-request-id": "req_psy_456"},
            json=EXAMPLE_PSYCHOMETRICS,
            request=request,
        )

    conduit = Conduit(
        api_key="sk_test",
        base_url="http://testserver",
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://testserver"
        ),
    )

    try:
        result = conduit.psychometrics.get("analysis_123")
    finally:
        conduit.close()

    if result.model.version != "checkpoint_v2":
        raise AssertionError("Expected model.version to parse correctly")
    if result.psychometrics["conscientiousness"] != 0.77:
        raise AssertionError("Expected psychometrics values to parse correctly")


def test_psychometrics_create_rejects_invalid_target() -> None:
    """Reject empty magic-hint targets before issuing a request."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(ConduitError):
            conduit.psychometrics.create(
                source={"file": b"audio-bytes"},
                target={"strategy": "magic_hint", "hint": "   "},
            )
    finally:
        conduit.close()


def test_psychometrics_create_rejects_mixed_source_variants() -> None:
    """Reject psychometrics source payloads that mix variants."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(InvalidSourceError):
            conduit.psychometrics.create(
                source={"file": b"audio-bytes", "url": "https://example.com/call.wav"},  # type: ignore[arg-type]
                target={"strategy": "dominant"},
            )
    finally:
        conduit.close()

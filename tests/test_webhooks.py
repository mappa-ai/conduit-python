"""Webhook tests for the Conduit Python SDK."""

from __future__ import annotations

import hashlib
import hmac
import time

import pytest

from conduit import Conduit, ConduitError, WebhookVerificationError

REPORT_COMPLETED_PAYLOAD = (
    "{"
    '"id":"evt_1",'
    '"type":"report.completed",'
    '"createdAt":"2026-03-15T00:00:00Z",'
    '"timestamp":"2026-03-15T00:00:00Z",'
    '"data":{"jobId":"job_1","reportId":"rep_1","status":"succeeded"}'
    "}"
)

UNKNOWN_EVENT_PAYLOAD = (
    "{"
    '"id":"evt_2",'
    '"type":"future.event",'
    '"createdAt":"2026-03-15T00:00:00Z",'
    '"timestamp":"2026-03-15T00:00:00Z",'
    '"data":{"jobId":"job_1"}'
    "}"
)


def test_verify_signature_accepts_valid_payload() -> None:
    """Accept a valid webhook signature."""
    secret = "whsec_test"
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode(),
        f"{timestamp}.{REPORT_COMPLETED_PAYLOAD}".encode(),
        hashlib.sha256,
    ).hexdigest()
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        verified = conduit.webhooks.verify_signature(
            REPORT_COMPLETED_PAYLOAD.encode(),
            {"conduit-signature": f"t={timestamp},v1={signature}"},
            secret,
        )
    finally:
        conduit.close()

    if verified is not True:
        raise AssertionError("Expected webhook verification to return True")


def test_verify_signature_rejects_duplicate_header_values() -> None:
    """Reject duplicate signature header values."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(WebhookVerificationError):
            conduit.webhooks.verify_signature(
                REPORT_COMPLETED_PAYLOAD,
                {"conduit-signature": ["t=1,v1=deadbeef", "t=2,v1=beefdead"]},
                "whsec_test",
            )
    finally:
        conduit.close()


def test_parse_event_validates_known_event_shape() -> None:
    """Parse a valid known event envelope."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        event = conduit.webhooks.parse_event(REPORT_COMPLETED_PAYLOAD)
    finally:
        conduit.close()

    if event.type != "report.completed":
        raise AssertionError("Expected report.completed event type")


def test_parse_event_passes_through_unknown_events() -> None:
    """Return unknown future event types without rejecting them."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        event = conduit.webhooks.parse_event(UNKNOWN_EVENT_PAYLOAD)
    finally:
        conduit.close()

    if event.type != "future.event":
        raise AssertionError("Expected future.event type")


def test_parse_event_rejects_invalid_known_event_shape() -> None:
    """Reject an invalid known event envelope."""
    invalid_payload = (
        "{"
        '"id":"evt_1",'
        '"type":"report.completed",'
        '"createdAt":"2026-03-15T00:00:00Z",'
        '"timestamp":"2026-03-15T00:00:00Z",'
        '"data":{"jobId":"job_1","status":"succeeded"}'
        "}"
    )
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(ConduitError):
            conduit.webhooks.parse_event(invalid_payload)
    finally:
        conduit.close()

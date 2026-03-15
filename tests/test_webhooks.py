"""Webhook unit tests for the Conduit Python SDK."""

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

MATCHING_COMPLETED_PAYLOAD = (
    "{"
    '"id":"evt_1",'
    '"type":"matching.completed",'
    '"createdAt":"2026-03-15T00:00:00Z",'
    '"timestamp":"2026-03-15T00:00:00Z",'
    '"data":{"jobId":"job_1","matchingId":"mat_1","status":"succeeded"}'
    "}"
)

INVALID_REPORT_COMPLETED_PAYLOAD = (
    "{"
    '"id":"evt_1",'
    '"type":"report.completed",'
    '"createdAt":"2026-03-15T00:00:00Z",'
    '"timestamp":"2026-03-15T00:00:00Z",'
    '"data":{"jobId":"job_1","status":"succeeded"}'
    "}"
)


def test_verify_signature_accepts_valid_payload() -> None:
    """Accept a valid webhook signature."""
    payload = REPORT_COMPLETED_PAYLOAD
    secret = "whsec_test"
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{payload}".encode(),
        hashlib.sha256,
    ).hexdigest()
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        verified = conduit.webhooks.verify_signature(
            payload,
            {"conduit-signature": f"t={timestamp},v1={signature}"},
            secret,
        )
    finally:
        conduit.close()

    if verified is not True:
        raise AssertionError("Expected webhook verification to return True")


def test_verify_signature_rejects_invalid_payload() -> None:
    """Reject an invalid webhook signature."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(WebhookVerificationError):
            conduit.webhooks.verify_signature(
                "{}",
                {"conduit-signature": "t=1,v1=deadbeef"},
                "whsec_test",
                tolerance_sec=999999999,
            )
    finally:
        conduit.close()


def test_parse_event_validates_known_event_shape() -> None:
    """Parse a valid known event envelope."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        event = conduit.webhooks.parse_event(MATCHING_COMPLETED_PAYLOAD)
    finally:
        conduit.close()

    if event.type != "matching.completed":
        raise AssertionError("Expected matching.completed event type")


def test_parse_event_rejects_invalid_known_event_shape() -> None:
    """Reject an invalid known event envelope."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(ConduitError):
            conduit.webhooks.parse_event(INVALID_REPORT_COMPLETED_PAYLOAD)
    finally:
        conduit.close()

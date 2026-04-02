"""Contract-shape tests for the Conduit Python SDK."""

from __future__ import annotations

import json
from typing import Any

import httpx

from conduit import Conduit


def test_reports_get_normalizes_legacy_backend_shape() -> None:
    """Normalize legacy backend report payloads into spec-shaped models."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET" or request.url.path != "/v1/reports/rep_123":
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        return httpx.Response(
            200,
            headers={"x-request-id": "req_123"},
            json={
                "createdAt": "2026-03-15T00:00:00Z",
                "entity": {"id": "ent_123", "label": "Dana"},
                "id": "rep_123",
                "jobId": "job_123",
                "json": {"sections": []},
                "markdown": "# Report",
                "media": {"mediaId": "med_123", "url": "https://example.com/report"},
                "output": {"template": "general_report"},
            },
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
        report = conduit.reports.get("rep_123")
    finally:
        conduit.close()

    if report.output.markdown != "# Report":
        raise AssertionError("Expected markdown to normalize from top-level payload")
    if report.output.report_url != "https://example.com/report":
        raise AssertionError("Expected report_url to normalize from legacy media.url")
    if report.entity_id != "ent_123" or report.media_id != "med_123":
        raise AssertionError("Expected entity_id and media_id to normalize correctly")


def test_matching_get_normalizes_legacy_backend_shape() -> None:
    """Normalize legacy backend matching payloads into spec-shaped models."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET" or request.url.path != "/v1/matching/mat_123":
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        return httpx.Response(
            200,
            headers={"x-request-id": "req_123"},
            json={
                "context": "behavioral_compatibility",
                "createdAt": "2026-03-15T00:00:00Z",
                "group": [
                    {
                        "entityId": "ent_group",
                        "resolvedLabel": "Manager",
                        "source": {"entityId": "ent_group"},
                    }
                ],
                "id": "mat_123",
                "jobId": "job_123",
                "json": {"sections": []},
                "markdown": "# Matching",
                "output": {"template": "matching"},
                "target": {
                    "entityId": "ent_target",
                    "resolvedLabel": "Candidate",
                    "source": {"entityId": "ent_target"},
                },
            },
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
        matching = conduit.matching.get("mat_123")
    finally:
        conduit.close()

    if matching.output.markdown != "# Matching":
        raise AssertionError("Expected markdown to normalize from top-level payload")
    if matching.context != "behavioral_compatibility":
        raise AssertionError("Expected context to normalize correctly")
    if matching.target is None or matching.target.entity_id != "ent_target":
        raise AssertionError("Expected target to normalize correctly")


def test_matching_create_accepts_canonical_subjects() -> None:
    """Accept canonical matching refs and adapt them to the current wire shape."""
    seen_body: dict[str, Any] | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_body
        if request.method != "POST" or request.url.path != "/v1/matching/jobs":
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        seen_body = json.loads(request.content)
        return httpx.Response(
            200,
            headers={"x-request-id": "req_123"},
            json={"estimatedWaitSec": 90, "jobId": "job_123", "status": "queued"},
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
        receipt = conduit.matching.create(
            context="behavioral_compatibility",
            target={"entity_id": "ent_target"},
            group=[
                {
                    "media_id": "med_group",
                    "selector": {"strategy": "dominant"},
                }
            ],
        )
    finally:
        conduit.close()

    if receipt.job_id != "job_123":
        raise AssertionError("Expected receipt.job_id to parse correctly")
    if seen_body is None:
        raise AssertionError("Expected request body to be captured")
    if seen_body["target"] != {"entityId": "ent_target", "type": "entity_id"}:
        raise AssertionError("Expected canonical target ref to adapt to backend wire")
    expected_group = [
        {
            "mediaId": "med_group",
            "selector": {"strategy": "dominant"},
            "type": "media_target",
        }
    ]
    if seen_body["group"] != expected_group:
        raise AssertionError("Expected canonical group ref to adapt to backend wire")

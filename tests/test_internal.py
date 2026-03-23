"""Internal client tests for the Conduit Python SDK."""

from __future__ import annotations

import httpx
import pytest

from conduit.errors import ConduitError, InitializationError
from conduit.internal import InternalConduit


def test_internal_conduit_requires_internal_api_key() -> None:
    """Reject empty internal API keys at construction time."""
    with pytest.raises(InitializationError):
        InternalConduit(internal_api_key="")


def test_internal_behavior_maps_get_uses_internal_auth_header() -> None:
    """Send x-internal-key and parse the behavior-map export payload."""

    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.method != "GET"
            or request.url.path != "/internal/behavior-maps/ent_123"
        ):
            raise AssertionError(
                f"Unexpected request: {request.method} {request.url.path}"
            )
        if request.headers.get("x-internal-key") != "internal_test_key_12345":
            raise AssertionError("Expected x-internal-key authentication")
        if request.headers.get("Mappa-Api-Key") is not None:
            raise AssertionError("Did not expect the public API key header")
        return httpx.Response(
            200,
            headers={"x-request-id": "req_internal"},
            json={
                "behaviorMap": {
                    "trait_baseline": {
                        "hexaco": {
                            "agreeableness": {
                                "facet": {"binned": "medium", "normalized": 0.5}
                            }
                        }
                    }
                },
                "createdAt": "2026-03-21T12:00:00.000Z",
                "entityId": "ent_123",
                "mapId": "map_123",
                "modelCheckpoint": "checkpoint_v1",
            },
            request=request,
        )

    conduit = InternalConduit(
        internal_api_key="internal_test_key_12345",
        base_url="http://testserver",
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://testserver"
        ),
    )

    try:
        behavior_map = conduit.behavior_maps.get("ent_123")
    finally:
        conduit.close()

    if behavior_map.entity_id != "ent_123":
        raise AssertionError("Expected entity_id to parse correctly")
    if behavior_map.map_id != "map_123":
        raise AssertionError("Expected map_id to parse correctly")
    if (
        behavior_map.behavior_map["trait_baseline"]["hexaco"]["agreeableness"]["facet"][
            "normalized"
        ]
        != 0.5
    ):
        raise AssertionError("Expected behavior_map to preserve nested prompt input")


def test_internal_behavior_maps_validates_entity_id() -> None:
    """Reject empty entity IDs before issuing a request."""
    conduit = InternalConduit(internal_api_key="internal_test_key_12345")

    try:
        with pytest.raises(ConduitError):
            conduit.behavior_maps.get("")
    finally:
        conduit.close()

"""Validation unit tests for the Conduit Python SDK."""

from __future__ import annotations

import pytest

from conduit import Conduit, ConduitError, InvalidSourceError


def test_reports_create_rejects_mixed_source_variants() -> None:
    """Reject source payloads that mix variants."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(InvalidSourceError):
            conduit.reports.create(
                source={"mediaId": "med_1", "path": "audio.mp3"},
                output={"template": "general_report"},
                target={"strategy": "dominant"},
            )
    finally:
        conduit.close()


def test_matching_create_rejects_invalid_context() -> None:
    """Reject non-canonical matching contexts."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(ConduitError):
            conduit.matching.create(
                context="freeform",
                target={"entityId": "ent_target"},
                group=[{"entityId": "ent_group"}],
            )
    finally:
        conduit.close()

"""Validation tests for the Conduit Python SDK."""

from __future__ import annotations

from typing import cast

import pytest

from conduit import Conduit, ConduitError, InvalidSourceError, ReportSource


def test_reports_create_rejects_mixed_source_variants() -> None:
    """Reject source payloads that mix variants."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(InvalidSourceError):
            conduit.reports.create(
                source=cast(
                    "ReportSource",
                    {"media_id": "med_1", "path": "audio.mp3"},
                ),
                output={"template": "general_report"},
                target={"strategy": "dominant"},
            )
    finally:
        conduit.close()


def test_reports_create_rejects_invalid_timerange() -> None:
    """Reject timerange selectors with invalid bounds."""
    conduit = Conduit(api_key="sk_test", base_url="http://localhost:8080")

    try:
        with pytest.raises(ConduitError):
            conduit.reports.create(
                source=cast("ReportSource", {"media_id": "med_1"}),
                output={"template": "general_report"},
                target={
                    "strategy": "timerange",
                    "time_range": {"start_seconds": 20, "end_seconds": 10},
                },
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
                target={"entity_id": "ent_target"},
                group=[{"entity_id": "ent_group"}],
            )
    finally:
        conduit.close()

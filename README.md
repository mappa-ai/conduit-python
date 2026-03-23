# Conduit Python SDK

This repository publishes the official Python SDK for Conduit and mirrors the
package source from the main Conduit monorepo. The SDK is maintained as part of
the main project; this repository exists to provide a focused public package
home for Python consumers.

Conduit is a behavioral intelligence API with two stable workflows:

- `reports` turns a recording into a structured behavioral report for a selected speaker.
- `matching` compares one target against a group in a closed interpretation context.

The Python package name is `mappa-conduit`. The import surface is `conduit`.

## Install

```bash
pip install mappa-conduit
```

## Python version

`mappa-conduit` requires Python 3.12 or newer.

## API surface

The stable public surface is:

- `conduit.reports.create(...)`
- `conduit.reports.get(...)`
- `conduit.matching.create(...)`
- `conduit.matching.get(...)`
- `conduit.webhooks.verify_signature(...)`
- `conduit.webhooks.parse_event(...)`
- `conduit.primitives.entities.*`
- `conduit.primitives.media.*`
- `conduit.primitives.jobs.*`

Use `reports` first. `matching` is also stable. `primitives` is the advanced,
lower-level surface for upload, reuse, inspection, and backoffice flows.

## Choose a workflow

- Use `reports.create(...)` when you want to analyze one speaker from a recording.
- Use `matching.create(...)` when you want to compare one target against a group.
- Use webhooks as the primary completion path in production.
- Use `receipt.handle.wait()` for local scripts and development.
- Use `receipt.handle.stream()` when you want live job stage updates.
- Use `conduit.primitives.media.upload(...)` when you want to upload once and reuse a `media_id` later.

## Quickstart

Report creation is receipt-first and asynchronous. `reports.create(...)` never
waits for report completion. It resolves the source, uploads if needed, creates
the job, and returns a receipt immediately.

```python
from conduit import Conduit

conduit = Conduit(api_key="sk_...")

receipt = conduit.reports.create(
    source={"url": "https://storage.example.com/call.wav"},
    output={"template": "general_report"},
    target={"strategy": "dominant"},
    webhook={"url": "https://my-app.com/webhooks/conduit"},
    idempotency_key="signup-call-42",
)

print(receipt.job_id)
print(receipt.status)
```

### What the receipt contains

- `job_id`: accepted job identifier
- `status`: usually `queued` or `running`
- `stage`: current stage when available
- `estimated_wait_sec`: advisory wait estimate when available
- `media_id`: included for report creation when the SDK knows it
- `handle`: convenience helpers for waiting, streaming, fetching job state, and cancellation

## Verified webhook handling

Webhooks are the primary production completion path. Always verify the raw body
before parsing the event. Delivery is at-least-once, so consumers should dedupe
on `event.id`.

```python
from pathlib import Path

from conduit import Conduit


conduit = Conduit(api_key="sk_...")


def has_seen(event_id: str) -> bool:
    return Path(f"/tmp/{event_id}").exists()


def mark_seen(event_id: str) -> None:
    Path(f"/tmp/{event_id}").touch()


def handle_webhook(raw_payload: bytes, headers: dict[str, str]) -> None:
    conduit.webhooks.verify_signature(
        raw_payload,
        headers,
        secret="whsec_...",
    )
    event = conduit.webhooks.parse_event(raw_payload)

    if has_seen(event.id):
        return

    if event.type == "report.completed":
        report_id = event.data["reportId"]
        report = conduit.reports.get(report_id)
        print(report.output.markdown)

    if event.type == "report.failed":
        print(event.data["error"]["message"])

    if event.type == "matching.completed":
        matching_id = event.data["matchingId"]
        matching = conduit.matching.get(matching_id)
        print(matching.output.markdown)

    if event.type == "matching.failed":
        print(event.data["error"]["message"])

    mark_seen(event.id)
```

### Webhook behavior

- Verify before parsing.
- `verify_signature(...)` expects the exact raw request body.
- Header lookup is case-insensitive and uses `conduit-signature`.
- Known event types are validated for shape.
- Unknown future event types are returned without rejection so consumers can forward-handle them safely.

## Common report workflows

### Create a report from a local file path

```python
receipt = conduit.reports.create(
    source={"path": "recordings/demo-call.wav"},
    output={"template": "general_report"},
    target={"strategy": "dominant"},
)
```

### Upload media once, then reuse `media_id`

```python
media = conduit.primitives.media.upload(path="recordings/demo-call.wav")

receipt = conduit.reports.create(
    source={"media_id": media.media_id},
    output={"template": "sales_playbook"},
    target={"strategy": "entity_id", "entity_id": "ent_123"},
)
```

### Wait locally for the completed report

`handle.wait()` is a convenience for scripts and local development. It is not
the recommended production path.

```python
report = receipt.handle.wait(timeout_ms=300_000)
print(report.output.template)
print(report.output.markdown)
```

### Stream job events

```python
for event in receipt.handle.stream(timeout_ms=300_000):
    print(event.type, event.job.status, event.stage, event.progress)
```

### Fetch current job state or request cancellation

```python
job = receipt.handle.job()
print(job.status)

job = receipt.handle.cancel()
print(job.status)
```

### Fetch a report only if it already exists

```python
maybe_report = receipt.handle.report()
if maybe_report is not None:
    print(maybe_report.id)
```

## Matching

Matching is also receipt-first and asynchronous.

```python
matching_receipt = conduit.matching.create(
    context="hiring_team_fit",
    target={"entity_id": "ent_candidate"},
    group=[
        {"entity_id": "ent_manager"},
        {
            "media_id": "med_panel",
            "selector": {"strategy": "dominant"},
        },
    ],
    webhook={"url": "https://my-app.com/webhooks/conduit"},
)

print(matching_receipt.job_id)
```

### Matching constraints

- `context` is currently a closed enum and must be `hiring_team_fit`.
- `target` must be exactly one matching subject.
- `group` must contain at least one matching subject.
- A matching subject must be either `{"entity_id": ...}` or `{"media_id": ..., "selector": ...}`.
- Duplicate direct `entity_id` references across target and group are rejected.

### Wait for a matching result

```python
matching = matching_receipt.handle.wait(timeout_ms=300_000)
print(matching.output.markdown)
```

## Primitives

`conduit.primitives` is the stable advanced surface.

### Entities

```python
entity = conduit.primitives.entities.get("ent_123")

page = conduit.primitives.entities.list(limit=20)

updated = conduit.primitives.entities.update("ent_123", {"label": "Top performer"})
```

### Media

```python
media = conduit.primitives.media.upload(path="recordings/demo-call.wav")

media = conduit.primitives.media.get(media.media_id)

page = conduit.primitives.media.list(limit=20)

result = conduit.primitives.media.set_retention_lock(media.media_id, locked=True)

deleted = conduit.primitives.media.delete(media.media_id)
```

### Jobs

```python
job = conduit.primitives.jobs.get(receipt.job_id)

job = conduit.primitives.jobs.cancel(receipt.job_id)
```

## Request shapes

Public request dictionaries use snake_case keys.

### Report source

Exactly one source variant is allowed.

```python
{"media_id": "med_123"}
{"file": binary_file_or_bytes, "label": "call-a"}
{"url": "https://storage.example.com/call.wav", "label": "call-a"}
{"path": "recordings/call.wav", "label": "call-a"}
```

Rules:

- Mixed source variants are rejected before any network call.
- `media_id`, `url`, and `path` must be non-empty strings.
- `source.url` must use `http` or `https`.
- `source.path` must point to a file, not a directory.

### Report output

```python
{"template": "general_report"}
{"template": "sales_playbook"}
{"template": "general_report", "template_params": {...}}
```

Supported stable templates:

- `general_report`
- `sales_playbook`

### Target selector

```python
{"strategy": "dominant"}
{"strategy": "dominant", "on_miss": "fallback_dominant"}

{
    "strategy": "timerange",
    "time_range": {"start_seconds": 15, "end_seconds": 45},
}

{"strategy": "entity_id", "entity_id": "ent_123"}
{"strategy": "magic_hint", "hint": "the interviewer"}
```

Rules:

- `strategy` must be one of `dominant`, `timerange`, `entity_id`, or `magic_hint`.
- `on_miss` may be `error` or `fallback_dominant`.
- `timerange` requires at least one bound.
- If both `start_seconds` and `end_seconds` are present, `start_seconds < end_seconds`.

### Matching subject

```python
{"entity_id": "ent_123"}

{
    "media_id": "med_123",
    "selector": {"strategy": "dominant"},
}
```

### Webhook config

```python
{
    "url": "https://my-app.com/webhooks/conduit",
    "headers": {"x-tenant-id": "tenant_123"},
}
```

## Models you will use most often

- `ReportJobReceipt` and `MatchingJobReceipt` from create calls
- `ReportRunHandle` and `MatchingRunHandle` for `wait()`, `stream()`, `cancel()`, `job()`, and resource fetch helpers
- `Job` for lifecycle state
- `Report` and `Matching` for completed resources
- `WebhookEvent` for verified event envelopes

## Error handling

All SDK failures use typed exceptions.

### API and auth errors

- `ApiError`: base HTTP API error
- `AuthError`: authentication or authorization failure
- `ValidationError`: invalid API payload
- `RateLimitError`: rate-limited request; includes `retry_after_ms` when available
- `InsufficientCreditsError`: insufficient credits; includes `required` and `available`

### Source and upload errors

- `InvalidSourceError`: invalid source shape or unsupported local source
- `RemoteFetchError`: remote URL fetch failure
- `RemoteFetchTimeoutError`: remote URL fetch timeout
- `RemoteFetchTooLargeError`: remote URL exceeded the configured size limit

### Job and streaming errors

- `JobFailedError`: terminal failed job
- `JobCanceledError`: terminal canceled job
- `TimeoutError`: SDK deadline expired
- `RequestAbortedError`: caller-provided cancellation signal fired
- `StreamError`: event stream failed after retries were exhausted

Example:

```python
from conduit import (
    Conduit,
    JobFailedError,
    RemoteFetchError,
    ValidationError,
)


conduit = Conduit(api_key="sk_...")

try:
    receipt = conduit.reports.create(
        source={"url": "https://storage.example.com/call.wav"},
        output={"template": "general_report"},
        target={"strategy": "dominant"},
    )
    report = receipt.handle.wait(timeout_ms=300_000)
except ValidationError as error:
    print(error.code)
except RemoteFetchError as error:
    print(error.url, error.status)
except JobFailedError as error:
    print(error.job_id)
```

## Timeouts, retries, idempotency

Client defaults:

- `timeout_ms=300000`
- `max_retries=2`

Behavior:

- create operations support caller-supplied `idempotency_key`
- transient `429` and `5xx` API errors are retried automatically
- transport timeouts and transport failures are wrapped in typed SDK errors
- `handle.stream()` reconnects automatically and resumes with `Last-Event-ID` when possible
- `handle.wait()` is built on top of stream semantics and raises typed terminal errors

## Cancellation

Sync helpers accept a `signal` object with an `is_set()` method. A
`threading.Event` works well.

```python
from threading import Event

cancel_event = Event()

report = receipt.handle.wait(signal=cancel_event)
```

## Runtime and upload notes

| Runtime | `source.file` | `source.url` | `source.path` | `handle.wait()` | `handle.stream()` | `webhooks.verify_signature()` |
| --- | --- | --- | --- | --- | --- | --- |
| CPython server runtime | Yes | Yes | Yes | Yes | Yes | Yes |

Important notes:

- `source.url` causes the SDK host to fetch the remote origin before upload.
- `source.url` follows redirects up to 5 hops.
- `source.url` fails with typed source errors when the remote request times out, returns an error status, or exceeds the configured size budget.
- `source.path` resolves relative paths from the current working directory.
- local file uploads stream from disk where possible
- remote URL sources are buffered to a temporary file before upload
- webhook verification requires the exact raw request body, not parsed JSON

## Client configuration

```python
from conduit import Conduit

conduit = Conduit(
    api_key="sk_...",
    base_url="https://api.mappa.ai",
    timeout_ms=300_000,
    max_retries=2,
)
```

Supported configuration:

- `api_key`: required API key
- `base_url`: API base URL
- `timeout_ms`: per-request timeout budget in milliseconds
- `max_retries`: retry budget for transient failures
- `max_source_bytes`: source upload limit guard
- `user_agent`: optional custom user agent
- `http_client`: optional custom `httpx.Client`
- `telemetry`: optional request/response/error hooks

If you pass a custom `http_client`, the SDK uses it for requests instead of
creating its own client.

## Typing

The package ships with type information, including typed request dictionaries,
models, receipts, handles, and error classes.

Preferred public keys are snake_case:

- `media_id`
- `entity_id`
- `template_params`
- `time_range`
- `start_seconds`
- `end_seconds`
- `on_miss`

## Internal-only surface

Python also exposes a trusted-backend-only internal module for reading persisted
behavior maps. This is not part of the stable public SDK contract.

```python
from conduit.internal import InternalConduit

internal_conduit = InternalConduit(internal_api_key="internal_...")

exported = internal_conduit.behavior_maps.get("ent_123")

print(exported.map_id)
print(
    exported.behavior_map["trait_baseline"]["hexaco"]["agreeableness"]["facet"][
        "normalized"
    ]
)
```

Rules:

- sends `x-internal-key`, never `Mappa-Api-Key`
- reads the latest persisted behavior map only
- returns `404` when no persisted map exists
- intended for trusted server-side integrations only

## Local checks

```bash
bun run check
bun run type-check
bun run test
```

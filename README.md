# Conduit Python SDK

Official Python SDK for the Conduit API.

## Install

```bash
pip install mappa-conduit
```

## Quickstart

```python
from conduit import Conduit

conduit = Conduit(api_key="sk_...")

receipt = conduit.reports.create(
    source={"path": "./call.mp3"},
    output={"template": "general_report"},
    target={"strategy": "dominant"},
    webhook={"url": "https://your-app.com/webhooks/conduit"},
)

print(receipt.job_id)
```

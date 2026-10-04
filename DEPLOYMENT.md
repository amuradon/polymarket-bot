# Deployment Guide - Polymarket Bot on GCP Cloud Run

This guide outlines the deployment procedures, container configuration, and **mandatory Cloud Run execution flags** for running Polymarket Bot applications (`paper`, `live`) in Google Cloud Platform (GCP).

---

## 1. Mandatory Cloud Run Flags (CRITICAL)

Unlike standard web APIs that only respond to inbound HTTP requests, the Polymarket Bot application is an **active daemon** that:
- Maintains persistent, bidirectional WebSockets to crypto exchanges (Binance, Coinbase, Kraken).
- Computes rolling 60s TWAP indicators and state updates every second on Vert.x event loops.
- Executes real-time statistical arbitrage strategies with sub-second order latency.

Therefore, when deploying or updating the service on GCP Cloud Run, **the following flags MUST ALWAYS be set**:

| Flag | Value | Rationale & Impact |
| :--- | :--- | :--- |
| **`--no-cpu-throttling`** | Enabled | **MANDATORY**: Cloud Run by default allocates CPU *only* during incoming HTTP request handling. Without this flag, Cloud Run throttles the container CPU to 0% whenever no browser/client is hitting the web dashboard. This freezes background WebSockets and Vert.x timers. Upon waking up, large clock jumps cause `BlockedThreadChecker` exceptions and zero trades. `--no-cpu-throttling` guarantees dedicated CPU 24/7. |
| **`--min-instances`** | `1` | **MANDATORY**: Cloud Run by default scales down to 0 instances when idle (`minScale: 0`). A trading bot must never scale to 0; it must continuously monitor markets and manage open positions. |
| **`--memory`** | `1Gi` (or `2Gi`) | Sufficient heap for Java 25 runtime, in-memory price caches, and historical klines. |
| **`--cpu`** | `1` (or `2`) | Dedicated vCPU for GC-free event loop processing. |
| **`--port`** | `8080` | Matches `quarkus.http.port=${PORT:8080}`. |

> [!CAUTION]
> **NEVER deploy without `--no-cpu-throttling` and `--min-instances 1`**.
> Omitting these flags will cause the trading bot to sleep, drop exchange feeds, fail to execute strategy signals, and produce false blocked thread warnings.

---

## 2. Deployment Protocol

### Step 1: Build Docker Container
From the repository root:
```bash
docker build -t europe-west1-docker.pkg.dev/polymarket-bots-508306/cloud-run-source-deploy/polymarket-bot-paper:latest -f paper/src/main/docker/Dockerfile.jvm .
```

### Step 2: Push to GCP Artifact Registry
```bash
docker push europe-west1-docker.pkg.dev/polymarket-bots-508306/cloud-run-source-deploy/polymarket-bot-paper:latest
```

### Step 3: Deploy / Update Cloud Run Service
Deploy the service ensuring all mandatory flags are provided:

```bash
gcloud run deploy polymarket-bot-paper \
  --image europe-west1-docker.pkg.dev/polymarket-bots-508306/cloud-run-source-deploy/polymarket-bot-paper:latest \
  --platform managed \
  --region europe-west1 \
  --project polymarket-bots-508306 \
  --port 8080 \
  --memory 1Gi \
  --cpu 1 \
  --no-cpu-throttling \
  --min-instances 1 \
  --max-instances 2 \
  --allow-unauthenticated
```

To update an existing service without re-specifying the full image:
```bash
gcloud run services update polymarket-bot-paper \
  --no-cpu-throttling \
  --min-instances 1 \
  --project polymarket-bots-508306 \
  --region europe-west1
```

---

## 3. Operational Verification

1. **Verify Continuous CPU Allocation & Scaling:**
   ```bash
   gcloud run services describe polymarket-bot-paper \
     --project polymarket-bots-508306 \
     --region europe-west1 \
     --format="yaml(spec.template.metadata.annotations)"
   ```
   Confirm the output contains:
   ```yaml
   autoscaling.knative.dev/minScale: '1'
   run.googleapis.com/cpu-throttling: 'false'
   ```

2. **Verify Startup & WebSocket Connectivity:**
   ```bash
   gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="polymarket-bot-paper"' \
     --limit 30 --project polymarket-bots-508306
   ```
   Check for:
   - `[BINANCE] WebSocket connected`
   - `[COINBASE] WebSocket connected`
   - `[KRAKEN] WebSocket connected`
   - `Seeded ... historical 15m candles into active strategy`
   - Zero `BlockedThreadChecker` warnings.

3. **Verify Dashboard:**
   Open the assigned Cloud Run URL (e.g. `https://polymarket-bot-paper-557828324948.europe-west1.run.app`) to view live status, rolling TWAPs, and trade logs.

---
name: aws-deployment
description: >-
  AWS eu-west-1 (Dublin) infrastructure, CloudFormation templates in infra/
  (polymarket-bot-base.yaml, polymarket-bot-ec2.yaml),
  Maven SemVer release workflow, Bare-OS systemd EC2 deployments with
  runtime-state preservation and automatic health-check rollback via SSM,
  Secrets Manager safety, and CloudWatch/EBS operations. Use when deploying
  releases, modifying CloudFormation stacks or infra scripts, inspecting
  production logs or telemetry, or working with AWS resources.
---

# AWS Deployment & Infrastructure Guide (`eu-west-1`)

## 1. AWS Governance & Constraints

- **Assigned Region**: All regional resources MUST be deployed in **`eu-west-1` (Ireland / Dublin)** (~5-8 ms backbone RTT to Polymarket CLOB in `eu-west-2` London). Never create regional resources (EC2, EBS, Lambda, S3, ECR) in other regions, and do not use cross-region StackSets, cross-region replication, or Lambda@Edge.
- **Tooling & IaC**:
  - Prefer the **AWS MCP Server** and specialized `aws-*` skills over ad-hoc CLI commands; verify unfamiliar AWS parameters/limits against documentation.
  - **AWS CloudFormation** (`infra/*.yaml`) is the mandatory standard for all production infrastructure. Use hyphens (never em dashes) in AWS resource names and descriptions.
- **Secret Safety**:
  - NEVER call `secretsmanager get-secret-value` or `batch-get-secret-value` in agent context; resolve secrets dynamically at runtime (`{{resolve:secretsmanager:secret-id:SecretString:json-key}}` with `asm-exec`).

---

## 2. Bare-OS Production Invariants (`polymarket-bot`)

- **Single Unified Host (`c8gn.xlarge` + 100 GiB `gp3` EBS)**: Runs the unified `bot` module (Strategy Engine in `LIVE` / `PAPER` / `OFF` + continuous `.jsonl.zst` Data Collector) directly on **Amazon Linux 2023 (ARM64)** with **Eclipse Temurin 25** via **`systemd`** (`polymarket-bot.service`, `-Xms4g -Xmx4g` ZGC) with `AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_SYS_NICE CAP_IPC_LOCK`, `CPUAffinity=0`, and `Nice=-20`.
- **Persistent 100 GiB EBS (`/mnt/ebs/polymarket-bot`)**: Stores `/mnt/ebs/polymarket-bot/collector`, `/mnt/ebs/polymarket-bot/paper`, and `/mnt/ebs/polymarket-bot/state/runtime-state.json` (`DeletionPolicy: Retain`, `Encrypted: true`).
- **State Continuity & Auto-Rollback (`/usr/local/bin/deploy-polymarket-bot.sh`)**:
  - Pre-stages `bot-{x.y.z}.tar.gz` into `/opt/polymarket-bot.staging` while the active service runs.
  - Stops `polymarket-bot` (`SIGTERM` saves `runtime-state.json` with `twapOpenPrice`, `strategyMode`, and open `Position`s + flushes `.jsonl.zst`), swaps `/opt/polymarket-bot` -> `/opt/polymarket-bot.previous` and `.staging` -> `/opt/polymarket-bot` in `< 1 ms`, and starts the new version.
  - Verifies `GET http://127.0.0.1:443/api/health` returns `.healthy == true` twice consecutively; on failure, stops the broken version and **automatically rolls back to `/opt/polymarket-bot.previous`**.
- **Access & Security**:
  - Inbound port `443` (bound directly by Quarkus) is whitelisted strictly for **`178.17.13.13/32`**.
  - Public SSH (port `22`) is closed; all administration and deployments run via **AWS Systems Manager (SSM)**.

---

## 3. Day-2 Code Release & Update Workflow (PowerShell)

Schedule deployments near the start of a new 15m interval; never recreate EC2 instances for application updates:

```powershell
# 1. Build SemVer release artifacts (.\bot\target\quarkus-app\app\bot-{x.y.z}.jar):
.\mvnw -B release:clean release:prepare
# Or specify explicit version:
.\mvnw -B release:clean release:prepare "-DreleaseVersion=1.4.0"

# 2. Package bot-{x.y.z}.tar.gz, upload to S3 artifact bucket, deploy via SSM, verify /api/health (or auto-rollback):
.\infra\deploy-bot.ps1
```

---

## 4. Service-Specific Runbooks (Progressive References)

Read **only** the reference document relevant to your current task:

- **[references/polymarket-bot.md](references/polymarket-bot.md)**: Unified `bot` CloudFormation stacks (`polymarket-bot-base.yaml` with 100 GiB `gp3` EBS, `polymarket-bot-ec2.yaml`), `c8gn.xlarge` 4-core pinning, ZGC flags, state persistence, and auto-rollback.
- **[references/operations.md](references/operations.md)**: Unified dashboard/telemetry/mode endpoints, SSM port forwarding, and CloudWatch log tailing.

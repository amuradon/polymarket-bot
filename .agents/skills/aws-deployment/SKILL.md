---
name: aws-deployment
description: >-
  AWS eu-west-1 (Dublin) infrastructure, CloudFormation templates in infra/,
  Maven SemVer release workflow, Bare-OS systemd EC2 deployments via SSM,
  Secrets Manager safety, and CloudWatch/EBS operations for live, data, and
  Telegram Alert Lambda. Use when deploying releases, modifying CloudFormation
  stacks or infra scripts, inspecting production logs or telemetry, or working
  with AWS resources.
---

# AWS Deployment & Infrastructure Guide (`eu-west-1`)

## 1. AWS Governance & Constraints

- **Assigned Region**: All regional resources MUST be deployed in **`eu-west-1` (Ireland / Dublin)** (~5-8 ms backbone RTT to Polymarket CLOB in `eu-west-2` London). Never create regional resources (EC2, EBS, Lambda, S3, ECR) in other regions, and do not use cross-region StackSets, cross-region replication, or Lambda@Edge.
- **Tooling & IaC**:
  - Prefer the **AWS MCP Server** and specialized `aws-*` skills over ad-hoc CLI commands; verify unfamiliar AWS parameters/limits against documentation.
  - **AWS CloudFormation** (`infra/*.yaml`) is the mandatory standard for all production infrastructure. Use hyphens (never em dashes) in AWS resource names and descriptions.
- **Secret Safety**:
  - Load the `aws-secrets-manager` skill before any credential, API key, or secret task.
  - NEVER call `secretsmanager get-secret-value` or `batch-get-secret-value` in agent context; resolve secrets dynamically at runtime (`{{resolve:secretsmanager:secret-id:SecretString:json-key}}` with `asm-exec`).

---

## 2. Shared Bare-OS Production Invariants (`live` & `data`)

- **Bare-OS Direct Execution (No Docker on EC2)**: Both `live` and `data` run directly on **Amazon Linux 2023 (ARM64)** with **Eclipse Temurin 25** via **`systemd`** (`polymarket-live.service` and `polymarket-data@{blue,green}.service`) with `AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_SYS_NICE CAP_IPC_LOCK` and `Nice=-20`, avoiding `dockerd`, `containerd`, `seccomp-bpf`, and `iptables` NAT overhead.
- **Kernel & Network Tuning**: Configured in `/etc/sysctl.d/99-polymarket-network.conf` (`default_qdisc = fq`, `tcp_congestion_control = bbr`, `rmem_default = 262144`, `rmem_max = 33554432`, `tcp_rmem = 4096 262144 33554432`, `tcp_wmem = 4096 65536 16777216`, `tcp_reordering = 32`, `tcp_max_reordering = 600`, `tcp_early_retrans = 0`, `tcp_autocorking = 0`, `tcp_keepalive_time = 30`, `tcp_keepalive_intvl = 5`, `tcp_keepalive_probes = 3`, `tcp_retries2 = 8`, `busy_read = 100`, `busy_poll = 100`, `netdev_max_backlog = 30000`, `ip_unprivileged_port_start = 443`) paired with native Linux `epoll` (`quarkus.vertx.prefer-native-transport=true`), IPv4-only JVM sockets (`-Djava.net.preferIPv4Stack=true`), and ENA IRQ pinning/coalescing on `CPU 0` (`polymarket-nic-tuning.service`).
- **Access & Security**:
  - Inbound port `443` (bound directly by Quarkus) is whitelisted strictly for **`178.17.13.13/32`**.
  - Public SSH (port `22`) is closed; all administration and deployments run via **AWS Systems Manager (SSM)**.

---

## 3. Day-2 Code Release & Update Workflow (PowerShell)

Once CloudFormation stacks are provisioned, never recreate EC2 instances for application updates:

```powershell
# 1. Build SemVer release artifacts (.\live\target\quarkus-app\... and .\data\target\quarkus-app\...):
.\mvnw -B release:clean release:prepare
# Or specify explicit version:
.\mvnw -B release:clean release:prepare "-DreleaseVersion=1.3.2"

# 2. Package {module}-{x.y.z}.tar.gz, upload to S3 artifact bucket, and deploy via SSM:
.\infra\deploy-live.ps1
.\infra\deploy-data.ps1

# Update Telegram Alert Lambda container image & function code:
.\infra\deploy-telegram-alert.ps1 -Version "1.0.2"
```

---

## 4. Service-Specific Runbooks (Progressive References)

Read **only** the reference document relevant to your current task:

- **[references/live-trading.md](references/live-trading.md)**: `live` CloudFormation stacks (`live-trading-base.yaml`, `live-trading-ec2.yaml`), `c8gn.xlarge` 4-core thread pinning, and Generational ZGC flags.
- **[references/data-collector.md](references/data-collector.md)**: `data` CloudFormation stacks (`data-collector-base.yaml`, `data-collector-ec2.yaml`), `t4g.small` Unlimited, 50 GB `gp3` EBS (`vol-028ed5d093f87c4f8`) hot-swap, and zero-loss Blue-Green `systemd` cutover.
- **[references/telegram-alert.md](references/telegram-alert.md)**: ECR + Java 25 Telegram Alert Lambda stacks (`telegram-alert-base.yaml`, `telegram-alert-lambda.yaml`) and CloudWatch subscription filters.
- **[references/operations.md](references/operations.md)**: Dashboard/telemetry endpoints, SSM port forwarding, and CloudWatch log tailing.

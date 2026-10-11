# Deployment Guide - Polymarket Bot on AWS EC2 with EBS (`eu-west-1`)

This guide outlines the deployment procedures, instance configuration, persistent storage setup, and **mandatory continuous execution requirements** for running Polymarket Bot applications (`data`, `paper`, `live`) on **Amazon Web Services (AWS) EC2 with Amazon EBS** in the **Ireland (`eu-west-1`)** region.

---

## 1. Mandatory EC2 & EBS Configuration (CRITICAL)

Unlike standard stateless web APIs that only respond to inbound HTTP requests, the Polymarket Bot application is an **active real-time daemon** that:
- Maintains persistent, bidirectional WebSockets to crypto exchanges (Binance, Coinbase, Kraken) and Polymarket CLOB.
- Computes rolling 60s TWAP indicators and state updates every second on Vert.x event loops.
- Continuously records compressed `.jsonl.zst` market streams and executes real-time strategies with sub-second latency.

Therefore, when provisioning and deploying on AWS EC2 in `eu-west-1`, **the following requirements MUST ALWAYS be met**:

| Parameter | Value / Configuration | Rationale & Impact |
| :--- | :--- | :--- |
| **AWS Region** | `eu-west-1` (Ireland / Dublin) | **MANDATORY**: Physical and network proximity across the Irish Sea (~5-8 ms RTT over the AWS backbone) to Polymarket CLOB servers in AWS London (`eu-west-2`). |
| **Continuous Execution** | `24/7 Running Instance` + `--restart unless-stopped` | **MANDATORY**: The trading and data recorder daemons must never scale to zero or suspend when no browser is connected to the dashboard. |
| **CPU Allocation** | Dedicated vCPU (e.g. `c7g.medium` / `m7g.medium`) or `T4g/T3` with **Unlimited CPU Credits** | **MANDATORY**: If CPU is throttled due to exhausted burstable credits, background WebSockets and Vert.x timers freeze, causing `BlockedThreadChecker` exceptions, clock jumps, and dropped market data. |
| **Persistent Storage** | **Amazon EBS (`gp3`)** mounted at `/work/data` | **MANDATORY**: Ensures recorded `.jsonl.zst` streams and paper trading state survive container restarts and upgrades. |
| **Memory** | `2 GiB+` RAM (`-Xmx1g` or `-Xmx1536m`) | Sufficient heap and off-heap memory for Java 25 runtime, Zstandard native buffers, in-memory price caches, and historical klines. |

> [!CAUTION]
> **NEVER deploy on throttled compute or ephemeral container storage without an EBS volume mount.**
> CPU throttling or instance hibernation will freeze Vert.x event loops, drop exchange WebSocket feeds, and corrupt continuous time-series recording.

---

## 2. Deployment Protocol (`eu-west-1`)

### Step 1: Prepare Persistent EBS Volume on EC2
Ensure an Amazon EBS `gp3` volume is attached and mounted on the EC2 host (e.g. at `/mnt/ebs/polymarket-data`) with appropriate write permissions for the Quarkus container user (`uid=185`):

```bash
sudo mkdir -p /mnt/ebs/polymarket-data/paper /mnt/ebs/polymarket-data/streams
sudo chown -R 185:185 /mnt/ebs/polymarket-data
```

### Step 2: Build & Push Docker Container to Amazon ECR (`eu-west-1`)
From the repository root, build the JVM container image and push it to Amazon ECR in `eu-west-1`:

```bash
export AWS_REGION=eu-west-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/polymarket-bot-paper"

aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker build -t ${ECR_URI}:latest -f paper/src/main/docker/Dockerfile.jvm .
docker push ${ECR_URI}:latest
```

### Step 3: Deploy / Update Container on EC2 (`eu-west-1`)
On the EC2 instance in `eu-west-1`, pull the latest image and run the container with the persistent EBS volume bind-mounted:

```bash
export AWS_REGION=eu-west-1
export ECR_URI="<account-id>.dkr.ecr.eu-west-1.amazonaws.com/polymarket-bot-paper"

aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin "<account-id>.dkr.ecr.eu-west-1.amazonaws.com"
docker pull ${ECR_URI}:latest

docker stop polymarket-bot-paper || true
docker rm polymarket-bot-paper || true

docker run -d \
  --name polymarket-bot-paper \
  --restart unless-stopped \
  -p 8080:8082 \
  -v /mnt/ebs/polymarket-data/paper:/work/data/paper \
  -e POLYMARKET_PAPER_DATA_DIR=/work/data/paper \
  ${ECR_URI}:latest
```

---

## 3. Operational Verification

1. **Verify Container Status & Restart Policy:**
   ```bash
   docker inspect -f '{{.State.Status}} | RestartPolicy={{.HostConfig.RestartPolicy.Name}}' polymarket-bot-paper
   ```
   Confirm the output shows `running | RestartPolicy=unless-stopped`.

2. **Verify EBS Volume Mount & Free Space:**
   ```bash
   df -h /mnt/ebs/polymarket-data
   ```

3. **Verify Startup & WebSocket Connectivity:**
   ```bash
   docker logs --tail 50 polymarket-bot-paper
   ```
   Check for:
   - `[BINANCE] WebSocket connected`
   - `[COINBASE] WebSocket connected`
   - `[KRAKEN] WebSocket connected`
   - `Seeded ... historical 15m candles into active strategy`
   - Zero `BlockedThreadChecker` warnings.

4. **Verify Dashboard:**
   Open the EC2 instance endpoint (`http://<ec2-host>:8080`) to view live status, rolling TWAPs, and trade logs.

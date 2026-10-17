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

---

## 4. Data Collector Deployment (`data` Module in `eu-west-1`)

The `data` module runs as a standalone 24/7 market recorder daemon on EC2 in `eu-west-1` with a dedicated 50 GB encrypted `gp3` EBS volume (`DeletionPolicy: Retain`). It records:
- **Binance Futures (`BTCUSDT`)**: `aggTrade`, `bookTicker`, `depth20@100ms`, `markPrice@1s`, `forceOrder`
- **Deribit Futures (`BTC-PERPETUAL`, `ETH-PERPETUAL`, `SOL-PERPETUAL`)**: `trades.<instrument>.100ms`
- **Polymarket PolyBolt (`wss://ws-live-v2.polymarket.com/ws`)**: `price.crypto` (`btcusd` live Chainlink tick) and `price.crypto.twap` (`btcusd` 60s TWAP)
- **Polymarket CLOB (`wss://ws-subscriptions-clob.polymarket.com/ws/market`)**: all active BTC (`5m`, `15m`, `1h`, `4h`), ETH (`15m`), and SOL (`15m`) markets (`book`, `price_change`, `last_trade_price`, `best_bid_ask`)

### Step 1: Store Mandatory PolyBolt Credentials in AWS Secrets Manager (`eu-west-1`)
Because the Data Collector enforces fail-fast validation on startup if `POLYMARKET_API_KEY`, `POLYMARKET_SECRET`, or `POLYMARKET_PASSPHRASE` are missing, store them in AWS Secrets Manager (`polymarket-bot/data-collector/credentials`) in `eu-west-1`.

### Step 2: Provision Infrastructure via CloudFormation (`infra/data-collector-ec2.yaml`)
Deploy the CloudFormation stack `polymarket-bot-data` in `eu-west-1`:
- Provisions an ECR repository (`polymarket-bot-data-repo`) with `ScanOnPush: true` and lifecycle retention.
- Provisions a `t3.small` instance with **Unlimited CPU Credits** (`CPUCredits: unlimited`), **IMDSv2** (`HttpTokens: required`, `HttpPutResponseHopLimit: 2`), and zero inbound Security Group ports (managed exclusively via **AWS Systems Manager (SSM)**).
- Provisions and mounts a 50 GB encrypted `gp3` EBS volume at `/mnt/ebs/polymarket-data`.

```bash
aws cloudformation deploy \
  --region eu-west-1 \
  --stack-name polymarket-bot-data \
  --template-file infra/data-collector-ec2.yaml \
  --capabilities CAPABILITY_NAMED_IAM
```

### Step 3: Build & Push Data Collector Image to ECR (`eu-west-1`)
```bash
./mvnw package -DskipTests -pl connector,data
docker build -t <account-id>.dkr.ecr.eu-west-1.amazonaws.com/polymarket-bot-data-repo:latest -f data/src/main/docker/Dockerfile.jvm .
docker push <account-id>.dkr.ecr.eu-west-1.amazonaws.com/polymarket-bot-data-repo:latest
```

### Step 4: Deploy Container on EC2 via AWS Systems Manager (SSM)
On the EC2 host (via `aws ssm send-command`), resolve the secret into `/etc/polymarket-collector.env` (`chmod 600`) and run the container with `--restart unless-stopped` and the EBS volume mounted:

```bash
docker run -d \
  --name polymarket-bot-data \
  --restart unless-stopped \
  -p 8084:8084 \
  --env-file /etc/polymarket-collector.env \
  -e POLYMARKET_COLLECTOR_DATA_DIR=/data/collector \
  -e JAVA_OPTS="-Xmx1g -XX:+UseZGC --enable-native-access=ALL-UNNAMED" \
  -v /mnt/ebs/polymarket-data:/data \
  <account-id>.dkr.ecr.eu-west-1.amazonaws.com/polymarket-bot-data-repo:latest
```

### Step 5: Verify Status & Download Daily `.jsonl.zst` Archives
Check collector health and stream counters via SSM:
```bash
curl -s http://localhost:8084/api/collector/status | jq .
```
Daily rotated `.jsonl.zst` files are stored on the persistent EBS volume under `/mnt/ebs/polymarket-data/collector/{binance,deribit,polymarket}/...`.

#### Automated Download, Verification & Remote Cleanup Script
Use the PowerShell script [`scripts/download-collector-data.ps1`](scripts/download-collector-data.ps1) to download all Binance, Deribit, and Polymarket data for a given UTC day directly into `D:\Crypto\data\Polymarket\collector` with the identical subdirectory structure. The script verifies 100% of the downloaded files locally before deleting them from the EC2 instance:

```powershell
# Preview files for a date without downloading or deleting:
.\scripts\download-collector-data.ps1 -Date 2026-10-15 -DryRun

# Download, verify, and delete remote files on EC2:
.\scripts\download-collector-data.ps1 -Date 2026-10-15

# Download and verify while keeping remote copies on EC2:
.\scripts\download-collector-data.ps1 -Date 2026-10-15 -KeepRemoteFiles
```



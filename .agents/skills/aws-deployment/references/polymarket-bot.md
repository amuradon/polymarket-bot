# Unified Polymarket Bot Deployment (`bot`)

## Compute, Storage & Runtime Profile
- **Instance**: Dedicated non-throttled **`c8gn.xlarge`** (4 vCPU Graviton4 ARM64, 8 GiB RAM, 30 Gbps Enhanced Networking / Nitro v5).
- **Persistent Storage**: Encrypted **100 GiB `gp3` EBS** (`DeletionPolicy: Retain`) mounted at `/mnt/ebs/polymarket-bot` via `/usr/local/bin/mount-polymarket-ebs.sh`:
  - `/mnt/ebs/polymarket-bot/collector`: Continuous `.jsonl.zst` archives (PolyBolt + deduplicated CLOB `current + 1` lookahead market, max `3` CLOB market connections).
  - `/mnt/ebs/polymarket-bot/paper`: Paper trading `trades.jsonl` + `metrics.json`.
  - `/mnt/ebs/polymarket-bot/state/runtime-state.json`: Persisted `RuntimeStateSnapshot` (`twapOpenPrice`, `strategyMode`, active market tokens, open `Position`s).
- **Core Mapping**:
  - `CPU 0`: ENA NIC IRQs (`rx-usecs 25`, `tx-usecs 64`), ZGC, Quarkus `:443`, Data Collector Zstandard writers, `telemetry-maintenance-worker`, and `clob-user-worker` (`POLYMARKET-CLOB-USER` in `LIVE`).
  - `CPU 1`: Consolidated single-EventLoop native `epoll` + BoringSSL `market-ws-io-reader`.
  - `CPU 2`: Fused lock-free L1-cache `market-strategy-worker` (`PolymarketLocalBook` + `PolyBolt` + `CalculatedSpotTwapEngine` + `TradingStrategy`).
  - `CPU 3`: Dedicated isolated `order-execution-io-worker` (EIP-712 signing + `POST /order`).
- **JVM**: Java 25 ZGC (`-XX:+UseZGC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseThreadPriorities -XX:ActiveProcessorCount=4 -XX:ConcGCThreads=1 -XX:ParallelGCThreads=2 -Xms4g -Xmx4g -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv6Addresses=false`).
- **Service & Logs**: `/etc/systemd/system/polymarket-bot.service` deployed via `/usr/local/bin/deploy-polymarket-bot.sh`; logs written to `/var/log/polymarket-bot/bot.log` and streamed to `/polymarket-bot/polymarket-bot-base/bot`.

---

## Step 1: Provision Base Stack (`infra/polymarket-bot-base.yaml`)
Creates encrypted `PolymarketBotArtifactBucket` (S3), CloudWatch Log Group `/polymarket-bot/polymarket-bot-base/bot`, IAM role (SSM/S3/SecretsManager/Logs/EBS), Security Group (`443` and `22` from `178.17.13.13/32`), and persistent **100 GiB `gp3` EBS volume** (or reuses `ExistingEbsVolumeId` when specified).

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-base `
  --template-file infra/polymarket-bot-base.yaml `
  --capabilities CAPABILITY_NAMED_IAM `
  --parameter-overrides EbsVolumeSizeGb=100 AllowedInboundCidr="178.17.13.13/32"
```

## Step 2: Provision Compute Stack (`infra/polymarket-bot-ec2.yaml`)
Creates the 24/7 `c8gn.xlarge` EC2 host, mounts the 100 GiB EBS volume without reformatting existing XFS data, installs ARM64 Temurin 25, applies HFT `sysctl` and ENA IRQ pinning, configures CloudWatch Agent, and registers `polymarket-bot.service`.

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-ec2 `
  --template-file infra/polymarket-bot-ec2.yaml `
  --parameter-overrides BaseStackName="polymarket-bot-base" InstanceType="c8gn.xlarge" StrategyMode="LIVE"
```

## Step 3: Build Release & Deploy with Auto-Rollback (`infra/deploy-bot.ps1`)
Builds `.\bot\target\quarkus-app\app\bot-{x.y.z}.jar`, verifies Quarkus CDI beans, uploads `bot-{x.y.z}.tar.gz` to S3, runs `deploy-polymarket-bot.sh` via SSM, verifies `GET http://127.0.0.1:443/api/health`, and automatically rolls back to `/opt/polymarket-bot.previous` if unhealthy:

```powershell
.\mvnw -B release:clean release:prepare
# Or explicit version: .\mvnw -B release:clean release:prepare "-DreleaseVersion=1.4.0"

.\infra\deploy-bot.ps1
```

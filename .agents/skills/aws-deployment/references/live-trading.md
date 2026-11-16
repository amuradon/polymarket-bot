# Live Trading Application Deployment (`live`)

## Compute & Runtime Profile
- **Instance**: Dedicated non-throttled **`c8gn.xlarge`** (4 vCPU Graviton4 ARM64, 8 GiB RAM, 30 Gbps Enhanced Networking / Nitro v5).
- **Core Mapping**: 4 physical Neoverse-V2 cores mapped to dedicated lock-free threads: `clob-ws-io-reader`, `clob-book-worker`, `strategy-calculator-worker`, `order-execution-io-worker`.
- **JVM**: Java 25 Generational ZGC (`-XX:+UseZGC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseThreadPriorities -Xms6g -Xmx6g`).
- **Service & Logs**: `/etc/systemd/system/polymarket-live.service` deployed via `/usr/local/bin/deploy-polymarket-live.sh`; logs written to `/var/log/polymarket-live/live-trading.log` and streamed by CloudWatch Agent to `/polymarket-bot/polymarket-bot-live-base/live-trading`.

---

## Step 1: Provision Base Stack (`infra/live-trading-base.yaml`)
Creates encrypted `LiveTradingArtifactBucket` (S3), CloudWatch Log Group, IAM role (SSM/S3/SecretsManager), and Security Group allowing inbound port `443` strictly from `178.17.13.13/32`.

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-live-base `
  --template-file infra/live-trading-base.yaml `
  --capabilities CAPABILITY_NAMED_IAM `
  --parameter-overrides AllowedInboundCidr="178.17.13.13/32"
```

## Step 2: Provision Compute Stack (`infra/live-trading-ec2.yaml`)
Creates the 24/7 `c8gn.xlarge` EC2 host, installs ARM64 Temurin 25, applies kernel `sysctl` network tuning, configures CloudWatch Agent, and registers `polymarket-live.service`.

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-live-ec2 `
  --template-file infra/live-trading-ec2.yaml `
  --parameter-overrides BaseStackName="polymarket-bot-live-base" InstanceType="c8gn.xlarge"
```

## Step 3: Build Release & Deploy (`infra/deploy-live.ps1`)
Builds `.\live\target\quarkus-app\app\live-{x.y.z}.jar`, packages `live-{x.y.z}.tar.gz`, uploads to S3, and triggers atomic `systemd` deployment via SSM:

```powershell
.\mvnw -B release:clean release:prepare
# Or explicit version: .\mvnw -B release:clean release:prepare "-DreleaseVersion=1.3.2"

.\infra\deploy-live.ps1
```

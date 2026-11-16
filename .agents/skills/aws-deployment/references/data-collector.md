# Market Data Collector Deployment (`data`)

## Compute, Storage & Zero-Loss Blue-Green Cutover
- **Instance & Storage**: **`t4g.small`** (Graviton2 ARM64, 2 vCPU with **Unlimited CPU Credits**, 2 GiB RAM + 2 GiB EBS swapfile) + persistent 50 GB `gp3` XFS EBS volume (`vol-028ed5d093f87c4f8`) mounted at `/mnt/ebs/polymarket-data` (`/usr/local/bin/mount-polymarket-ebs.sh`).
- **JVM**: Java 25 Generational ZGC (`-XX:+UseZGC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseThreadPriorities -Xms512m -Xmx640m`).
- **Logs & Secrets**: IAM access to `polymarket-sniper/clob-credentials`; logs at `/var/log/polymarket-data/data-collector.log` streamed to `/polymarket-bot/polymarket-bot-data-base/data-collector`.
- **In-Host Blue-Green `systemd` Slots**:
  - `polymarket-data@blue` (control port `8084`) and `polymarket-data@green` (control port `8085`); the active slot binds public port `443`.
  - `/usr/local/bin/deploy-polymarket-data.sh` boots the new release in the inactive slot with `POLYMARKET_COLLECTOR_INITIAL_WRITE_MODE=STANDBY` (connecting all WS streams and buffering frames in memory without touching `.jsonl.zst` files).
  - Once `/api/collector/status` reports `readyForCutover == true`, it calls `/api/collector/cutover/freeze` on the old slot (closing Zstd frames at `T_cutover`) and `/api/collector/cutover/activate?afterTimestampMs=T_cutover` on the new slot (replaying frames with `ts > T_cutover`), achieving **0 ms data loss and 0 duplicate records**. Failed readiness aborts cleanly without disturbing the active slot.

---

## Step 1: Provision Base Stack (`infra/data-collector-base.yaml`)
Creates encrypted `DataCollectorArtifactBucket`, CloudWatch Log Group, IAM role, Security Group (`443` from `178.17.13.13/32`), and binds persistent EBS volume `vol-028ed5d093f87c4f8` (or creates a new volume if empty).

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-data-base `
  --template-file infra/data-collector-base.yaml `
  --capabilities CAPABILITY_NAMED_IAM `
  --parameter-overrides `
      ExistingEbsVolumeId="vol-028ed5d093f87c4f8" `
      AllowedInboundCidr="178.17.13.13/32"
```

## Step 2: Provision Compute Stack (`infra/data-collector-ec2.yaml`)
Provisions the 24/7 `t4g.small` host. If `vol-028ed5d093f87c4f8` is still attached to a previous instance, `UserData` finishes booting without blocking so the old host records uninterrupted until Step 3 performs the ~6-8s EBS hot-swap.

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-data-ec2 `
  --template-file infra/data-collector-ec2.yaml `
  --parameter-overrides BaseStackName="polymarket-bot-data-base" InstanceType="t4g.small"
```

## Step 3: Build, Deploy & EBS Hot-Swap (`infra/deploy-data.ps1`)
Packages `data-{x.y.z}.tar.gz`, uploads to S3, and deploys via SSM (when migrating hosts, automatically pre-stages the release, snapshots `vol-028ed5d093f87c4f8`, unmounts/detaches from the old host, attaches/mounts on the new host without reformatting in ~6-8s, and starts recording):

```powershell
.\mvnw -B release:clean release:prepare
.\infra\deploy-data.ps1

# Only after migrating away from a legacy monolithic stack:
# aws cloudformation delete-stack --region eu-west-1 --stack-name polymarket-bot-data
```

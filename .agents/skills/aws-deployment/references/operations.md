# Production Operations, Port Forwarding & Logs

## 1. Unified Web Dashboard, Mode Switching, Health & Telemetry
- Direct access from whitelisted IP `178.17.13.13`:
  - **Dashboard**: `http://<Bot-EC2-Public-IP>:443/`
  - **Health / Status / Telemetry**: `/api/health`, `/api/status`, `/api/telemetry`, `/api/collector/status`
  - **Switch Strategy Mode (`LIVE` | `PAPER` | `OFF`)**: `POST /api/mode` with `{"mode":"PAPER"}` / `{"mode":"OFF"}` / `{"mode":"LIVE"}`
- Or via **AWS SSM Port Forwarding** from any authorized workstation (PowerShell):

```powershell
$BotInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-ec2 --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)
aws ssm start-session `
  --target $BotInstanceId `
  --region eu-west-1 `
  --document-name AWS-StartPortForwardingSession `
  --parameters "portNumber=443,localPortNumber=8080"
```

---

## 2. CloudWatch Log Tailing (PowerShell)

```powershell
aws logs tail "/polymarket-bot/polymarket-bot-base/bot" --region eu-west-1 --follow
```

---

## 3. Syncing Recorded `.jsonl.zst` Market Streams from EBS
For full options and DuckDB Parquet conversion, load the `data-and-backtest-ops` skill:

```powershell
# Dry-run preview for a UTC date:
.\scripts\download-collector-data.ps1 -Date 2026-11-08 -DryRun

# Download, verify byte sizes, and delete verified files from EC2 EBS (/mnt/ebs/polymarket-bot/collector):
.\scripts\download-collector-data.ps1 -Date 2026-11-08
```

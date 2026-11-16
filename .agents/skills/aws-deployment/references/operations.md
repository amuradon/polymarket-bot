# Production Operations, Port Forwarding & Logs

## 1. Web Dashboard, Telemetry & Collector Status
- Direct access from whitelisted IP `178.17.13.13`:
  - **Live Trading**: `http://<Live-EC2-Public-IP>:443`, `/api/live/telemetry`, `/api/live/status`
  - **Data Collector**: `http://<Data-EC2-Public-IP>:443/api/collector/status`
- Or via **AWS SSM Port Forwarding** from any authorized workstation (PowerShell):

```powershell
# Live Trading (EC2 port 443 -> localhost:8080):
$LiveInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-live-ec2 --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)
aws ssm start-session `
  --target $LiveInstanceId `
  --region eu-west-1 `
  --document-name AWS-StartPortForwardingSession `
  --parameters "portNumber=443,localPortNumber=8080"

# Data Collector (EC2 port 443 -> localhost:8084):
$DataInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-data-ec2 --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)
aws ssm start-session `
  --target $DataInstanceId `
  --region eu-west-1 `
  --document-name AWS-StartPortForwardingSession `
  --parameters "portNumber=443,localPortNumber=8084"
```

---

## 2. CloudWatch Log Tailing (PowerShell)

```powershell
# Live Trading logs:
aws logs tail "/polymarket-bot/polymarket-bot-live-base/live-trading" --region eu-west-1 --follow

# Data Collector logs:
aws logs tail "/polymarket-bot/polymarket-bot-data-base/data-collector" --region eu-west-1 --follow
```

---

## 3. Syncing Recorded `.jsonl.zst` Market Streams from EBS
For full options and DuckDB Parquet conversion, load the `data-and-backtest-ops` skill:

```powershell
# Dry-run preview for a UTC date:
.\scripts\download-collector-data.ps1 -Date 2026-11-08 -DryRun

# Download, verify byte sizes, and delete verified files from EC2 EBS:
.\scripts\download-collector-data.ps1 -Date 2026-11-08
```

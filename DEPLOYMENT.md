# Deployment Guide - Polymarket Bot on AWS EC2 with CloudFormation (`eu-west-1`)

This guide outlines the deployment procedures, infrastructure provisioning, container image build workflow, and **mandatory continuous execution requirements** for running the Polymarket Bot applications ([`live`](file:///c:/work/workspace-java/polymarket-bot/live), [`data`](file:///c:/work/workspace-java/polymarket-bot/data), [`paper`](file:///c:/work/workspace-java/polymarket-bot/paper)) on **Amazon Web Services (AWS) EC2 with Amazon EBS** in the **Ireland (`eu-west-1`)** region.

**AWS CloudFormation is the preferred and mandatory standard** for provisioning and managing all production infrastructure in this repository. All resources (compute, encrypted persistent storage, networking, IAM roles, ECR repositories, and CloudWatch log routing to Telegram alerts) are defined as code in [`infra/`](file:///c:/work/workspace-java/polymarket-bot/infra).

---

## 1. Mandatory Infrastructure & Compute Requirements (CRITICAL)

Unlike standard stateless web APIs that only respond to inbound HTTP requests, the Polymarket Bot application is an **active real-time trading and recording daemon** that:
- Maintains persistent, bidirectional WebSockets to crypto exchanges (Binance, Coinbase, Kraken, Deribit) and Polymarket CLOB.
- Computes rolling 60s TWAP indicators and state updates every second on non-blocking Vert.x event loops.
- Continuously records compressed `.jsonl.zst` market streams and executes real-time strategies with sub-second latency.

When provisioning and deploying on AWS in `eu-west-1`, **the following requirements MUST ALWAYS be met**:

| Parameter | Value / Configuration | Rationale & Impact |
| :--- | :--- | :--- |
| **Preferred IaC** | **AWS CloudFormation** | **MANDATORY**: Infrastructure as Code ensures reproducibility, automated Nitro EBS mounting, CloudWatch log streams, and least-privilege IAM policies. |
| **AWS Region** | `eu-west-1` (Ireland / Dublin) | **MANDATORY**: Physical and network proximity across the Irish Sea (~5-8 ms RTT over the AWS backbone) to Polymarket CLOB servers in AWS London (`eu-west-2`). |
| **Continuous Execution** | `24/7 Running Instance` + `--restart unless-stopped` | **MANDATORY**: The trading and data recorder daemons must never scale to zero or suspend when no browser is connected to the dashboard. |
| **Compute / CPU Allocation** | Dedicated vCPU (`c8g.large` Graviton4 for Live; `t4g.small` with **Unlimited CPU Credits** for Data) | **MANDATORY**: If CPU is throttled due to exhausted burstable credits, background WebSockets and Vert.x timers freeze, causing `BlockedThreadChecker` exceptions, clock jumps, and dropped market data. |
| **Persistent Storage** | **Amazon EBS (`gp3`)** mounted at `/mnt/ebs/polymarket-data` | **MANDATORY**: Retained independently of instances (`DeletionPolicy: Retain`). Ensures live trade journals (`trades.jsonl`, `metrics.json`) and recorded `.jsonl.zst` streams survive container restarts and host redeployments. |
| **Memory** | `2 GiB+` RAM (`-Xmx1g` to `-Xmx1536m`) | Sufficient heap and off-heap memory for Java 25 runtime, ZGC garbage collection (`-XX:+UseZGC`), native Zstandard compression buffers, and in-memory price caches. |
| **Network Security** | **Zero Public Inbound Ports** (No SSH port 22, no port 8080/8084 open) | Management is performed securely via **AWS Systems Manager (SSM)** Session Manager and Run Command. Web dashboards are accessed via SSM port forwarding. |
| **IMDSv2** | `HttpTokens: required`, `HttpPutResponseHopLimit: 2` | **MANDATORY**: Required by AWS security standards and allows Dockerized applications to access EC2 instance profile credentials. |

> [!CAUTION]
> **NEVER deploy on throttled compute or ephemeral container storage without an EBS volume mount.**
> CPU throttling or instance hibernation will freeze Vert.x event loops, drop exchange WebSocket feeds, and corrupt continuous time-series recording.

---

## 2. Infrastructure as Code: CloudFormation (Preferred Method)

The repository provides production-ready AWS CloudFormation templates in [`infra/`](file:///c:/work/workspace-java/polymarket-bot/infra):

1. **Live Trading Stack ([`infra/live-trading-ec2.yaml`](file:///c:/work/workspace-java/polymarket-bot/infra/live-trading-ec2.yaml))**:
   - Provisions an Amazon ECR repository (`polymarket-bot-live-repo`) with image scanning and lifecycle policies.
   - Provisions a 24/7 `c8g.large` Graviton4 ARM64 instance with IMDSv2 token enforcement and zero inbound security group rules.
   - Creates and attaches an encrypted `gp3` Amazon EBS volume mounted at `/mnt/ebs/polymarket-data` (`DeletionPolicy: Retain`).
   - Grants least-privilege IAM permissions for AWS Systems Manager (SSM), ECR image pulls, CloudWatch Logs, and reading secrets from AWS Secrets Manager.
   - Creates a CloudWatch Logs group (`/polymarket-bot/${StackName}/live-trading`) configured with Docker's `awslogs` driver.
   - Deploys an integrated Java 25 Telegram alert AWS Lambda function subscribing to `WARN`, `ERROR`, `FATAL`, and `Exception` log patterns.
   - Generates `/usr/local/bin/start-polymarket-live.sh` on the EC2 host via UserData for automated 24/7 container execution.

2. **Market Data Collector Stack ([`infra/data-collector-ec2.yaml`](file:///c:/work/workspace-java/polymarket-bot/infra/data-collector-ec2.yaml))**:
   - Provisions an Amazon ECR repository (`polymarket-bot-data-repo`) with image scanning and retention policies.
   - Provisions a 24/7 `t4g.small` Graviton2 ARM64 instance configured with **Unlimited CPU Credits** (`CPUCredits: unlimited`).
   - Creates and attaches a dedicated 50 GB encrypted `gp3` Amazon EBS volume mounted at `/mnt/ebs/polymarket-data` (`DeletionPolicy: Retain`).
   - Configures zero inbound security group rules, allowing only necessary outbound WebSocket/HTTPS traffic.

### Step 1: Store Credentials in AWS Secrets Manager (`eu-west-1`)

Before launching the CloudFormation stacks, create the required secrets in AWS Secrets Manager in `eu-west-1`:

- **Polymarket Live Trading Credentials** (`polymarket-sniper/clob-credentials`):
  ```json
  {
    "api_key": "YOUR_POLYMARKET_API_KEY",
    "secret": "YOUR_POLYMARKET_SECRET",
    "passphrase": "YOUR_POLYMARKET_PASSPHRASE",
    "signer_key": "YOUR_PRIVATE_KEY",
    "signer_address": "0xYourSignerAddress",
    "address": "0xYourFunderAddress"
  }
  ```

- **Telegram Alert Bot Credentials** (`polymarket-sniper/telegram-credentials`):
  ```json
  {
    "bot_token": "YOUR_TELEGRAM_BOT_TOKEN",
    "chat_id": "YOUR_TELEGRAM_CHAT_ID"
  }
  ```

- **Data Collector Credentials** (`polymarket-bot/data-collector/credentials`):
  ```json
  {
    "POLYMARKET_API_KEY": "YOUR_KEY",
    "POLYMARKET_SECRET": "YOUR_SECRET",
    "POLYMARKET_PASSPHRASE": "YOUR_PASSPHRASE",
    "DERIBIT_CLIENT_ID": "YOUR_DERIBIT_CLIENT_ID",
    "DERIBIT_CLIENT_SECRET": "YOUR_DERIBIT_CLIENT_SECRET"
  }
  ```

### Step 2: Deploy CloudFormation Stacks

Deploy the infrastructure using the AWS CLI.

#### Windows 11 (PowerShell) - Primary

```powershell
# Set default AWS region
$env:AWS_REGION = "eu-west-1"

# 1. Deploy Live Trading Stack (c8g.large, EBS, ECR, CloudWatch Logs, Telegram Alert Lambda)
aws cloudformation deploy `
  --region $env:AWS_REGION `
  --stack-name polymarket-bot-live `
  --template-file infra/live-trading-ec2.yaml `
  --capabilities CAPABILITY_NAMED_IAM

# 2. Deploy Data Collector Stack (t4g.small Unlimited, 50 GB EBS, ECR)
$CollectorSecretArn = (aws secretsmanager describe-secret --secret-id "polymarket-bot/data-collector/credentials" --region $env:AWS_REGION --query "ARN" --output text)

aws cloudformation deploy `
  --region $env:AWS_REGION `
  --stack-name polymarket-bot-data `
  --template-file infra/data-collector-ec2.yaml `
  --capabilities CAPABILITY_NAMED_IAM `
  --parameter-overrides PolymarketSecretArn="$CollectorSecretArn"
```

#### Linux / macOS (Bash) - Alternative

```bash
export AWS_REGION="eu-west-1"

# 1. Deploy Live Trading Stack
aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name polymarket-bot-live \
  --template-file infra/live-trading-ec2.yaml \
  --capabilities CAPABILITY_NAMED_IAM

# 2. Deploy Data Collector Stack
COLLECTOR_SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "polymarket-bot/data-collector/credentials" --region "${AWS_REGION}" --query "ARN" --output text)

aws cloudformation deploy \
  --region "${AWS_REGION}" \
  --stack-name polymarket-bot-data \
  --template-file infra/data-collector-ec2.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides PolymarketSecretArn="${COLLECTOR_SECRET_ARN}"
```

---

## 3. Building & Pushing Docker Images

The production EC2 hosts deployed by CloudFormation (`c8g.large` and `t4g.small`) run on **AWS Graviton (ARM64)** architecture. Because local development workstations (Windows 11 or Linux x86_64 PCs) commonly use AMD64/x86_64 processors, all Docker container images **MUST be cross-compiled for `linux/arm64`** using **Docker Buildx**.

### 3.1 Local Windows 11 Host (Primary Workflow)

#### Prerequisites on Windows 11:
- **Docker Desktop for Windows** with WSL2 backend enabled. Verify Buildx is active: `docker buildx version`.
- **Java 25 JDK** installed and active in your path: `java -version`.
- **AWS CLI v2** configured with credentials for your AWS account: `aws sts get-caller-identity`.

#### Step 1: Package Applications via Maven Wrapper (`.\mvnw.cmd`)
From the repository root on Windows 11 PowerShell:

```powershell
# Build Live Trading application (includes common, connector, trading, live)
.\mvnw.cmd clean package -DskipTests -pl connector,trading,live

# Build Data Collector application (includes connector, data)
.\mvnw.cmd clean package -DskipTests -pl connector,data
```

#### Step 2: Authenticate Docker with Amazon ECR
In PowerShell, authenticate your local Docker daemon against the ECR registry in `eu-west-1`:

```powershell
$AWS_REGION = "eu-west-1"
$AWS_ACCOUNT_ID = (aws sts get-caller-identity --query "Account" --output text).Trim()
$ECR_REGISTRY = "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY
```

#### Step 3: Cross-Compile (`linux/arm64`) & Push to Amazon ECR

Build and push the multi-architecture ARM64 images directly to the ECR repositories created by CloudFormation:

```powershell
# 1. Build and push Live Trading image (note build context is the 'live' directory)
docker buildx build --platform linux/arm64 `
  -t "$ECR_REGISTRY/polymarket-bot-live-repo:latest" `
  -f live/src/main/docker/Dockerfile.jvm `
  --push live

# 2. Build and push Data Collector image (note build context is the root directory)
docker buildx build --platform linux/arm64 `
  -t "$ECR_REGISTRY/polymarket-bot-data-repo:latest" `
  -f data/src/main/docker/Dockerfile.jvm `
  --push .
```

#### Step 4: Deploy Java 25 Telegram Alert Lambda (Optional / Update)
If you modify or update the Telegram alert Lambda component, use the dedicated deployment script [`infra/telegram_alert/deploy-telegram-alert.ps1`](file:///c:/work/workspace-java/polymarket-bot/infra/telegram_alert/deploy-telegram-alert.ps1):

```powershell
.\infra\telegram_alert\deploy-telegram-alert.ps1 -Region eu-west-1 -StackName "polymarket-bot-live"
```

---

### 3.2 Linux / macOS Host (Alternative Workflow)

For developers building and deploying from a Linux or macOS machine, the equivalent Bash commands are:

#### Step 1: Package Applications via Maven Wrapper (`./mvnw`)
```bash
# Build Live Trading application
./mvnw clean package -DskipTests -pl connector,trading,live

# Build Data Collector application
./mvnw clean package -DskipTests -pl connector,data
```

#### Step 2: Authenticate Docker with Amazon ECR
```bash
export AWS_REGION="eu-west-1"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"
```

#### Step 3: Cross-Compile (`linux/arm64`) & Push to Amazon ECR
```bash
# 1. Build and push Live Trading image
docker buildx build --platform linux/arm64 \
  -t "${ECR_REGISTRY}/polymarket-bot-live-repo:latest" \
  -f live/src/main/docker/Dockerfile.jvm \
  --push live

# 2. Build and push Data Collector image
docker buildx build --platform linux/arm64 \
  -t "${ECR_REGISTRY}/polymarket-bot-data-repo:latest" \
  -f data/src/main/docker/Dockerfile.jvm \
  --push .
```

---

## 4. Container Deployment & Execution on EC2 via AWS Systems Manager (SSM)

Because the EC2 instances are provisioned with **zero inbound security group rules**, do not use SSH. Instead, connect and manage containers via **AWS Systems Manager (SSM)** Session Manager or Run Command.

### 4.1 Deploy / Update Live Trading Daemon (`live`)

Connect to the Live Trading EC2 instance using SSM Session Manager:

```powershell
# Retrieve Instance ID from CloudFormation Stack Outputs
$LiveInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-live --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)

# Start interactive SSM session
aws ssm start-session --target $LiveInstanceId --region eu-west-1
```

Once inside the EC2 session (as `ssm-user` or `ec2-user`), execute the container update:

```bash
# Login to ECR on the EC2 instance
AWS_REGION=eu-west-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/polymarket-bot-live-repo:latest"

aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker pull "$IMAGE_URI"

# Execute launch script created by CloudFormation UserData
# Syntax: /usr/local/bin/start-polymarket-live.sh <image_uri> <dry_run: true|false>
sudo /usr/local/bin/start-polymarket-live.sh "$IMAGE_URI" false
```

The script automatically:
- Stops and removes any existing `polymarket-live` container.
- Configures Docker log streaming to CloudWatch Logs (`LiveTradingLogGroup`) with `awslogs-create-stream=true`.
- Bind-mounts the persistent gp3 EBS volume from `/mnt/ebs/polymarket-data`.
- Injects Quarkus production configuration (`QUARKUS_PROFILE=prod`) and AWS Secrets Manager credentials reference (`polymarket-sniper/clob-credentials`).
- Runs the container with `--restart unless-stopped`.

---

### 4.2 Deploy / Update Market Data Collector (`data`)

Connect to the Data Collector EC2 instance using SSM Session Manager:

```powershell
$DataInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-data --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)

aws ssm start-session --target $DataInstanceId --region eu-west-1
```

On the Data Collector EC2 host:

```bash
AWS_REGION=eu-west-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/polymarket-bot-data-repo:latest"

aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker pull "$IMAGE_URI"

# Stop existing collector container
docker stop polymarket-bot-data || true
docker rm polymarket-bot-data || true

# Run updated container with 24/7 restart policy and persistent EBS mount
docker run -d \
  --name polymarket-bot-data \
  --restart unless-stopped \
  -p 8084:8084 \
  --env-file /etc/polymarket-collector.env \
  -e POLYMARKET_COLLECTOR_DATA_DIR=/data/collector \
  -e JAVA_OPTS="-Xmx1g -XX:+UseZGC --enable-native-access=ALL-UNNAMED" \
  -v /mnt/ebs/polymarket-data:/data \
  "$IMAGE_URI"
```

---

## 5. Operational Verification, Observability & Data Sync

### 5.1 Local Dashboard Access via SSM Port Forwarding
Because no public HTTP ports are opened to the internet, access the web dashboard (`http://localhost:8080`) from your local development workstation using SSM port forwarding:

#### Windows 11 (PowerShell):
```powershell
$LiveInstanceId = (aws cloudformation describe-stacks --stack-name polymarket-bot-live --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)

aws ssm start-session `
  --target $LiveInstanceId `
  --region eu-west-1 `
  --document-name AWS-StartPortForwardingSession `
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

#### Linux / macOS (Bash):
```bash
LIVE_INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name polymarket-bot-live --region eu-west-1 --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)

aws ssm start-session \
  --target "${LIVE_INSTANCE_ID}" \
  --region eu-west-1 \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Open `http://localhost:8080` in your web browser to monitor live TWAP state, active strategy execution, and trade logs.

### 5.2 Verify CloudWatch Logs & Telegram Alerting
1. **Container Logs in CloudWatch**:
   Container logs are streamed in real time to `/polymarket-bot/polymarket-bot-live/live-trading`:
   ```powershell
   aws logs tail "/polymarket-bot/polymarket-bot-live/live-trading" --region eu-west-1 --follow
   ```
2. **Automated Telegram Alerts**:
   The CloudWatch Logs Subscription Filter (`TelegramAlertSubscriptionFilter`) monitors for patterns matching `?WARN ?warn ?ERROR ?error ?FATAL ?fatal ?Exception ?Error`.
   When an event matches, CloudWatch automatically invokes the Java 25 Telegram alert Lambda function, sending a formatted notification directly to your Telegram chat.

### 5.3 Automated Data Download, Verification & Remote Cleanup
To download rotated `.jsonl.zst` market stream files from the Data Collector EC2 instance to your local analytical storage, use the sync scripts:

- **Windows 11 (PowerShell)**: [`scripts/download-collector-data.ps1`](file:///c:/work/workspace-java/polymarket-bot/scripts/download-collector-data.ps1)
  ```powershell
  # Preview files for a date without downloading:
  .\scripts\download-collector-data.ps1 -Date 2026-10-15 -DryRun

  # Download, verify, and delete remote files on EC2:
  .\scripts\download-collector-data.ps1 -Date 2026-10-15

  # Download and verify while keeping remote files on EC2:
  .\scripts\download-collector-data.ps1 -Date 2026-10-15 -KeepRemoteFiles
  ```

- **Linux / macOS (Bash)**: [`scripts/download-collector-data.sh`](file:///c:/work/workspace-java/polymarket-bot/scripts/download-collector-data.sh)
  ```bash
  # Download, verify, and delete remote files on EC2:
  ./scripts/download-collector-data.sh --date 2026-10-15
  ```

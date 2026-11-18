#!/usr/bin/env bash
# ==============================================================================
# Packages the unified Quarkus Polymarket Bot fast-jar archive, uploads it to
# Amazon S3, and deploys it as a Bare-OS systemd service on AWS EC2 in eu-west-1
# with runtime-state preservation, /api/health verification, and auto-rollback.
#
# Resolves the release version {x.y.z} from:
#   ./bot/target/quarkus-app/app/bot-{x.y.z}.jar
#
# Usage:
#   ./deploy-bot.sh [-r <region>] [-b <base-stack>] [-e <ec2-stack>] [-a <artifact-bucket>] [-i <instance-id>]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REGION="eu-west-1"
BASE_STACK_NAME="polymarket-bot-base"
EC2_STACK_NAME="polymarket-bot-ec2"
ARTIFACT_BUCKET=""
INSTANCE_ID=""

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -r, --region <region>            AWS Region (default: eu-west-1)
  -b, --base-stack <name>          Base Infrastructure Stack Name (default: polymarket-bot-base)
  -e, --ec2-stack <name>           EC2 Compute Stack Name (default: polymarket-bot-ec2)
  -a, --artifact-bucket <bucket>   Amazon S3 artifact bucket name override
  -i, --instance-id <id>           EC2 Instance ID override
  -h, --help                       Show this help message
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -b|--base-stack)
            BASE_STACK_NAME="$2"
            shift 2
            ;;
        -e|--ec2-stack)
            EC2_STACK_NAME="$2"
            shift 2
            ;;
        -a|--artifact-bucket)
            ARTIFACT_BUCKET="$2"
            shift 2
            ;;
        -i|--instance-id)
            INSTANCE_ID="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

echo "=========================================================="
echo " Polymarket Bot: Package & Deploy Unified Bot (Bare-OS)   "
echo " Region: ${REGION} | Base: ${BASE_STACK_NAME} | EC2: ${EC2_STACK_NAME}"
echo "=========================================================="

# 1. Determine version from ./bot/target/quarkus-app/app/bot-{x.y.z}.jar
echo "[1/4] Resolving version from ./bot/target/quarkus-app/app/bot-{x.y.z}.jar..."
QUARKUS_APP_DIR="${REPO_ROOT}/bot/target/quarkus-app"
APP_DIR="${QUARKUS_APP_DIR}/app"
if [ ! -d "$APP_DIR" ]; then
    echo "Error: Directory '${APP_DIR}' not found. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi

shopt -s nullglob
JAR_FILES=("${APP_DIR}"/bot-*.jar)
shopt -u nullglob

if [ "${#JAR_FILES[@]}" -eq 0 ]; then
    echo "Error: No 'bot-*.jar' found in '${APP_DIR}'. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi
if [ "${#JAR_FILES[@]}" -gt 1 ]; then
    echo "Error: Multiple 'bot-*.jar' files found in '${APP_DIR}'. Expected a single release JAR." >&2
    exit 1
fi

JAR_NAME="$(basename "${JAR_FILES[0]}")"
if ! [[ "$JAR_NAME" =~ ^bot-([0-9]+\.[0-9]+\.[0-9]+)\.jar$ ]]; then
    echo "Error: JAR file '${JAR_NAME}' does not match expected release format 'bot-{x.y.z}.jar' (e.g. bot-1.4.0.jar). Run './mvnw release:clean release:prepare' first." >&2
    exit 1
fi
RELEASE_VERSION="${BASH_REMATCH[1]}"
echo "      Detected artifact: ${JAR_NAME} (Version: ${RELEASE_VERSION})"

GENERATED_BYTECODE_JAR="${QUARKUS_APP_DIR}/quarkus/generated-bytecode.jar"
if [ ! -f "$GENERATED_BYTECODE_JAR" ]; then
    echo "Error: Missing '${GENERATED_BYTECODE_JAR}'. Run './mvnw package -pl bot -am -DskipTests' before deploying." >&2
    exit 1
fi
GENERATED_ENTRIES="$(jar tf "$GENERATED_BYTECODE_JAR")"
for MARKER in \
    "StrategyTradingEngine_Bean.class" \
    "DataCollectorService_Bean.class" \
    "RuntimeCoordinator_Bean.class" \
    "RuntimeCoordinator_Observer_onStart" \
    "DashboardResource_Bean.class"; do
    if ! grep -q "$MARKER" <<< "$GENERATED_ENTRIES"; then
        echo "Error: Verification failed: '${GENERATED_BYTECODE_JAR}' is missing CDI bean entry matching '${MARKER}'. Run './mvnw package -pl bot -am -DskipTests' and re-run deploy-bot.sh." >&2
        exit 1
    fi
done
echo "      Verified Quarkus CDI bytecode in generated-bytecode.jar"

# 2. Resolve S3 Artifact Bucket Name
if [ -z "$ARTIFACT_BUCKET" ]; then
    echo "[2/4] Resolving S3 Artifact Bucket name from stack '${BASE_STACK_NAME}'..."
    ARTIFACT_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name "${BASE_STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='ArtifactBucketName'].OutputValue" \
        --output text 2>/dev/null || true)

    if [ -z "$ARTIFACT_BUCKET" ] || [ "$ARTIFACT_BUCKET" = "None" ]; then
        ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        ARTIFACT_BUCKET="${BASE_STACK_NAME}-artifacts-${ACCOUNT_ID}-${REGION}"
    fi
fi
echo "      S3 Artifact Bucket: ${ARTIFACT_BUCKET}"

# 3. Package ./bot/target/quarkus-app into bot-{x.y.z}.tar.gz and upload to S3
echo "[3/4] Packaging quarkus-app and uploading to S3..."
ARCHIVE_PATH="${REPO_ROOT}/bot/target/bot-${RELEASE_VERSION}.tar.gz"
rm -f "$ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$QUARKUS_APP_DIR" .

S3_ARTIFACT_URI="s3://${ARTIFACT_BUCKET}/releases/bot-${RELEASE_VERSION}.tar.gz"
aws s3 cp "$ARCHIVE_PATH" "$S3_ARTIFACT_URI" --region "${REGION}" --only-show-errors
echo "SUCCESS: Uploaded release archive to ${S3_ARTIFACT_URI}."

# 4. Deploy Bare-OS systemd Service on EC2 via AWS Systems Manager (SSM)
echo "[4/4] Deploying systemd service on EC2 instance via AWS Systems Manager..."
if [ -z "$INSTANCE_ID" ]; then
    INSTANCE_ID=$(aws cloudformation describe-stacks \
        --stack-name "${EC2_STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" \
        --output text 2>/dev/null || true)
fi

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
    echo "WARNING: EC2 Instance ID could not be resolved from stack '${EC2_STACK_NAME}'." >&2
    echo "Please provision the compute stack using 'infra/polymarket-bot-ec2.yaml' or specify -i <instance-id>." >&2
    exit 0
fi

echo "      Target EC2 Instance ID: ${INSTANCE_ID}"

TEMP_PARAMS=$(mktemp)
cat << EOF > "${TEMP_PARAMS}"
{
  "commands": [
    "set -euxo pipefail",
    "export AWS_REGION=${REGION}",
    "sudo /usr/local/bin/deploy-polymarket-bot.sh \"${S3_ARTIFACT_URI}\"",
    "sudo systemctl status polymarket-bot --no-pager",
    "curl -fsS http://127.0.0.1:443/api/health"
  ]
}
EOF

SSM_COMMAND_ID=$(aws ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name "AWS-RunShellScript" \
    --comment "Deploy Polymarket Bot ${RELEASE_VERSION} (Bare-OS systemd + auto-rollback)" \
    --parameters "file://${TEMP_PARAMS}" \
    --region "${REGION}" \
    --query "Command.CommandId" \
    --output text)
rm -f "${TEMP_PARAMS}"

echo "      Dispatched SSM Command ID: ${SSM_COMMAND_ID}. Waiting for execution & health check..."

aws ssm wait command-executed \
    --command-id "${SSM_COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" || true

OUTPUT=$(aws ssm get-command-invocation \
    --command-id "${SSM_COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query "StandardOutputContent" \
    --output text)

STATUS=$(aws ssm get-command-invocation \
    --command-id "${SSM_COMMAND_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --region "${REGION}" \
    --query "Status" \
    --output text)

echo "---------------- SSM Execution Output ----------------"
echo "$OUTPUT"
echo "------------------------------------------------------"

if [ "$STATUS" = "Success" ]; then
    echo "SUCCESS: Polymarket Bot ${RELEASE_VERSION} deployed, verified healthy, and running on ${INSTANCE_ID}!"
else
    ERROR_OUTPUT=$(aws ssm get-command-invocation \
        --command-id "${SSM_COMMAND_ID}" \
        --instance-id "${INSTANCE_ID}" \
        --region "${REGION}" \
        --query "StandardErrorContent" \
        --output text)
    echo "ERROR: Deployment failed with status '${STATUS}' (automatic rollback executed if previous release existed): ${ERROR_OUTPUT}" >&2
    exit 1
fi

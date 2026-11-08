#!/usr/bin/env bash
# ==============================================================================
# Builds the Docker image, pushes it to Amazon ECR, and deploys the Quarkus Live
# Trading application to AWS EC2 in eu-west-1.
#
# Resolves the Docker image version {x.y.z} from:
#   ./live/target/quarkus-app/app/live-{x.y.z}.jar
#
# Usage:
#   ./deploy-live.sh [-r <region>] [-b <base-stack>] [-e <ec2-stack>] [-u <repository-uri>] [-i <instance-id>]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REGION="eu-west-1"
BASE_STACK_NAME="polymarket-bot-live-base"
EC2_STACK_NAME="polymarket-bot-live-ec2"
REPOSITORY_URI=""
INSTANCE_ID=""

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -r, --region <region>       AWS Region (default: eu-west-1)
  -b, --base-stack <name>     Base Infrastructure Stack Name (default: polymarket-bot-live-base)
  -e, --ec2-stack <name>      EC2 Compute Stack Name (default: polymarket-bot-live-ec2)
  -u, --repository-uri <uri>  Amazon ECR repository URI override
  -i, --instance-id <id>      EC2 Instance ID override
  -h, --help                  Show this help message
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
        -u|--repository-uri)
            REPOSITORY_URI="$2"
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
echo " Polymarket Bot: Build Image & Deploy Live Trading App    "
echo " Region: ${REGION} | Base: ${BASE_STACK_NAME} | EC2: ${EC2_STACK_NAME}"
echo "=========================================================="

# 1. Determine version from ./live/target/quarkus-app/app/live-{x.y.z}.jar
echo "[1/5] Resolving version from ./live/target/quarkus-app/app/live-{x.y.z}.jar..."
APP_DIR="${REPO_ROOT}/live/target/quarkus-app/app"
if [ ! -d "$APP_DIR" ]; then
    echo "Error: Directory '${APP_DIR}' not found. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi

shopt -s nullglob
JAR_FILES=("${APP_DIR}"/live-*.jar)
shopt -u nullglob

if [ "${#JAR_FILES[@]}" -eq 0 ]; then
    echo "Error: No 'live-*.jar' found in '${APP_DIR}'. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi
if [ "${#JAR_FILES[@]}" -gt 1 ]; then
    echo "Error: Multiple 'live-*.jar' files found in '${APP_DIR}'. Expected a single release JAR." >&2
    exit 1
fi

JAR_NAME="$(basename "${JAR_FILES[0]}")"
if ! [[ "$JAR_NAME" =~ ^live-([0-9]+\.[0-9]+\.[0-9]+)\.jar$ ]]; then
    echo "Error: JAR file '${JAR_NAME}' does not match expected release format 'live-{x.y.z}.jar' (e.g. live-1.0.13.jar). Run './mvnw release:clean release:prepare' first." >&2
    exit 1
fi
RELEASE_VERSION="${BASH_REMATCH[1]}"
echo "      Detected artifact: ${JAR_NAME} (Version: ${RELEASE_VERSION})"

# 2. Resolve Amazon ECR Repository URI
if [ -z "$REPOSITORY_URI" ]; then
    echo "[2/5] Resolving Amazon ECR repository URI..."
    REPOSITORY_URI=$(aws cloudformation describe-stacks \
        --stack-name "${BASE_STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" \
        --output text 2>/dev/null || true)

    if [ -z "$REPOSITORY_URI" ] || [ "$REPOSITORY_URI" = "None" ]; then
        ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        REPOSITORY_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${BASE_STACK_NAME}-repo"
    fi
fi
echo "      ECR Repository URI: ${REPOSITORY_URI}"

# 3. Authenticate Docker with Amazon ECR
echo "[3/5] Authenticating Docker with Amazon ECR in ${REGION}..."
ECR_DOMAIN="${REPOSITORY_URI%%/*}"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_DOMAIN}"

# 4. Build and Push Multi-Platform ARM64 Docker Images (Docker Buildx)
echo "[4/5] Building and pushing linux/arm64 Docker image via Buildx..."
VERSIONED_URI="${REPOSITORY_URI}:${RELEASE_VERSION}"
LATEST_URI="${REPOSITORY_URI}:latest"

pushd "$REPO_ROOT" > /dev/null
docker buildx build --platform linux/arm64 --provenance=false \
    -t "${VERSIONED_URI}" \
    -t "${LATEST_URI}" \
    -f live/src/main/docker/Dockerfile.jvm \
    --push live
popd > /dev/null

echo "SUCCESS: Pushed ${VERSIONED_URI} and ${LATEST_URI} to Amazon ECR."

# 5. Deploy Container on EC2 via AWS Systems Manager (SSM)
echo "[5/5] Deploying container on EC2 instance via AWS Systems Manager..."
if [ -z "$INSTANCE_ID" ]; then
    INSTANCE_ID=$(aws cloudformation describe-stacks \
        --stack-name "${EC2_STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" \
        --output text 2>/dev/null || true)
fi

if [ -z "$INSTANCE_ID" ] || [ "$INSTANCE_ID" = "None" ]; then
    echo "WARNING: EC2 Instance ID could not be resolved from stack '${EC2_STACK_NAME}'." >&2
    echo "Please provision the compute stack using 'infra/live-trading-ec2.yaml' or specify -i <instance-id>." >&2
    exit 0
fi

echo "      Target EC2 Instance ID: ${INSTANCE_ID}"

TEMP_PARAMS=$(mktemp)
cat << EOF > "${TEMP_PARAMS}"
{
  "commands": [
    "set -euxo pipefail",
    "export AWS_REGION=${REGION}",
    "ACCOUNT_ID=\$(aws sts get-caller-identity --query Account --output text)",
    "aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin \"\${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com\"",
    "docker pull ${VERSIONED_URI}",
    "sudo /usr/local/bin/start-polymarket-live.sh \"${VERSIONED_URI}\"",
    "sleep 3",
    "docker ps --filter name=polymarket-live"
  ]
}
EOF

SSM_COMMAND_ID=$(aws ssm send-command \
    --instance-ids "${INSTANCE_ID}" \
    --document-name "AWS-RunShellScript" \
    --comment "Deploy Polymarket Live Trading ${RELEASE_VERSION}" \
    --parameters "file://${TEMP_PARAMS}" \
    --region "${REGION}" \
    --query "Command.CommandId" \
    --output text)
rm -f "${TEMP_PARAMS}"

echo "      Dispatched SSM Command ID: ${SSM_COMMAND_ID}. Waiting for execution..."

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
    echo "SUCCESS: Live Trading application ${RELEASE_VERSION} deployed and running on ${INSTANCE_ID}!"
else
    ERROR_OUTPUT=$(aws ssm get-command-invocation \
        --command-id "${SSM_COMMAND_ID}" \
        --instance-id "${INSTANCE_ID}" \
        --region "${REGION}" \
        --query "StandardErrorContent" \
        --output text)
    echo "ERROR: Deployment failed with status '${STATUS}': ${ERROR_OUTPUT}" >&2
    exit 1
fi

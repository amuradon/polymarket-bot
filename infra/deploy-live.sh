#!/usr/bin/env bash
# ==============================================================================
# Builds, releases, packages, and deploys the Quarkus Live Trading application to AWS EC2 in eu-west-1.
# Usage:
#   ./deploy-live.sh [-v <version>] [-r <region>] [-b <base-stack>] [-e <ec2-stack>] [--skip-release] [--skip-build]
# Example:
#   ./deploy-live.sh -v 1.1.0
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REGION="eu-west-1"
BASE_STACK_NAME="polymarket-bot-live-base"
EC2_STACK_NAME="polymarket-bot-live-ec2"
REPOSITORY_URI=""
INSTANCE_ID=""
VERSION=""
SKIP_RELEASE=false
SKIP_BUILD=false

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -v, --version <semver>      Optional SemVer version for Maven release (format: x.y.z)
  -r, --region <region>       AWS Region (default: eu-west-1)
  -b, --base-stack <name>     Base Infrastructure Stack Name (default: polymarket-bot-live-base)
  -e, --ec2-stack <name>      EC2 Compute Stack Name (default: polymarket-bot-live-ec2)
  -u, --repository-uri <uri>  Amazon ECR repository URI override
  -i, --instance-id <id>      EC2 Instance ID override
      --skip-release          Skip Maven release execution
      --skip-build            Skip Maven packaging
  -h, --help                  Show this help message
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
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
        --skip-release)
            SKIP_RELEASE=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
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
echo " Polymarket Bot: Release, Build & Deploy Live Trading App "
echo " Region: ${REGION} | Base: ${BASE_STACK_NAME} | EC2: ${EC2_STACK_NAME}"
echo "=========================================================="

# 1. Determine SemVer & Execute Maven Release
RELEASE_VERSION=""
if [ "$SKIP_RELEASE" = false ]; then
    echo "[1/6] Executing Maven release on project root..."
    pushd "$REPO_ROOT" > /dev/null
    if [ -n "$VERSION" ]; then
        if ! [[ "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
            echo "Error: Version must be in format x.y.z (SemVer), e.g. 1.1.0" >&2
            exit 1
        fi
        MAJOR="${BASH_REMATCH[1]}"
        MINOR="${BASH_REMATCH[2]}"
        PATCH="${BASH_REMATCH[3]}"
        NEXT_PATCH=$((PATCH + 1))
        DEV_VERSION="${MAJOR}.${MINOR}.${NEXT_PATCH}-SNAPSHOT"
        RELEASE_VERSION="$VERSION"

        echo "      Release version:     ${RELEASE_VERSION}"
        echo "      Development version: ${DEV_VERSION}"

        ./mvnw -B release:clean release:prepare \
            "-DreleaseVersion=${RELEASE_VERSION}" \
            "-DdevelopmentVersion=${DEV_VERSION}" \
            "-Dtag=${RELEASE_VERSION}"
    else
        RAW_VER=$(grep -m1 '<version>' pom.xml | sed -E 's/.*<version>(.*)<\/version>.*/\1/')
        RELEASE_VERSION="${RAW_VER%-SNAPSHOT}"
        echo "      Auto-incrementing from current POM version: ${RAW_VER} (Release: ${RELEASE_VERSION})"

        ./mvnw -B release:clean release:prepare
    fi
    popd > /dev/null
else
    echo "[1/6] Skipping Maven release as requested..."
    if [ -n "$VERSION" ]; then
        RELEASE_VERSION="$VERSION"
    else
        RAW_VER=$(grep -m1 '<version>' "${REPO_ROOT}/pom.xml" | sed -E 's/.*<version>(.*)<\/version>.*/\1/')
        RELEASE_VERSION="${RAW_VER%-SNAPSHOT}"
    fi
    echo "      Target image version tag: ${RELEASE_VERSION}"
fi

# 2. Package Quarkus application via Maven wrapper
if [ "$SKIP_BUILD" = false ]; then
    echo "[2/6] Packaging Quarkus application via Maven wrapper..."
    pushd "$REPO_ROOT" > /dev/null
    ./mvnw clean package -DskipTests -pl live -am
    popd > /dev/null
else
    echo "[2/6] Skipping Maven package as requested..."
fi

# 3. Resolve Amazon ECR Repository URI
if [ -z "$REPOSITORY_URI" ]; then
    echo "[3/6] Resolving Amazon ECR repository URI..."
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

# 4. Authenticate Docker with Amazon ECR
echo "[4/6] Authenticating Docker with Amazon ECR in ${REGION}..."
ECR_DOMAIN="${REPOSITORY_URI%%/*}"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_DOMAIN}"

# 5. Build and Push Multi-Platform ARM64 Docker Images (Docker Buildx)
echo "[5/6] Building and pushing linux/arm64 Docker image via Buildx..."
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

# 6. Deploy Container on EC2 via AWS Systems Manager (SSM)
echo "[6/6] Deploying container on EC2 instance via AWS Systems Manager..."
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

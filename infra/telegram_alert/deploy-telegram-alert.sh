#!/usr/bin/env bash
# ==============================================================================
# Builds, releases, packages, and uploads the Java 25 Telegram Alert Lambda container image to Amazon ECR.
# Usage:
#   ./deploy-telegram-alert.sh [-v <version>] [-r <region>] [-s <stack-name>] [--skip-release] [--skip-build]
# Example:
#   ./deploy-telegram-alert.sh -v 1.0.1
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="eu-west-1"
BASE_STACK_NAME="polymarket-bot-alert-base"
FUNCTION_NAME="polymarket-bot-alert-lambda-function"
REPOSITORY_URI=""
VERSION=""
SKIP_RELEASE=false
SKIP_BUILD=false

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -v, --version <semver>      Optional SemVer version for Maven release (format: x.y.z)
  -r, --region <region>       AWS Region (default: eu-west-1)
  -s, --stack <stack-name>    Telegram Alert Base Stack Name (default: polymarket-bot-alert-base)
  -u, --repository-uri <uri>  Amazon ECR repository URI override
  -f, --function-name <name>  Lambda function name to update in-place (default: polymarket-bot-alert-lambda-function)
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
        -s|--stack)
            BASE_STACK_NAME="$2"
            shift 2
            ;;
        -u|--repository-uri)
            REPOSITORY_URI="$2"
            shift 2
            ;;
        -f|--function-name)
            FUNCTION_NAME="$2"
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
echo " Polymarket Bot: Release & Upload Alert Lambda Container  "
echo " Region: ${REGION} | Base Stack: ${BASE_STACK_NAME}"
echo "=========================================================="

# 1. Determine SemVer & Execute Maven Release
RELEASE_VERSION=""
if [ "$SKIP_RELEASE" = false ]; then
    echo "[1/5] Executing Maven release..."
    pushd "$SCRIPT_DIR" > /dev/null
    if [ -n "$VERSION" ]; then
        if ! [[ "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
            echo "Error: Version must be in format x.y.z (SemVer), e.g. 1.0.1" >&2
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

        mvn -B release:clean release:prepare \
            "-DreleaseVersion=${RELEASE_VERSION}" \
            "-DdevelopmentVersion=${DEV_VERSION}" \
            "-Dtag=telegram-alert-${RELEASE_VERSION}"
    else
        RAW_VER=$(grep -m1 '<version>' pom.xml | sed -E 's/.*<version>(.*)<\/version>.*/\1/')
        RELEASE_VERSION="${RAW_VER%-SNAPSHOT}"
        echo "      Auto-incrementing from current POM version: ${RAW_VER} (Release: ${RELEASE_VERSION})"

        mvn -B release:clean release:prepare
    fi
    popd > /dev/null
else
    echo "[1/5] Skipping Maven release as requested..."
    if [ -n "$VERSION" ]; then
        RELEASE_VERSION="$VERSION"
    else
        RAW_VER=$(grep -m1 '<version>' "${SCRIPT_DIR}/pom.xml" | sed -E 's/.*<version>(.*)<\/version>.*/\1/')
        RELEASE_VERSION="${RAW_VER%-SNAPSHOT}"
    fi
    echo "      Target image version tag: ${RELEASE_VERSION}"
fi

# 2. Package shaded jar via Maven
if [ "$SKIP_BUILD" = false ]; then
    echo "[2/5] Packaging shaded uber-jar via Maven..."
    pushd "$SCRIPT_DIR" > /dev/null
    mvn clean package -DskipTests
    popd > /dev/null
else
    echo "[2/5] Skipping Maven package as requested..."
fi

# 3. Resolve Amazon ECR Repository URI
if [ -z "$REPOSITORY_URI" ]; then
    echo "[3/5] Resolving Amazon ECR repository URI..."
    REPOSITORY_URI=$(aws cloudformation describe-stacks \
        --stack-name "${BASE_STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='TelegramAlertEcrRepositoryUri'].OutputValue" \
        --output text 2>/dev/null || true)

    if [ -z "$REPOSITORY_URI" ] || [ "$REPOSITORY_URI" = "None" ]; then
        ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        REPOSITORY_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${BASE_STACK_NAME}-telegram-alert-repo"
    fi
fi
echo "      ECR Repository URI: ${REPOSITORY_URI}"

# 4. Authenticate Docker with Amazon ECR
echo "[4/5] Authenticating Docker with Amazon ECR in ${REGION}..."
ECR_DOMAIN="${REPOSITORY_URI%%/*}"
aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_DOMAIN}"

# 5. Build and Push Docker Images
echo "[5/5] Building and pushing Docker container image..."
VERSIONED_URI="${REPOSITORY_URI}:${RELEASE_VERSION}"
LATEST_URI="${REPOSITORY_URI}:latest"

docker build -t "${VERSIONED_URI}" -t "${LATEST_URI}" "${SCRIPT_DIR}"
docker push "${VERSIONED_URI}"
docker push "${LATEST_URI}"

echo "SUCCESS: Pushed ${VERSIONED_URI} and ${LATEST_URI} to Amazon ECR."

# 6. Update existing Lambda function if present
if aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" > /dev/null 2>&1; then
    echo "Updating deployed Lambda function '${FUNCTION_NAME}' to image ${VERSIONED_URI}..."
    aws lambda update-function-code \
        --function-name "${FUNCTION_NAME}" \
        --image-uri "${VERSIONED_URI}" \
        --region "${REGION}" \
        --no-cli-pager > /dev/null

    aws lambda wait function-updated \
        --function-name "${FUNCTION_NAME}" \
        --region "${REGION}"

    echo "SUCCESS: Lambda function '${FUNCTION_NAME}' updated to ${VERSIONED_URI}"
else
    echo "NOTE: Function '${FUNCTION_NAME}' not yet created. Deploy CFN stack 'infra/telegram-alert-lambda.yaml' with ImageTag=${RELEASE_VERSION}."
fi

#!/usr/bin/env bash
# ==============================================================================
# Packages the Quarkus Data Collector fast-jar archive, uploads it to Amazon S3,
# and deploys it as a Bare-OS systemd service on AWS EC2 in eu-west-1 (with
# automated ~6-8s EBS hot-swap support when migrating instances).
#
# Resolves the release version {x.y.z} from:
#   ./data/target/quarkus-app/app/data-{x.y.z}.jar
#
# Usage:
#   ./deploy-data.sh [-r <region>] [-b <base-stack>] [-e <ec2-stack>] [-a <artifact-bucket>] [-i <instance-id>]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REGION="eu-west-1"
BASE_STACK_NAME="polymarket-bot-data-base"
EC2_STACK_NAME="polymarket-bot-data-ec2"
ARTIFACT_BUCKET=""
INSTANCE_ID=""

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -r, --region <region>            AWS Region (default: eu-west-1)
  -b, --base-stack <name>          Base Infrastructure Stack Name (default: polymarket-bot-data-base)
  -e, --ec2-stack <name>           EC2 Compute Stack Name (default: polymarket-bot-data-ec2)
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

run_ssm_commands() {
    local target_instance="$1"
    local comment="$2"
    shift 2
    local temp_params
    temp_params=$(mktemp)

    python3 - "$temp_params" "$@" << 'PYEOF'
import json, sys
out_path = sys.argv[1]
cmds = sys.argv[2:]
with open(out_path, "w", encoding="utf-8") as f:
    json.dump({"commands": cmds}, f)
PYEOF

    local cmd_id
    cmd_id=$(aws ssm send-command \
        --instance-ids "${target_instance}" \
        --document-name "AWS-RunShellScript" \
        --comment "${comment}" \
        --parameters "file://${temp_params}" \
        --region "${REGION}" \
        --query "Command.CommandId" \
        --output text)
    rm -f "${temp_params}"

    aws ssm wait command-executed \
        --command-id "${cmd_id}" \
        --instance-id "${target_instance}" \
        --region "${REGION}" || true

    SSM_LAST_STATUS=$(aws ssm get-command-invocation \
        --command-id "${cmd_id}" \
        --instance-id "${target_instance}" \
        --region "${REGION}" \
        --query "Status" \
        --output text)

    SSM_LAST_STDOUT=$(aws ssm get-command-invocation \
        --command-id "${cmd_id}" \
        --instance-id "${target_instance}" \
        --region "${REGION}" \
        --query "StandardOutputContent" \
        --output text)

    SSM_LAST_STDERR=$(aws ssm get-command-invocation \
        --command-id "${cmd_id}" \
        --instance-id "${target_instance}" \
        --region "${REGION}" \
        --query "StandardErrorContent" \
        --output text)
}

echo "=========================================================="
echo " Polymarket Bot: Package & Deploy Data Collector (Bare-OS)"
echo " Region: ${REGION} | Base: ${BASE_STACK_NAME} | EC2: ${EC2_STACK_NAME}"
echo "=========================================================="

# 1. Determine version from ./data/target/quarkus-app/app/data-{x.y.z}.jar
echo "[1/4] Resolving version from ./data/target/quarkus-app/app/data-{x.y.z}.jar..."
QUARKUS_APP_DIR="${REPO_ROOT}/data/target/quarkus-app"
APP_DIR="${QUARKUS_APP_DIR}/app"
if [ ! -d "$APP_DIR" ]; then
    echo "Error: Directory '${APP_DIR}' not found. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi

shopt -s nullglob
JAR_FILES=("${APP_DIR}"/data-*.jar)
shopt -u nullglob

if [ "${#JAR_FILES[@]}" -eq 0 ]; then
    echo "Error: No 'data-*.jar' found in '${APP_DIR}'. Run './mvnw release:clean release:prepare' before deploying." >&2
    exit 1
fi
if [ "${#JAR_FILES[@]}" -gt 1 ]; then
    echo "Error: Multiple 'data-*.jar' files found in '${APP_DIR}'. Expected a single release JAR." >&2
    exit 1
fi

JAR_NAME="$(basename "${JAR_FILES[0]}")"
if ! [[ "$JAR_NAME" =~ ^data-([0-9]+\.[0-9]+\.[0-9]+)\.jar$ ]]; then
    echo "Error: JAR file '${JAR_NAME}' does not match expected release format 'data-{x.y.z}.jar' (e.g. data-1.2.1.jar). Run './mvnw release:clean release:prepare' first." >&2
    exit 1
fi
RELEASE_VERSION="${BASH_REMATCH[1]}"
echo "      Detected artifact: ${JAR_NAME} (Version: ${RELEASE_VERSION})"

# 2. Resolve S3 Artifact Bucket Name and EBS Volume ID
echo "[2/4] Resolving S3 Artifact Bucket and EBS Volume ID from stack '${BASE_STACK_NAME}'..."
if [ -z "$ARTIFACT_BUCKET" ]; then
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

EBS_VOLUME_ID=$(aws cloudformation describe-stacks \
    --stack-name "${BASE_STACK_NAME}" \
    --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='EbsVolumeId'].OutputValue" \
    --output text 2>/dev/null || true)

echo "      S3 Artifact Bucket: ${ARTIFACT_BUCKET}"
if [ -n "$EBS_VOLUME_ID" ] && [ "$EBS_VOLUME_ID" != "None" ]; then
    echo "      Persistent EBS Volume ID: ${EBS_VOLUME_ID}"
fi

# 3. Package ./data/target/quarkus-app into data-{x.y.z}.tar.gz and upload to S3
echo "[3/4] Packaging quarkus-app and uploading to S3..."
ARCHIVE_PATH="${REPO_ROOT}/data/target/data-${RELEASE_VERSION}.tar.gz"
rm -f "$ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$QUARKUS_APP_DIR" .

S3_ARTIFACT_URI="s3://${ARTIFACT_BUCKET}/releases/data-${RELEASE_VERSION}.tar.gz"
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
    echo "Please provision the compute stack using 'infra/data-collector-ec2.yaml' or specify -i <instance-id>." >&2
    exit 0
fi

echo "      Target EC2 Instance ID: ${INSTANCE_ID}"

ATTACHED_INSTANCE_ID=""
if [ -n "$EBS_VOLUME_ID" ] && [ "$EBS_VOLUME_ID" != "None" ]; then
    ATTACHED_INSTANCE_ID=$(aws ec2 describe-volumes \
        --region "${REGION}" \
        --volume-ids "${EBS_VOLUME_ID}" \
        --query "Volumes[0].Attachments[0].InstanceId" \
        --output text 2>/dev/null || true)
fi

if [ -n "$ATTACHED_INSTANCE_ID" ] && [ "$ATTACHED_INSTANCE_ID" != "None" ] && [ "$ATTACHED_INSTANCE_ID" != "$INSTANCE_ID" ]; then
    echo "      Detected EBS Volume ${EBS_VOLUME_ID} currently attached to previous instance ${ATTACHED_INSTANCE_ID}."
    echo "      [4a] Pre-staging release ${RELEASE_VERSION} on new instance ${INSTANCE_ID} (zero downtime on ${ATTACHED_INSTANCE_ID})..."

    run_ssm_commands "${INSTANCE_ID}" "Pre-stage Data Collector ${RELEASE_VERSION}" \
        "set -euxo pipefail" \
        "export AWS_REGION=${REGION}" \
        "sudo /usr/local/bin/deploy-polymarket-data.sh \"${S3_ARTIFACT_URI}\" --stage-only"

    if [ "$SSM_LAST_STATUS" != "Success" ]; then
        echo "ERROR: Failed to pre-stage release on ${INSTANCE_ID}: ${SSM_LAST_STDERR}" >&2
        exit 1
    fi

    echo "      [4b] Triggering online EBS Snapshot backup of ${EBS_VOLUME_ID}..."
    SNAPSHOT_ID=$(aws ec2 create-snapshot \
        --region "${REGION}" \
        --volume-id "${EBS_VOLUME_ID}" \
        --description "Pre-migration backup of ${EBS_VOLUME_ID} before hot-swap to ${INSTANCE_ID} (v${RELEASE_VERSION})" \
        --query "SnapshotId" \
        --output text)
    echo "           Created online backup Snapshot: ${SNAPSHOT_ID}"

    echo "      [4c] Executing fast EBS Hot-Swap from ${ATTACHED_INSTANCE_ID} to ${INSTANCE_ID}..."
    SWAP_START=$(date +%s)

    run_ssm_commands "${ATTACHED_INSTANCE_ID}" "Stop collector and unmount EBS for hot-swap" \
        "set -euxo pipefail" \
        "if command -v docker >/dev/null 2>&1; then sudo docker stop -t 5 polymarket-bot-data 2>/dev/null || true; sudo docker rm -f polymarket-bot-data 2>/dev/null || true; fi" \
        "sudo systemctl stop polymarket-data 2>/dev/null || true" \
        "sync" \
        "sudo umount /mnt/ebs/polymarket-data 2>/dev/null || sudo umount -l /mnt/ebs/polymarket-data 2>/dev/null || true"

    aws ec2 detach-volume --region "${REGION}" --volume-id "${EBS_VOLUME_ID}" --instance-id "${ATTACHED_INSTANCE_ID}" >/dev/null
    for _ in $(seq 1 60); do
        VOL_STATE=$(aws ec2 describe-volumes --region "${REGION}" --volume-ids "${EBS_VOLUME_ID}" --query "Volumes[0].State" --output text)
        if [ "$VOL_STATE" = "available" ]; then
            break
        fi
        sleep 0.5
    done

    run_ssm_commands "${INSTANCE_ID}" "Activate staged Data Collector ${RELEASE_VERSION} and mount EBS" \
        "set -euxo pipefail" \
        "export AWS_REGION=${REGION}" \
        "sudo /usr/local/bin/deploy-polymarket-data.sh \"${S3_ARTIFACT_URI}\" --activate-staged" \
        "sleep 2" \
        "sudo systemctl status polymarket-data --no-pager"

    SWAP_END=$(date +%s)
    SWAP_ELAPSED=$(( SWAP_END - SWAP_START ))

    echo "---------------- SSM Execution Output ----------------"
    echo "$SSM_LAST_STDOUT"
    echo "------------------------------------------------------"

    if [ "$SSM_LAST_STATUS" = "Success" ]; then
        echo "SUCCESS: EBS Hot-Swap completed in ${SWAP_ELAPSED}s! Data Collector ${RELEASE_VERSION} is running on ${INSTANCE_ID}."
    else
        echo "ERROR: Activation failed with status '${SSM_LAST_STATUS}': ${SSM_LAST_STDERR}" >&2
        exit 1
    fi
else
    run_ssm_commands "${INSTANCE_ID}" "Deploy Polymarket Data Collector ${RELEASE_VERSION} (Bare-OS systemd)" \
        "set -euxo pipefail" \
        "export AWS_REGION=${REGION}" \
        "if command -v docker >/dev/null 2>&1; then sudo docker rm -f polymarket-bot-data 2>/dev/null || true; sudo systemctl disable --now docker 2>/dev/null || true; fi" \
        "if command -v iptables >/dev/null 2>&1; then sudo iptables -t nat -F 2>/dev/null || true; fi" \
        "sudo /usr/local/bin/deploy-polymarket-data.sh \"${S3_ARTIFACT_URI}\"" \
        "sleep 3" \
        "sudo systemctl status polymarket-data --no-pager"

    echo "---------------- SSM Execution Output ----------------"
    echo "$SSM_LAST_STDOUT"
    echo "------------------------------------------------------"

    if [ "$SSM_LAST_STATUS" = "Success" ]; then
        echo "SUCCESS: Data Collector application ${RELEASE_VERSION} deployed and running on ${INSTANCE_ID}!"
    else
        echo "ERROR: Deployment failed with status '${SSM_LAST_STATUS}': ${SSM_LAST_STDERR}" >&2
        exit 1
    fi
fi

#!/usr/bin/env bash
#
# download-collector-data.sh
#
# Downloads daily Zstandard (.jsonl.zst) market data from the AWS EC2 Data Collector instance to local disk,
# maintaining the identical directory structure, verifies file integrity, and deletes verified files from EC2.
# Also supports cleaning up remote files that were already downloaded previously.
#

set -euo pipefail

# ANSI color codes
COLOR_RESET="\033[0m"
COLOR_CYAN="\033[1;36m"
COLOR_GREEN="\033[1;32m"
COLOR_YELLOW="\033[1;33m"
COLOR_RED="\033[1;31m"
COLOR_WHITE="\033[1;37m"
COLOR_GRAY="\033[0;90m"

log_info() {
    echo -e "${COLOR_CYAN}$*${COLOR_RESET}"
}
log_success() {
    echo -e "${COLOR_GREEN}$*${COLOR_RESET}"
}
log_warn() {
    echo -e "${COLOR_YELLOW}$*${COLOR_RESET}"
}
log_error() {
    echo -e "${COLOR_RED}$*${COLOR_RESET}"
}

usage() {
    cat <<EOF
Usage:
  $(basename "$0") -d <YYYY-MM-DD> [options]
  $(basename "$0") --clean-already-downloaded [options]

Modes:
  -d, --date <YYYY-MM-DD>       Target UTC date (e.g. 2026-10-15).
  -c, --clean-already-downloaded
                                Scan local files in destination-dir, compare against EC2 files,
                                and delete verified matching files on EC2.

Options:
  -o, --destination-dir <path>  Local root directory for collector data.
                                (Default: /data/polymarket/collector or ./collector)
  -h, --ec2-host <alias>        EC2 SSH host alias configured in ~/.ssh/config.
                                (Default: i-058e25a0b3334934d)
  -r, --remote-base-dir <path>  Root directory of collector data on EC2.
                                (Default: /mnt/ebs/polymarket-data/collector)
  -n, --dry-run                 Preview matching files and operations without downloading or deleting.
  -k, --keep-remote-files       Retain files on EC2 after successful download and verification.
      --help                    Display this help message.

Examples:
  $(basename "$0") -d 2026-10-15
  $(basename "$0") -d 2026-10-15 --dry-run
  $(basename "$0") -d 2026-10-15 -o /custom/data/path --keep-remote-files
  $(basename "$0") --clean-already-downloaded -o /data/polymarket/collector
EOF
    exit "${1:-0}"
}

# Default values
TARGET_DATE=""
CLEAN_ALREADY_DOWNLOADED=false
DESTINATION_DIR=""
EC2_HOST="i-0e826b5eb507d87be"
REMOTE_BASE_DIR="/mnt/ebs/polymarket-data/collector"
DRY_RUN=false
KEEP_REMOTE_FILES=false

# Auto-detect default destination dir if not provided
if [ -d "/data/polymarket/collector" ]; then
    DESTINATION_DIR="/data/polymarket/collector"
else
    DESTINATION_DIR="${COLLECTOR_DATA_DIR:-./collector}"
fi

# Parse CLI options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--date)
            TARGET_DATE="$2"
            shift 2
            ;;
        -c|--clean-already-downloaded)
            CLEAN_ALREADY_DOWNLOADED=true
            shift
            ;;
        -o|--destination-dir)
            DESTINATION_DIR="$2"
            shift 2
            ;;
        -h|--ec2-host)
            EC2_HOST="$2"
            shift 2
            ;;
        -r|--remote-base-dir)
            REMOTE_BASE_DIR="$2"
            shift 2
            ;;
        -n|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -k|--keep-remote-files)
            KEEP_REMOTE_FILES=true
            shift
            ;;
        --help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Ensure ssh, scp, and tar exist
for bin in ssh scp tar python3; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        log_error "Required command '$bin' is not installed or not in PATH."
        exit 1
    fi
done

# Check session manager plugin path if present
if [ -d "/usr/local/sessionmanagerplugin/bin" ] && [[ ":$PATH:" != *":/usr/local/sessionmanagerplugin/bin:"* ]]; then
    export PATH="/usr/local/sessionmanagerplugin/bin:$PATH"
fi

# ==============================================================================
# MODE: CLEAN ALREADY DOWNLOADED
# ==============================================================================
if [ "$CLEAN_ALREADY_DOWNLOADED" = true ]; then
    log_info "================================================================="
    log_info " Polymarket Data Collector - Remote Reconcile & Cleanup (Linux)"
    echo -e "${COLOR_WHITE} EC2 Instance Host : ${EC2_HOST}${COLOR_RESET}"
    echo -e "${COLOR_WHITE} Remote Base Dir   : ${REMOTE_BASE_DIR}${COLOR_RESET}"
    echo -e "${COLOR_WHITE} Local Destination : ${DESTINATION_DIR}${COLOR_RESET}"
    if [ "$DRY_RUN" = true ]; then
        log_warn " Mode              : DRY-RUN (No remote files will be deleted)"
    else
        log_success " Mode              : VERIFY AND DELETE ALREADY DOWNLOADED FILES"
    fi
    log_info "================================================================="

    if [ ! -d "$DESTINATION_DIR" ]; then
        log_error "Local destination directory '$DESTINATION_DIR' does not exist."
        exit 1
    fi

    log_info "[1/3] Scanning local files in $DESTINATION_DIR..."
    TEMP_MANIFEST=$(mktemp /tmp/reconcile_manifest_XXXXXX.tsv)
    TEMP_PY=$(mktemp /tmp/reconcile_check_XXXXXX.py)

    trap 'rm -f "$TEMP_MANIFEST" "$TEMP_PY"' EXIT

    python3 - <<EOF > "$TEMP_MANIFEST"
import os, sys

dest = os.path.abspath("$DESTINATION_DIR")
count = 0
for root, dirs, files in os.walk(dest):
    for f in files:
        if f.endswith(".jsonl.zst"):
            full = os.path.join(root, f)
            rel = os.path.relpath(full, dest).replace(os.sep, '/')
            size = os.path.getsize(full)
            sys.stdout.write(f"{rel}\t{size}\n")
            count += 1
EOF

    LOCAL_COUNT=$(wc -l < "$TEMP_MANIFEST" | tr -d ' ')
    log_success "      Found $LOCAL_COUNT local .jsonl.zst files."

    cat << 'EOF' > "$TEMP_PY"
import os, sys

remote_base = sys.argv[1]
manifest_file = "/tmp/reconcile_manifest.tsv"

if not os.path.exists(manifest_file):
    print("ERROR: Manifest missing")
    sys.exit(1)

local_manifest = {}
with open(manifest_file, "r") as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split("\t")
        if len(parts) == 2:
            local_manifest[parts[0]] = int(parts[1])

matched = []
mismatch = []
remote_total = 0

for root, dirs, files in os.walk(remote_base):
    for f in files:
        if not f.endswith(".jsonl.zst"): continue
        remote_total += 1
        full_path = os.path.join(root, f)
        rel = os.path.relpath(full_path, remote_base).replace('\\', '/')
        rem_size = os.path.getsize(full_path)
        if rel in local_manifest:
            loc_size = local_manifest[rel]
            if loc_size == rem_size:
                matched.append((rel, full_path, rem_size))
            else:
                mismatch.append((rel, loc_size, rem_size))

total_mb = sum(s for _, _, s in matched) / (1024 * 1024)
print(f"RECONCILE\t{len(matched)}\t{total_mb:.2f}\t{len(mismatch)}\t{remote_total}")

with open("/tmp/reconcile_delete.txt", "w") as out:
    for _, full_path, _ in matched:
        out.write(full_path + "\n")
EOF

    log_info "[2/3] Comparing with EC2 EBS volume..."
    scp -q -o BatchMode=yes "$TEMP_MANIFEST" "${EC2_HOST}:/tmp/reconcile_manifest.tsv"
    scp -q -o BatchMode=yes "$TEMP_PY" "${EC2_HOST}:/tmp/reconcile_check.py"
    REC_OUTPUT=$(ssh -o BatchMode=yes "$EC2_HOST" "python3 /tmp/reconcile_check.py '$REMOTE_BASE_DIR'")

    if [[ ! "$REC_OUTPUT" =~ ^RECONCILE[[:space:]]+([0-9]+)[[:space:]]+([0-9\.]+)[[:space:]]+([0-9]+)[[:space:]]+([0-9]+) ]]; then
        log_error "Reconciliation script failed. Output: $REC_OUTPUT"
        exit 1
    fi

    MATCHED_COUNT="${BASH_REMATCH[1]}"
    MATCHED_MB="${BASH_REMATCH[2]}"
    MISMATCH_COUNT="${BASH_REMATCH[3]}"
    REMOTE_TOTAL="${BASH_REMATCH[4]}"

    log_success "      Matched & Verified Files : $MATCHED_COUNT ($MATCHED_MB MB)"
    if [ "$MISMATCH_COUNT" -gt 0 ]; then
        log_error "      Size Mismatches (skipped): $MISMATCH_COUNT"
    else
        echo -e "${COLOR_GRAY}      Size Mismatches (skipped): 0${COLOR_RESET}"
    fi
    echo -e "${COLOR_GRAY}      Total Remote Files       : $REMOTE_TOTAL${COLOR_RESET}"

    if [ "$MATCHED_COUNT" -eq 0 ]; then
        log_warn "[INFO] No matching files to delete on EC2."
        ssh -o BatchMode=yes "$EC2_HOST" "rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
        exit 0
    fi

    if [ "$DRY_RUN" = true ]; then
        log_warn "[DRY-RUN] Files that would be deleted from EC2:"
        ssh -o BatchMode=yes "$EC2_HOST" "head -n 20 /tmp/reconcile_delete.txt"
        log_warn "[DRY-RUN] Completed. No files deleted."
        ssh -o BatchMode=yes "$EC2_HOST" "rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
        exit 0
    fi

    log_info "[3/3] Deleting $MATCHED_COUNT verified files on EC2..."
    ssh -o BatchMode=yes "$EC2_HOST" "cat /tmp/reconcile_delete.txt | xargs -r -d '\n' sudo rm -f && rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
    DF_OUTPUT=$(ssh -o BatchMode=yes "$EC2_HOST" "df -h '$REMOTE_BASE_DIR'")
    log_success "      Files deleted successfully. Remote EBS volume status:\n$DF_OUTPUT"
    log_success "\nSUCCESS: Reclaimed $MATCHED_MB MB across $MATCHED_COUNT files on EC2."
    exit 0
fi

# ==============================================================================
# MODE: DOWNLOAD DAILY ARCHIVE
# ==============================================================================
if [ -z "$TARGET_DATE" ]; then
    log_error "Target date (-d|--date YYYY-MM-DD) is required for download mode."
    usage
fi

if [[ ! "$TARGET_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log_error "Target date '$TARGET_DATE' must be in YYYY-MM-DD format."
    exit 1
fi

UTC_TODAY=$(date -u +"%Y-%m-%d")
if [ "$TARGET_DATE" = "$UTC_TODAY" ] && [ "$KEEP_REMOTE_FILES" = false ]; then
    log_warn "Requested date '$TARGET_DATE' matches today (UTC). The Data Collector daemon is actively writing to today's daily files."
    log_warn "Automatically enabling --keep-remote-files to protect running streams from deletion."
    KEEP_REMOTE_FILES=true
fi

log_info "================================================================="
log_info " Polymarket Data Collector - Daily Archive Downloader (Linux)"
echo -e "${COLOR_WHITE} Target Date (UTC) : ${TARGET_DATE}${COLOR_RESET}"
echo -e "${COLOR_WHITE} EC2 Instance Host : ${EC2_HOST}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Remote Base Dir   : ${REMOTE_BASE_DIR}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Local Destination : ${DESTINATION_DIR}${COLOR_RESET}"
if [ "$DRY_RUN" = true ]; then
    log_warn " Mode              : DRY-RUN (No files will be downloaded or deleted)"
elif [ "$KEEP_REMOTE_FILES" = true ]; then
    log_warn " Mode              : DOWNLOAD & KEEP REMOTE FILES"
else
    log_success " Mode              : DOWNLOAD, VERIFY & DELETE FROM EC2"
fi
log_info "================================================================="

log_info "[1/4] Scanning EC2 for files belonging to $TARGET_DATE..."

REMOTE_SCAN_CMD=$(cat << EOF
python3 - << 'PYEOF'
import os, sys, datetime

date_str = "$TARGET_DATE"
try:
    day_start = int(datetime.datetime.strptime(date_str + " 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z").timestamp())
except Exception as e:
    print(f"ERROR: Invalid date format {e}")
    sys.exit(1)
day_end = day_start + 86400

base = "$REMOTE_BASE_DIR"
if not os.path.exists(base):
    print(f"ERROR: Base directory {base} does not exist")
    sys.exit(1)

manifest_lines = []
files_lines = []

for root, dirs, files in os.walk(base):
    for f in files:
        if not f.endswith(".jsonl.zst"):
            continue
        full_path = os.path.join(root, f)
        matched = False
        if ("-" + date_str + ".jsonl.zst") in f:
            matched = True
        else:
            parts = f.replace(".jsonl.zst", "").split("-")
            if parts and parts[-1].isdigit():
                try:
                    epoch = int(parts[-1])
                    if day_start <= epoch < day_end:
                        matched = True
                except ValueError:
                    pass
        if matched:
            rel = os.path.relpath(full_path, base).replace('\\\\', '/')
            size = os.path.getsize(full_path)
            manifest_lines.append(f"{rel}\t{size}")
            files_lines.append(rel)

manifest_path = f"/tmp/collector-{date_str}.manifest"
files_path = f"/tmp/collector-{date_str}.files"

with open(manifest_path, "w") as out:
    out.write("\n".join(manifest_lines) + ("\n" if manifest_lines else ""))

with open(files_path, "w") as out:
    out.write("\n".join(files_lines) + ("\n" if files_lines else ""))

total_size = sum(int(line.split("\t")[1]) for line in manifest_lines)
print(f"FOUND\t{len(manifest_lines)}\t{total_size}")
PYEOF
EOF
)

SCAN_RESULT=$(ssh -o BatchMode=yes "$EC2_HOST" "$REMOTE_SCAN_CMD")

if [[ ! "$SCAN_RESULT" =~ ^FOUND[[:space:]]+([0-9]+)[[:space:]]+([0-9]+) ]]; then
    log_error "Failed to scan remote files on EC2 host $EC2_HOST. Output: $SCAN_RESULT"
    exit 1
fi

FILE_COUNT="${BASH_REMATCH[1]}"
TOTAL_BYTES="${BASH_REMATCH[2]}"
TOTAL_MB=$(awk -v b="$TOTAL_BYTES" 'BEGIN { printf "%.2f", b / (1024*1024) }')

if [ "$FILE_COUNT" -eq 0 ]; then
    log_warn "[INFO] No files found on EC2 for date $TARGET_DATE."
    ssh -o BatchMode=yes "$EC2_HOST" "rm -f /tmp/collector-$TARGET_DATE.manifest /tmp/collector-$TARGET_DATE.files"
    exit 0
fi

log_success "      Found $FILE_COUNT files ($TOTAL_MB MB) on EC2 for date $TARGET_DATE."

if [ "$DRY_RUN" = true ]; then
    log_warn "[DRY-RUN] Remote file manifest:"
    MANIFEST_CONTENT=$(ssh -o BatchMode=yes "$EC2_HOST" "cat /tmp/collector-$TARGET_DATE.manifest")
    while IFS=$'\t' read -r rel size; do
        if [ -n "$rel" ]; then
            size_mb=$(awk -v b="$size" 'BEGIN { printf "%.2f", b / (1024*1024) }')
            echo -e "${COLOR_GRAY}  - $rel ($size_mb MB)${COLOR_RESET}"
        fi
    done <<< "$MANIFEST_CONTENT"
    ssh -o BatchMode=yes "$EC2_HOST" "rm -f /tmp/collector-$TARGET_DATE.manifest /tmp/collector-$TARGET_DATE.files"
    log_warn "[DRY-RUN] Dry run completed. No files downloaded or deleted."
    exit 0
fi

log_info "[2/4] Preparing local directory: $DESTINATION_DIR..."
mkdir -p "$DESTINATION_DIR"

log_info "[3/4] Streaming $FILE_COUNT files ($TOTAL_MB MB) via SSH..."
START_TIME=$(date +%s)

ssh -o BatchMode=yes "$EC2_HOST" "tar -cf - -C '$REMOTE_BASE_DIR' -T '/tmp/collector-$TARGET_DATE.files'" | tar -xf - -C "$DESTINATION_DIR"

END_TIME=$(date +%s)
ELAPSED_SEC=$(( END_TIME - START_TIME ))
if [ "$ELAPSED_SEC" -le 0 ]; then
    ELAPSED_SEC=1
fi
SPEED_MB_SEC=$(awk -v mb="$TOTAL_MB" -v s="$ELAPSED_SEC" 'BEGIN { printf "%.2f", mb / s }')
log_success "      Download completed in ${ELAPSED_SEC}s (${SPEED_MB_SEC} MB/s)."

log_info "[4/4] Verifying all $FILE_COUNT downloaded files locally..."
LOCAL_VERIFY_MANIFEST=$(mktemp /tmp/local_manifest_XXXXXX.tsv)
trap 'rm -f "$LOCAL_VERIFY_MANIFEST"' EXIT
ssh -o BatchMode=yes "$EC2_HOST" "cat /tmp/collector-$TARGET_DATE.manifest" > "$LOCAL_VERIFY_MANIFEST"

VERIFY_RESULT=$(python3 - << EOF
import os, sys

dest = os.path.abspath("$DESTINATION_DIR")
manifest = "$LOCAL_VERIFY_MANIFEST"

verified = 0
errors = []

with open(manifest, "r") as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split("\t")
        if len(parts) < 2: continue
        rel, exp_size = parts[0], int(parts[1])
        local_path = os.path.join(dest, rel.replace('/', os.sep))
        if not os.path.isfile(local_path):
            errors.append(f"Missing file: {rel}")
            continue
        act_size = os.path.getsize(local_path)
        if act_size != exp_size:
            errors.append(f"Size mismatch: {rel} (expected {exp_size}, got {act_size})")
            continue
        verified += 1

if errors:
    print("FAILED")
    for err in errors:
        print(err)
    sys.exit(1)
else:
    print(f"OK\t{verified}")
EOF
)

if [[ ! "$VERIFY_RESULT" =~ ^OK[[:space:]]+([0-9]+) ]]; then
    log_error "Verification failed! Halting before deletion.\n$VERIFY_RESULT"
    log_warn "NO FILES WERE DELETED ON EC2 due to verification failure."
    exit 1
fi

log_success "      Verification 100% SUCCESSFUL: All $FILE_COUNT files match expected sizes."

if [ "$KEEP_REMOTE_FILES" = false ]; then
    log_info "[CLEANUP] Deleting $FILE_COUNT verified files from EC2 ($REMOTE_BASE_DIR)..."
    DELETE_CMD="cut -f1 '/tmp/collector-$TARGET_DATE.manifest' | sed 's|^|$REMOTE_BASE_DIR/|' | xargs -r -d '\n' sudo rm -f && rm -f '/tmp/collector-$TARGET_DATE.manifest' '/tmp/collector-$TARGET_DATE.files'"
    ssh -o BatchMode=yes "$EC2_HOST" "$DELETE_CMD"
    log_success "          Remote files successfully deleted from EC2."
else
    log_warn "[CLEANUP] Keeping remote files on EC2 (--keep-remote-files was specified)."
    ssh -o BatchMode=yes "$EC2_HOST" "rm -f '/tmp/collector-$TARGET_DATE.manifest' '/tmp/collector-$TARGET_DATE.files'"
fi

log_success "\nSUCCESS: Successfully downloaded, verified, and cleaned $FILE_COUNT files ($TOTAL_MB MB) for $TARGET_DATE."

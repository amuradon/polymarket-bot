<#
.SYNOPSIS
    Packages the Quarkus Data Collector fast-jar archive, uploads it to Amazon S3, and deploys it as a Bare-OS systemd service on AWS EC2 in eu-west-1 (with automated ~6-8s EBS hot-swap support when migrating instances).

.DESCRIPTION
    1. Determines the release version {x.y.z} from .\data\target\quarkus-app\app\data-{x.y.z}.jar.
    2. Resolves the S3 Artifact Bucket name and persistent EBS Volume ID from the Base Infrastructure stack in the target region (default: eu-west-1).
    3. Packages .\data\target\quarkus-app into .\data\target\data-{x.y.z}.tar.gz.
    4. Uploads the release archive to s3://<ArtifactBucketName>/releases/data-{x.y.z}.tar.gz.
    5. Deploys the archive and restarts the polymarket-data systemd service on the running 24/7 EC2 instance via AWS Systems Manager (SSM).
       If the persistent EBS volume is currently attached to a previous EC2 instance, pre-stages the release on the new instance first,
       creates an online EBS snapshot backup, and executes a fast ~6-8s EBS hot-swap so data recording resumes in the same .jsonl.zst files.

.PARAMETER Region
    Target AWS Region (default: eu-west-1).

.PARAMETER BaseStackName
    Name of the Data Collector Base Infrastructure stack (default: polymarket-bot-data-base).

.PARAMETER Ec2StackName
    Name of the Data Collector Compute stack (default: polymarket-bot-data-ec2).

.PARAMETER ArtifactBucket
    Amazon S3 bucket name override. If omitted, resolved from BaseStackName outputs.

.PARAMETER InstanceId
    EC2 Instance ID override. If omitted, resolved from Ec2StackName outputs.

.EXAMPLE
    .\infra\deploy-data.ps1
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$BaseStackName = "polymarket-bot-data-base",

    [Parameter(Mandatory = $false)]
    [string]$Ec2StackName = "polymarket-bot-data-ec2",

    [Parameter(Mandatory = $false)]
    [string]$ArtifactBucket = "",

    [Parameter(Mandatory = $false)]
    [string]$InstanceId = ""
)

$ErrorActionPreference = "Stop"
$env:PYTHONIOENCODING = "utf-8"

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $RepoRoot "pom.xml"))) {
    $RepoRoot = (Get-Location).Path
}

function Invoke-SsmShellScript {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetInstanceId,

        [Parameter(Mandatory = $true)]
        [string[]]$ShellCommands,

        [Parameter(Mandatory = $true)]
        [string]$Comment
    )

    $TempParamsFile = [System.IO.Path]::GetTempFileName()
    $ForwardPath = $TempParamsFile.Replace('\', '/')
    $CmdId = ""
    try {
        $ParamsObj = @{ commands = $ShellCommands }
        $ParamsJson = $ParamsObj | ConvertTo-Json -Depth 5
        $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        [System.IO.File]::WriteAllText($TempParamsFile, $ParamsJson, $utf8NoBom)

        $CmdId = (aws ssm send-command `
            --instance-ids $TargetInstanceId `
            --document-name "AWS-RunShellScript" `
            --comment $Comment `
            --parameters "file://$ForwardPath" `
            --region $Region `
            --query "Command.CommandId" `
            --output text)

        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($CmdId)) {
            throw "Failed to dispatch SSM command to instance $TargetInstanceId (Exit code: $LASTEXITCODE)"
        }
        $CmdId = $CmdId.Trim()
    } finally {
        Remove-Item -Force -ErrorAction SilentlyContinue $TempParamsFile
    }

    $Status = "Pending"
    $WaitCount = 0
    while ($Status -in @("Pending", "InProgress", "Delayed") -and $WaitCount -lt 120) {
        Start-Sleep -Milliseconds 1000
        $WaitCount += 1
        $Status = (aws ssm get-command-invocation `
            --command-id $CmdId `
            --instance-id $TargetInstanceId `
            --region $Region `
            --query "Status" `
            --output text 2>$null).Trim()
    }

    $Stdout = aws ssm get-command-invocation `
        --command-id $CmdId `
        --instance-id $TargetInstanceId `
        --region $Region `
        --query "StandardOutputContent" `
        --output text

    $Stderr = aws ssm get-command-invocation `
        --command-id $CmdId `
        --instance-id $TargetInstanceId `
        --region $Region `
        --query "StandardErrorContent" `
        --output text

    return [PSCustomObject]@{
        CommandId = $CmdId
        Status    = $Status
        Stdout    = $Stdout
        Stderr    = $Stderr
    }
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot: Package & Deploy Data Collector (Bare-OS)" -ForegroundColor Cyan
Write-Host " Region: $Region | Base: $BaseStackName | EC2: $Ec2StackName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Determine version from .\data\target\quarkus-app\app\data-{x.y.z}.jar
Write-Host "[1/4] Resolving version from .\data\target\quarkus-app\app\data-{x.y.z}.jar..." -ForegroundColor Yellow
$QuarkusAppDir = Join-Path $RepoRoot "data\target\quarkus-app"
$AppDir = Join-Path $QuarkusAppDir "app"
if (-not (Test-Path $AppDir)) {
    throw "Directory '$AppDir' not found. Run 'mvnw release:clean release:prepare' before deploying."
}

$JarFiles = @(Get-ChildItem -Path $AppDir -Filter "data-*.jar" -File)
if ($JarFiles.Count -eq 0) {
    throw "No 'data-*.jar' found in '$AppDir'. Run 'mvnw release:clean release:prepare' before deploying."
}
if ($JarFiles.Count -gt 1) {
    throw "Multiple 'data-*.jar' files found in '$AppDir': $($JarFiles.Name -join ', '). Expected a single release JAR."
}

$JarName = $JarFiles[0].Name
if ($JarName -notmatch '^data-(\d+\.\d+\.\d+)\.jar$') {
    throw "JAR file '$JarName' does not match expected release format 'data-{x.y.z}.jar' (e.g. data-1.2.1.jar). Run 'mvnw release:clean release:prepare' first."
}
$ReleaseVersion = $matches[1]
Write-Host "      Detected artifact: $JarName (Version: $ReleaseVersion)" -ForegroundColor Green

# 2. Resolve S3 Artifact Bucket Name and EBS Volume ID
$EbsVolumeId = ""
Write-Host "[2/4] Resolving S3 Artifact Bucket and EBS Volume ID from stack '$BaseStackName'..." -ForegroundColor Yellow
try {
    if (-not $ArtifactBucket) {
        $ArtifactBucket = (aws cloudformation describe-stacks `
            --stack-name $BaseStackName `
            --region $Region `
            --query "Stacks[0].Outputs[?OutputKey=='ArtifactBucketName'].OutputValue" `
            --output text 2>$null).Trim()
    }
    $EbsVolumeId = (aws cloudformation describe-stacks `
        --stack-name $BaseStackName `
        --region $Region `
        --query "Stacks[0].Outputs[?OutputKey=='EbsVolumeId'].OutputValue" `
        --output text 2>$null).Trim()
} catch {
    # Fallback handled below
}

if (-not $ArtifactBucket -or $ArtifactBucket -eq "None") {
    $AccountId = (aws sts get-caller-identity --query "Account" --output text).Trim()
    $ArtifactBucket = "$BaseStackName-artifacts-$AccountId-$Region"
}
Write-Host "      S3 Artifact Bucket: $ArtifactBucket" -ForegroundColor Green
if ($EbsVolumeId -and $EbsVolumeId -ne "None") {
    Write-Host "      Persistent EBS Volume ID: $EbsVolumeId" -ForegroundColor Green
}

# 3. Package .\data\target\quarkus-app into data-{x.y.z}.tar.gz and upload to S3
Write-Host "[3/4] Packaging quarkus-app and uploading to S3..." -ForegroundColor Yellow
$ArchivePath = Join-Path $RepoRoot "data\target\data-$ReleaseVersion.tar.gz"
if (Test-Path $ArchivePath) {
    Remove-Item -Force $ArchivePath
}

tar -czf $ArchivePath -C $QuarkusAppDir .
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $ArchivePath)) {
    throw "Failed to create release archive '$ArchivePath' using tar."
}

$S3ArtifactUri = "s3://$ArtifactBucket/releases/data-$ReleaseVersion.tar.gz"
aws s3 cp $ArchivePath $S3ArtifactUri --region $Region --only-show-errors
if ($LASTEXITCODE -ne 0) {
    throw "Failed to upload '$ArchivePath' to '$S3ArtifactUri' (Exit code: $LASTEXITCODE)"
}
Write-Host "SUCCESS: Uploaded release archive to $S3ArtifactUri." -ForegroundColor Green

# 4. Deploy Bare-OS systemd Service on EC2 via AWS Systems Manager (SSM)
Write-Host "[4/4] Deploying systemd service on EC2 instance via AWS Systems Manager..." -ForegroundColor Yellow
if (-not $InstanceId) {
    try {
        $InstanceId = (aws cloudformation describe-stacks `
            --stack-name $Ec2StackName `
            --region $Region `
            --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" `
            --output text 2>$null).Trim()
    } catch {
        $InstanceId = ""
    }
}

if (-not $InstanceId -or $InstanceId -eq "None") {
    Write-Host "WARNING: EC2 Instance ID could not be resolved from stack '$Ec2StackName'." -ForegroundColor Yellow
    Write-Host "Please provision the compute stack using 'infra/data-collector-ec2.yaml' or specify -InstanceId." -ForegroundColor Yellow
    exit 0
}

Write-Host "      Target EC2 Instance ID: $InstanceId" -ForegroundColor Green

# Check if the persistent EBS volume is currently attached to a different (previous) EC2 instance
$AttachedInstanceId = ""
if ($EbsVolumeId -and $EbsVolumeId -ne "None") {
    try {
        $AttachedInstanceId = (aws ec2 describe-volumes `
            --region $Region `
            --volume-ids $EbsVolumeId `
            --query "Volumes[0].Attachments[0].InstanceId" `
            --output text 2>$null).Trim()
    } catch {
        $AttachedInstanceId = ""
    }
}

if ($AttachedInstanceId -and $AttachedInstanceId -ne "None" -and $AttachedInstanceId -ne $InstanceId) {
    Write-Host "      Detected EBS Volume $EbsVolumeId currently attached to previous instance $AttachedInstanceId." -ForegroundColor Yellow
    Write-Host "      [4a] Pre-staging release $ReleaseVersion on new instance $InstanceId (zero downtime on $AttachedInstanceId)..." -ForegroundColor Yellow

    $StageResult = Invoke-SsmShellScript -TargetInstanceId $InstanceId -Comment "Pre-stage Data Collector $ReleaseVersion" -ShellCommands @(
        "set -euxo pipefail",
        "export AWS_REGION=$Region",
        "sudo /usr/local/bin/deploy-polymarket-data.sh `"$S3ArtifactUri`" --stage-only"
    )
    if ($StageResult.Status -ne "Success") {
        throw "Failed to pre-stage release on new instance $InstanceId`: $($StageResult.Stderr)"
    }

    Write-Host "      [4b] Triggering online EBS Snapshot backup of $EbsVolumeId..." -ForegroundColor Yellow
    $SnapshotId = (aws ec2 create-snapshot `
        --region $Region `
        --volume-id $EbsVolumeId `
        --description "Pre-migration backup of $EbsVolumeId before hot-swap to $InstanceId (v$ReleaseVersion)" `
        --query "SnapshotId" `
        --output text).Trim()
    Write-Host "           Created online backup Snapshot: $SnapshotId" -ForegroundColor Green

    Write-Host "      [4c] Executing fast EBS Hot-Swap from $AttachedInstanceId to $InstanceId..." -ForegroundColor Yellow
    $SwapWatch = [System.Diagnostics.Stopwatch]::StartNew()

    # Cleanly stop collector and unmount XFS volume on old instance
    $StopOldResult = Invoke-SsmShellScript -TargetInstanceId $AttachedInstanceId -Comment "Stop collector and unmount EBS for hot-swap" -ShellCommands @(
        "set -euxo pipefail",
        "if command -v docker >/dev/null 2>&1; then sudo docker stop -t 5 polymarket-bot-data 2>/dev/null || true; sudo docker rm -f polymarket-bot-data 2>/dev/null || true; fi",
        "sudo systemctl stop polymarket-data 2>/dev/null || true",
        "sync",
        "sudo umount /mnt/ebs/polymarket-data 2>/dev/null || sudo umount -l /mnt/ebs/polymarket-data 2>/dev/null || true"
    )
    if ($StopOldResult.Status -ne "Success") {
        Write-Warning "Stop/unmount on old instance $AttachedInstanceId returned $($StopOldResult.Status): $($StopOldResult.Stderr)"
    }

    aws ec2 detach-volume --region $Region --volume-id $EbsVolumeId --instance-id $AttachedInstanceId | Out-Null
    for ($i = 0; $i -lt 60; $i++) {
        $VolState = (aws ec2 describe-volumes --region $Region --volume-ids $EbsVolumeId --query "Volumes[0].State" --output text).Trim()
        if ($VolState -eq "available") {
            break
        }
        Start-Sleep -Milliseconds 500
    }

    $ActivateResult = Invoke-SsmShellScript -TargetInstanceId $InstanceId -Comment "Activate staged Data Collector $ReleaseVersion and mount EBS" -ShellCommands @(
        "set -euxo pipefail",
        "export AWS_REGION=$Region",
        "sudo /usr/local/bin/deploy-polymarket-data.sh `"$S3ArtifactUri`" --activate-staged",
        "sleep 2",
        "sudo systemctl status polymarket-data --no-pager"
    )
    $SwapWatch.Stop()

    Write-Host "---------------- SSM Execution Output ----------------" -ForegroundColor Cyan
    Write-Host $ActivateResult.Stdout
    Write-Host "------------------------------------------------------" -ForegroundColor Cyan

    if ($ActivateResult.Status -eq "Success") {
        $ElapsedSec = [math]::Round($SwapWatch.Elapsed.TotalSeconds, 1)
        Write-Host "SUCCESS: EBS Hot-Swap completed in ${ElapsedSec}s! Data Collector $ReleaseVersion is running on $InstanceId." -ForegroundColor Green
    } else {
        Write-Host "ERROR: Activation failed with status '$($ActivateResult.Status)': $($ActivateResult.Stderr)" -ForegroundColor Red
        exit 1
    }
} else {
    $DeployResult = Invoke-SsmShellScript -TargetInstanceId $InstanceId -Comment "Deploy Polymarket Data Collector $ReleaseVersion (Bare-OS systemd)" -ShellCommands @(
        "set -euxo pipefail",
        "export AWS_REGION=$Region",
        "if command -v docker >/dev/null 2>&1; then sudo docker rm -f polymarket-bot-data 2>/dev/null || true; sudo systemctl disable --now docker 2>/dev/null || true; fi",
        "if command -v iptables >/dev/null 2>&1; then sudo iptables -t nat -F 2>/dev/null || true; fi",
        "sudo /usr/local/bin/deploy-polymarket-data.sh `"$S3ArtifactUri`"",
        "sleep 3",
        "sudo systemctl status polymarket-data --no-pager"
    )

    Write-Host "---------------- SSM Execution Output ----------------" -ForegroundColor Cyan
    Write-Host $DeployResult.Stdout
    Write-Host "------------------------------------------------------" -ForegroundColor Cyan

    if ($DeployResult.Status -eq "Success") {
        Write-Host "SUCCESS: Data Collector application $ReleaseVersion deployed and running on $InstanceId!" -ForegroundColor Green
    } else {
        Write-Host "ERROR: Deployment failed with status '$($DeployResult.Status)': $($DeployResult.Stderr)" -ForegroundColor Red
        exit 1
    }
}

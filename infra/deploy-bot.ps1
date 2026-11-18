<#
.SYNOPSIS
    Packages the unified Quarkus Polymarket Bot fast-jar archive, uploads it to Amazon S3, and deploys it as a Bare-OS systemd service on AWS EC2 in eu-west-1 with health verification and automatic rollback.

.DESCRIPTION
    1. Determines the release version {x.y.z} from .\bot\target\quarkus-app\app\bot-{x.y.z}.jar.
    2. Verifies Quarkus CDI bean entries inside .\bot\target\quarkus-app\quarkus\generated-bytecode.jar.
    3. Resolves the S3 Artifact Bucket name from the Base Infrastructure stack in eu-west-1 (default: polymarket-bot-base).
    4. Packages .\bot\target\quarkus-app into .\bot\target\bot-{x.y.z}.tar.gz and uploads it to s3://<ArtifactBucketName>/releases/bot-{x.y.z}.tar.gz.
    5. Executes /usr/local/bin/deploy-polymarket-bot.sh via AWS Systems Manager (SSM), preserving runtime-state.json (twapOpenPrice, strategyMode, openPositions), verifying /api/health, and automatically rolling back to .previous on failure.

.PARAMETER Region
    Target AWS Region (default: eu-west-1).

.PARAMETER BaseStackName
    Name of the Polymarket Bot Base Infrastructure stack (default: polymarket-bot-base).

.PARAMETER Ec2StackName
    Name of the Polymarket Bot Compute stack (default: polymarket-bot-ec2).

.PARAMETER ArtifactBucket
    Amazon S3 bucket name override. If omitted, resolved from BaseStackName outputs.

.PARAMETER InstanceId
    EC2 Instance ID override. If omitted, resolved from Ec2StackName outputs.

.EXAMPLE
    .\infra\deploy-bot.ps1
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$BaseStackName = "polymarket-bot-base",

    [Parameter(Mandatory = $false)]
    [string]$Ec2StackName = "polymarket-bot-ec2",

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

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot: Package & Deploy Unified Bot (Bare-OS)   " -ForegroundColor Cyan
Write-Host " Region: $Region | Base: $BaseStackName | EC2: $Ec2StackName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Determine version from .\bot\target\quarkus-app\app\bot-{x.y.z}.jar
Write-Host "[1/4] Resolving version from .\bot\target\quarkus-app\app\bot-{x.y.z}.jar..." -ForegroundColor Yellow
$QuarkusAppDir = Join-Path $RepoRoot "bot\target\quarkus-app"
$AppDir = Join-Path $QuarkusAppDir "app"
if (-not (Test-Path $AppDir)) {
    throw "Directory '$AppDir' not found. Run 'mvnw release:clean release:prepare' before deploying."
}

$JarFiles = @(Get-ChildItem -Path $AppDir -Filter "bot-*.jar" -File)
if ($JarFiles.Count -eq 0) {
    throw "No 'bot-*.jar' found in '$AppDir'. Run 'mvnw release:clean release:prepare' before deploying."
}
if ($JarFiles.Count -gt 1) {
    throw "Multiple 'bot-*.jar' files found in '$AppDir': $($JarFiles.Name -join ', '). Expected a single release JAR."
}

$JarName = $JarFiles[0].Name
if ($JarName -notmatch '^bot-(\d+\.\d+\.\d+)\.jar$') {
    throw "JAR file '$JarName' does not match expected release format 'bot-{x.y.z}.jar' (e.g. bot-1.4.0.jar). Run 'mvnw release:clean release:prepare' first."
}
$ReleaseVersion = $matches[1]
Write-Host "      Detected artifact: $JarName (Version: $ReleaseVersion)" -ForegroundColor Green

# Verify that quarkus/generated-bytecode.jar contains all required unified bot CDI beans
$GeneratedBytecodeJar = Join-Path $QuarkusAppDir "quarkus\generated-bytecode.jar"
if (-not (Test-Path $GeneratedBytecodeJar)) {
    throw "Missing '$GeneratedBytecodeJar'. Run '.\mvnw package -pl bot -am -DskipTests' before deploying."
}
$GeneratedEntries = @(jar tf $GeneratedBytecodeJar)
$RequiredBeanMarkers = @(
    "StrategyTradingEngine_Bean.class",
    "DataCollectorService_Bean.class",
    "RuntimeCoordinator_Bean.class",
    "RuntimeCoordinator_Observer_onStart",
    "DashboardResource_Bean.class"
)
foreach ($Marker in $RequiredBeanMarkers) {
    if (-not ($GeneratedEntries | Where-Object { $_ -like "*$Marker*" })) {
        throw "Verification failed: '$GeneratedBytecodeJar' is missing CDI bean entry matching '$Marker'. Run '.\mvnw package -pl bot -am -DskipTests' and re-run deploy-bot.ps1."
    }
}
Write-Host "      Verified Quarkus CDI bytecode in generated-bytecode.jar ($($GeneratedEntries.Count) entries)" -ForegroundColor Green

# 2. Resolve S3 Artifact Bucket Name
if (-not $ArtifactBucket) {
    Write-Host "[2/4] Resolving S3 Artifact Bucket name from stack '$BaseStackName'..." -ForegroundColor Yellow
    try {
        $ArtifactBucket = (aws cloudformation describe-stacks `
            --stack-name $BaseStackName `
            --region $Region `
            --query "Stacks[0].Outputs[?OutputKey=='ArtifactBucketName'].OutputValue" `
            --output text 2>$null).Trim()
    } catch {
        $ArtifactBucket = ""
    }

    if (-not $ArtifactBucket -or $ArtifactBucket -eq "None") {
        $AccountId = (aws sts get-caller-identity --query "Account" --output text).Trim()
        $ArtifactBucket = "$BaseStackName-artifacts-$AccountId-$Region"
    }
}
Write-Host "      S3 Artifact Bucket: $ArtifactBucket" -ForegroundColor Green

# 3. Package .\bot\target\quarkus-app into bot-{x.y.z}.tar.gz and upload to S3
Write-Host "[3/4] Packaging quarkus-app and uploading to S3..." -ForegroundColor Yellow
$ArchivePath = Join-Path $RepoRoot "bot\target\bot-$ReleaseVersion.tar.gz"
if (Test-Path $ArchivePath) {
    Remove-Item -Force $ArchivePath
}

tar -czf $ArchivePath -C $QuarkusAppDir .
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $ArchivePath)) {
    throw "Failed to create release archive '$ArchivePath' using tar."
}

$S3ArtifactUri = "s3://$ArtifactBucket/releases/bot-$ReleaseVersion.tar.gz"
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
    Write-Host "Please provision the compute stack using 'infra/polymarket-bot-ec2.yaml' or specify -InstanceId." -ForegroundColor Yellow
    exit 0
}

Write-Host "      Target EC2 Instance ID: $InstanceId" -ForegroundColor Green

$Commands = @(
    "set -euxo pipefail",
    "export AWS_REGION=$Region",
    "sudo /usr/local/bin/deploy-polymarket-bot.sh `"$S3ArtifactUri`"",
    "sudo systemctl status polymarket-bot --no-pager",
    "curl -fsS http://127.0.0.1:443/api/health"
)

$TempParamsFile = [System.IO.Path]::GetTempFileName()
$ForwardPath = $TempParamsFile.Replace('\', '/')
$SsmCommandId = ""
try {
    $ParamsObj = @{
        commands = $Commands
    }
    $ParamsJson = $ParamsObj | ConvertTo-Json -Depth 5
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($TempParamsFile, $ParamsJson, $utf8NoBom)

    $SsmCommandId = (aws ssm send-command `
        --instance-ids $InstanceId `
        --document-name "AWS-RunShellScript" `
        --comment "Deploy Polymarket Bot $ReleaseVersion (Bare-OS systemd + auto-rollback)" `
        --parameters "file://$ForwardPath" `
        --region $Region `
        --query "Command.CommandId" `
        --output text)

    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($SsmCommandId)) {
        throw "Failed to dispatch SSM command to instance $InstanceId (Exit code: $LASTEXITCODE)"
    }
    $SsmCommandId = $SsmCommandId.Trim()
} finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $TempParamsFile
}

Write-Host "      Dispatched SSM Command ID: $SsmCommandId. Waiting for execution & health check..." -ForegroundColor Yellow

$Status = "Pending"
$WaitCount = 0
while ($Status -in @("Pending", "InProgress", "Delayed") -and $WaitCount -lt 60) {
    Start-Sleep -Seconds 3
    $WaitCount += 1
    $Status = (aws ssm get-command-invocation `
        --command-id $SsmCommandId `
        --instance-id $InstanceId `
        --region $Region `
        --query "Status" `
        --output text 2>$null).Trim()
}

$Output = aws ssm get-command-invocation `
    --command-id $SsmCommandId `
    --instance-id $InstanceId `
    --region $Region `
    --query "StandardOutputContent" `
    --output text

Write-Host "---------------- SSM Execution Output ----------------" -ForegroundColor Cyan
Write-Host $Output
Write-Host "------------------------------------------------------" -ForegroundColor Cyan

if ($Status -eq "Success") {
    Write-Host "SUCCESS: Polymarket Bot $ReleaseVersion deployed, verified healthy, and running on $InstanceId!" -ForegroundColor Green
} else {
    $ErrorOutput = aws ssm get-command-invocation `
        --command-id $SsmCommandId `
        --instance-id $InstanceId `
        --region $Region `
        --query "StandardErrorContent" `
        --output text
    Write-Host "ERROR: Deployment failed with status '$Status' (automatic rollback executed if previous release existed): $ErrorOutput" -ForegroundColor Red
    exit 1
}

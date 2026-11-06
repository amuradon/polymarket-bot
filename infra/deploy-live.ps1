<#
.SYNOPSIS
    Builds, releases, packages, and deploys the Quarkus Live Trading application to AWS EC2 in eu-west-1.

.DESCRIPTION
    1. Executes Maven release with optional SemVer versioning (e.g. 1.1.0).
       - If -Version x.y.z is supplied: sets -DreleaseVersion=x.y.z and -DdevelopmentVersion=x.y.(z+1)-SNAPSHOT.
       - If -Version is omitted: Maven release automatically increments the patch version in batch mode.
    2. Packages the Quarkus Live Trading multi-module application via Maven.
    3. Authenticates Docker with Amazon ECR in the target region (default: eu-west-1).
    4. Cross-compiles for AWS Graviton (linux/arm64) using Docker Buildx and tags with SemVer and 'latest'.
    5. Pushes Docker images to the Amazon ECR repository.
    6. Deploys the updated container on the 24/7 EC2 instance via AWS Systems Manager (SSM) without open SSH ports.

.PARAMETER Version
    Optional SemVer version for Maven release (format: x.y.z). If omitted, patch version is incremented automatically.

.PARAMETER Region
    Target AWS Region (default: eu-west-1).

.PARAMETER BaseStackName
    Name of the Live Trading Base Infrastructure stack (default: polymarket-bot-live-base).

.PARAMETER Ec2StackName
    Name of the Live Trading Compute stack (default: polymarket-bot-live-ec2).

.PARAMETER RepositoryUri
    Amazon ECR repository URI override. If omitted, resolved from BaseStackName outputs.

.PARAMETER InstanceId
    EC2 Instance ID override. If omitted, resolved from Ec2StackName outputs.

.PARAMETER DryRun
    If specified, runs the live trading engine in dry-run mode (paper orders against live Polymarket CLOB).

.PARAMETER SkipRelease
    If specified, skips running Maven release.

.PARAMETER SkipBuild
    If specified, skips running Maven package.

.EXAMPLE
    # Auto-increment patch version and deploy:
    .\infra\deploy-live.ps1

    # Specify exact SemVer release version:
    .\infra\deploy-live.ps1 -Version "1.1.0"

    # Deploy in dry-run mode without Maven release:
    .\infra\deploy-live.ps1 -SkipRelease -DryRun
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Version = "",

    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$BaseStackName = "polymarket-bot-live-base",

    [Parameter(Mandatory = $false)]
    [string]$Ec2StackName = "polymarket-bot-live-ec2",

    [Parameter(Mandatory = $false)]
    [string]$RepositoryUri = "",

    [Parameter(Mandatory = $false)]
    [string]$InstanceId = "",

    [Parameter(Mandatory = $false)]
    [switch]$DryRun,

    [Parameter(Mandatory = $false)]
    [switch]$SkipRelease,

    [Parameter(Mandatory = $false)]
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $RepoRoot "pom.xml"))) {
    $RepoRoot = (Get-Location).Path
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot: Release, Build & Deploy Live Trading App " -ForegroundColor Cyan
Write-Host " Region: $Region | Base: $BaseStackName | EC2: $Ec2StackName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Determine SemVer & Execute Maven Release
$ReleaseVersion = ""
if (-not $SkipRelease) {
    Write-Host "[1/6] Executing Maven release on project root..." -ForegroundColor Yellow
    Push-Location $RepoRoot
    try {
        if ($Version) {
            if ($Version -notmatch '^(\d+)\.(\d+)\.(\d+)$') {
                throw "Invalid version format '$Version'. Version must be in format x.y.z (SemVer), e.g. 1.1.0"
            }
            $major = $matches[1]
            $minor = $matches[2]
            $patch = [int]$matches[3]
            $nextPatch = $patch + 1
            $devVersion = "$major.$minor.$nextPatch-SNAPSHOT"
            $ReleaseVersion = $Version

            Write-Host "      Release version:     $ReleaseVersion" -ForegroundColor Green
            Write-Host "      Development version: $devVersion" -ForegroundColor Green

            & .\mvnw.cmd -B release:clean release:prepare `
                "-DreleaseVersion=$ReleaseVersion" `
                "-DdevelopmentVersion=$devVersion" `
                "-Dtag=$ReleaseVersion"
        } else {
            [xml]$pomXml = Get-Content (Join-Path $RepoRoot "pom.xml")
            $rawVer = $pomXml.project.version
            $ReleaseVersion = $rawVer -replace '-SNAPSHOT', ''
            Write-Host "      Auto-incrementing from current POM version: $rawVer (Release: $ReleaseVersion)" -ForegroundColor Green

            & .\mvnw.cmd -B release:clean release:prepare
        }

        if ($LASTEXITCODE -ne 0) {
            throw "Maven release failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[1/6] Skipping Maven release as requested..." -ForegroundColor Yellow
    if ($Version) {
        $ReleaseVersion = $Version
    } else {
        [xml]$pomXml = Get-Content (Join-Path $RepoRoot "pom.xml")
        $ReleaseVersion = $pomXml.project.version -replace '-SNAPSHOT', ''
    }
    Write-Host "      Target image version tag: $ReleaseVersion" -ForegroundColor Green
}

# 2. Package Quarkus Live Trading application via Maven
if (-not $SkipBuild) {
    Write-Host "[2/6] Packaging Quarkus application via Maven wrapper..." -ForegroundColor Yellow
    Push-Location $RepoRoot
    try {
        & .\mvnw.cmd clean package -DskipTests -pl connector,trading,live
        if ($LASTEXITCODE -ne 0) {
            throw "Maven package failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[2/6] Skipping Maven package as requested..." -ForegroundColor Yellow
}

# 3. Resolve Amazon ECR Repository URI
if (-not $RepositoryUri) {
    Write-Host "[3/6] Resolving Amazon ECR repository URI..." -ForegroundColor Yellow
    try {
        $RepositoryUri = (aws cloudformation describe-stacks `
            --stack-name $BaseStackName `
            --region $Region `
            --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" `
            --output text 2>$null).Trim()
    } catch {
        $RepositoryUri = ""
    }

    if (-not $RepositoryUri -or $RepositoryUri -eq "None") {
        $AccountId = (aws sts get-caller-identity --query "Account" --output text).Trim()
        $RepositoryUri = "$AccountId.dkr.ecr.$Region.amazonaws.com/$BaseStackName-repo"
    }
}
Write-Host "      ECR Repository URI: $RepositoryUri" -ForegroundColor Green

# 4. Authenticate Docker with Amazon ECR
Write-Host "[4/6] Authenticating Docker with Amazon ECR in $Region..." -ForegroundColor Yellow
$EcrDomain = ($RepositoryUri -split '/')[0]
aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $EcrDomain
if ($LASTEXITCODE -ne 0) {
    throw "ECR authentication failed with exit code $LASTEXITCODE"
}

# 5. Build and Push Multi-Platform ARM64 Docker Images (Docker Buildx)
Write-Host "[5/6] Building and pushing linux/arm64 Docker image via Buildx..." -ForegroundColor Yellow
$VersionedUri = "$RepositoryUri`:$ReleaseVersion"
$LatestUri = "$RepositoryUri`:latest"

Push-Location $RepoRoot
try {
    docker buildx build --platform linux/arm64 `
        -t $VersionedUri `
        -t $LatestUri `
        -f live/src/main/docker/Dockerfile.jvm `
        --push live

    if ($LASTEXITCODE -ne 0) {
        throw "Docker buildx build failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
Write-Host "SUCCESS: Pushed $VersionedUri and $LatestUri to Amazon ECR." -ForegroundColor Green

# 6. Deploy Container on EC2 via AWS Systems Manager (SSM)
Write-Host "[6/6] Deploying container on EC2 instance via AWS Systems Manager..." -ForegroundColor Yellow
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
    Write-Host "Please provision the compute stack using 'infra/live-trading-ec2.yaml' or specify -InstanceId." -ForegroundColor Yellow
    exit 0
}

Write-Host "      Target EC2 Instance ID: $InstanceId" -ForegroundColor Green

$DryRunVal = if ($DryRun) { "true" } else { "false" }

$Commands = @(
    "set -euxo pipefail",
    "export AWS_REGION=$Region",
    "ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)",
    "aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin `"`$ACCOUNT_ID.dkr.ecr.$Region.amazonaws.com`"",
    "docker pull $VersionedUri",
    "sudo /usr/local/bin/start-polymarket-live.sh `"$VersionedUri`" $DryRunVal",
    "sleep 3",
    "docker ps --filter name=polymarket-live"
)

$CommandStr = $Commands -join "`n"

$SsmCommandId = (aws ssm send-command `
    --instance-ids $InstanceId `
    --document-name "AWS-RunShellScript" `
    --comment "Deploy Polymarket Live Trading $ReleaseVersion" `
    --parameters "commands=[`"$($CommandStr -replace '"', '\"')`"]" `
    --region $Region `
    --query "Command.CommandId" `
    --output text).Trim()

Write-Host "      Dispatched SSM Command ID: $SsmCommandId. Waiting for execution..." -ForegroundColor Yellow

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
    Write-Host "SUCCESS: Live Trading application $ReleaseVersion deployed and running on $InstanceId!" -ForegroundColor Green
} else {
    $ErrorOutput = aws ssm get-command-invocation `
        --command-id $SsmCommandId `
        --instance-id $InstanceId `
        --region $Region `
        --query "StandardErrorContent" `
        --output text
    Write-Host "ERROR: Deployment failed with status '$Status': $ErrorOutput" -ForegroundColor Red
    exit 1
}

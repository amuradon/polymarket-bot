<#
.SYNOPSIS
    Builds, releases, packages, and uploads the Java 25 Telegram Alert Lambda container image to Amazon ECR.

.DESCRIPTION
    1. Executes Maven release with optional SemVer versioning (e.g. 1.0.1).
       - If -Version x.y.z is supplied: sets -DreleaseVersion=x.y.z and -DdevelopmentVersion=x.y.(z+1)-SNAPSHOT.
       - If -Version is omitted: Maven release automatically increments the patch version in batch mode.
    2. Packages the Java 25 shaded uber-jar via Maven.
    3. Authenticates Docker with Amazon ECR in the target region (default: eu-west-1).
    4. Builds and tags the Docker image with the released SemVer version and 'latest'.
    5. Pushes the Docker images to the Amazon ECR repository provisioned by infra/telegram-alert-base.yaml.
    6. If the Lambda function already exists, updates its code in-place.

.PARAMETER Version
    Optional SemVer version for Maven release (format: x.y.z). If omitted, patch version is incremented automatically.

.PARAMETER Region
    Target AWS Region (default: eu-west-1).

.PARAMETER BaseStackName
    Name of the Telegram Alert Base Infrastructure stack (default: polymarket-bot-alert-base).

.PARAMETER RepositoryUri
    Amazon ECR repository URI override. If omitted, resolved from BaseStackName outputs or account ID.

.PARAMETER FunctionName
    Optional Lambda function name to update in-place if it already exists (default: polymarket-bot-alert-lambda-function).

.PARAMETER SkipRelease
    If specified, skips running Maven release.

.PARAMETER SkipBuild
    If specified, skips running Maven package.

.EXAMPLE
    # Auto-increment patch version:
    .\infra\deploy-telegram-alert.ps1

    # Specify exact SemVer release version:
    .\infra\deploy-telegram-alert.ps1 -Version "1.0.1"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Version = "",

    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$BaseStackName = "polymarket-bot-alert-base",

    [Parameter(Mandatory = $false)]
    [string]$RepositoryUri = "",

    [Parameter(Mandatory = $false)]
    [string]$FunctionName = "polymarket-bot-alert-lambda-function",

    [Parameter(Mandatory = $false)]
    [switch]$SkipRelease,

    [Parameter(Mandatory = $false)]
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$AlertModuleDir = Join-Path $ScriptDir "telegram_alert"
if (-not (Test-Path $AlertModuleDir)) {
    $AlertModuleDir = $ScriptDir
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot: Release & Upload Alert Lambda Container  " -ForegroundColor Cyan
Write-Host " Region: $Region | Base Stack: $BaseStackName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Determine SemVer & Execute Maven Release
$ReleaseVersion = ""
if (-not $SkipRelease) {
    Write-Host "[1/5] Executing Maven release..." -ForegroundColor Yellow
    Push-Location $AlertModuleDir
    try {
        if ($Version) {
            if ($Version -notmatch '^(\d+)\.(\d+)\.(\d+)$') {
                throw "Invalid version format '$Version'. Version must be in format x.y.z (SemVer), e.g. 1.0.1"
            }
            $major = $matches[1]
            $minor = $matches[2]
            $patch = [int]$matches[3]
            $nextPatch = $patch + 1
            $devVersion = "$major.$minor.$nextPatch-SNAPSHOT"
            $ReleaseVersion = $Version

            Write-Host "      Release version:     $ReleaseVersion" -ForegroundColor Green
            Write-Host "      Development version: $devVersion" -ForegroundColor Green

            & mvn -B release:clean release:prepare `
                "-DreleaseVersion=$ReleaseVersion" `
                "-DdevelopmentVersion=$devVersion" `
                "-Dtag=telegram-alert-$ReleaseVersion"
        } else {
            [xml]$pomXml = Get-Content (Join-Path $AlertModuleDir "pom.xml")
            $rawVer = $pomXml.project.version
            $ReleaseVersion = $rawVer -replace '-SNAPSHOT', ''
            Write-Host "      Auto-incrementing from current POM version: $rawVer (Release: $ReleaseVersion)" -ForegroundColor Green

            & mvn -B release:clean release:prepare
        }

        if ($LASTEXITCODE -ne 0) {
            throw "Maven release failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[1/5] Skipping Maven release as requested..." -ForegroundColor Yellow
    if ($Version) {
        $ReleaseVersion = $Version
    } else {
        [xml]$pomXml = Get-Content (Join-Path $AlertModuleDir "pom.xml")
        $ReleaseVersion = $pomXml.project.version -replace '-SNAPSHOT', ''
    }
    Write-Host "      Target image version tag: $ReleaseVersion" -ForegroundColor Green
}

# 2. Package the Java 25 shaded jar via Maven
if (-not $SkipBuild) {
    Write-Host "[2/5] Packaging shaded uber-jar via Maven..." -ForegroundColor Yellow
    Push-Location $AlertModuleDir
    try {
        & mvn clean package -DskipTests
        if ($LASTEXITCODE -ne 0) {
            throw "Maven package failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[2/5] Skipping Maven package as requested..." -ForegroundColor Yellow
}

$JarPath = Join-Path $AlertModuleDir "target\telegram-alert-$ReleaseVersion.jar"
if (-not (Test-Path $JarPath)) {
    # Fallback to wildcard search in target directory
    $FoundJar = Get-ChildItem -Path (Join-Path $AlertModuleDir "target") -Filter "telegram-alert-*.jar" | Where-Object { $_.Name -notmatch "original-" } | Select-Object -First 1
    if ($FoundJar) {
        $JarPath = $FoundJar.FullName
    } else {
        throw "Target shaded jar not found in $AlertModuleDir\target. Please run without -SkipBuild."
    }
}
Write-Host "      Using shaded jar: $JarPath" -ForegroundColor Green

# 3. Resolve Amazon ECR Repository URI
if (-not $RepositoryUri) {
    Write-Host "[3/5] Resolving Amazon ECR repository URI..." -ForegroundColor Yellow
    try {
        $RepositoryUri = (aws cloudformation describe-stacks `
            --stack-name $BaseStackName `
            --region $Region `
            --query "Stacks[0].Outputs[?OutputKey=='TelegramAlertEcrRepositoryUri'].OutputValue" `
            --output text 2>$null).Trim()
    } catch {
        $RepositoryUri = ""
    }

    if (-not $RepositoryUri -or $RepositoryUri -eq "None") {
        $AccountId = (aws sts get-caller-identity --query "Account" --output text).Trim()
        $RepositoryUri = "$AccountId.dkr.ecr.$Region.amazonaws.com/$BaseStackName-telegram-alert-repo"
    }
}
Write-Host "      ECR Repository URI: $RepositoryUri" -ForegroundColor Green

# 4. Authenticate Docker with Amazon ECR
Write-Host "[4/5] Authenticating Docker with Amazon ECR in $Region..." -ForegroundColor Yellow
$EcrDomain = ($RepositoryUri -split '/')[0]
aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $EcrDomain
if ($LASTEXITCODE -ne 0) {
    throw "ECR authentication failed with exit code $LASTEXITCODE"
}

# 5. Build and Push Docker Images (SemVer + latest)
Write-Host "[5/5] Building and pushing Docker container image (linux/arm64 & linux/amd64 compatible)..." -ForegroundColor Yellow
$VersionedUri = "$RepositoryUri`:$ReleaseVersion"
$LatestUri = "$RepositoryUri`:latest"

docker build -t $VersionedUri -t $LatestUri $AlertModuleDir
if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed with exit code $LASTEXITCODE"
}

docker push $VersionedUri
if ($LASTEXITCODE -ne 0) {
    throw "Docker push for $VersionedUri failed with exit code $LASTEXITCODE"
}

docker push $LatestUri
if ($LASTEXITCODE -ne 0) {
    throw "Docker push for $LatestUri failed with exit code $LASTEXITCODE"
}

Write-Host "SUCCESS: Pushed $VersionedUri and $LatestUri to Amazon ECR." -ForegroundColor Green

# 6. Update existing Lambda function if present
$FunctionExists = (aws lambda get-function --function-name $FunctionName --region $Region 2>&1)
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating deployed Lambda function '$FunctionName' to image $VersionedUri..." -ForegroundColor Yellow
    aws lambda update-function-code `
        --function-name $FunctionName `
        --image-uri $VersionedUri `
        --region $Region `
        --no-cli-pager | Out-Null

    aws lambda wait function-updated `
        --function-name $FunctionName `
        --region $Region

    Write-Host "SUCCESS: Lambda function '$FunctionName' updated to $VersionedUri" -ForegroundColor Green
} else {
    Write-Host "NOTE: Function '$FunctionName' not yet created. Deploy CFN stack 'infra/telegram-alert-lambda.yaml' with ImageTag=$ReleaseVersion." -ForegroundColor Cyan
}

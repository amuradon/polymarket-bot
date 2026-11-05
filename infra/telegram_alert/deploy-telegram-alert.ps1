<#
.SYNOPSIS
    Builds, packages, and deploys the Java 25 Telegram Alert Lambda container image to Amazon ECR and AWS Lambda.

.DESCRIPTION
    1. Compiles the Java 25 project and packages the shaded uber-jar via Maven.
    2. Logs into Amazon ECR in the target region (default: eu-west-1).
    3. Builds the Docker container image using the Amazon Corretto 25 runtime.
    4. Pushes the Docker image to the Amazon ECR repository.
    5. Updates the AWS Lambda function code with the newly pushed image URI.

.PARAMETER Region
    Target AWS Region (default: eu-west-1).

.PARAMETER StackName
    CloudFormation stack name (default: polymarket-bot-live).

.PARAMETER RepositoryUri
    Amazon ECR repository URI. If omitted, automatically resolves using AWS Account ID and '${StackName}-telegram-alert-repo'.

.PARAMETER FunctionName
    AWS Lambda function name (default: '${StackName}-telegram-alert').

.PARAMETER ImageTag
    Docker image tag (default: latest).

.PARAMETER SkipBuild
    If specified, skips running Maven package.

.EXAMPLE
    .\deploy-telegram-alert.ps1 -Region eu-west-1 -StackName "polymarket-bot-live"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$StackName = "polymarket-bot-live",

    [Parameter(Mandatory = $false)]
    [string]$RepositoryUri = "",

    [Parameter(Mandatory = $false)]
    [string]$FunctionName = "",

    [Parameter(Mandatory = $false)]
    [string]$ImageTag = "latest",

    [Parameter(Mandatory = $false)]
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

if (-not $FunctionName) {
    $FunctionName = "$StackName-telegram-alert"
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot: Deploy Java 25 Telegram Alert Lambda     " -ForegroundColor Cyan
Write-Host " Region: $Region | Stack: $StackName | Function: $FunctionName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Compile and package the Java 25 shaded jar
if (-not $SkipBuild) {
    Write-Host "[1/4] Building Java 25 shaded jar via Maven..." -ForegroundColor Yellow
    Push-Location $ScriptDir
    try {
        & mvn clean package
        if ($LASTEXITCODE -ne 0) {
            throw "Maven build failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "[1/4] Skipping Maven package as requested..." -ForegroundColor Yellow
}

$JarPath = Join-Path $ScriptDir "target\telegram-alert-1.0.0.jar"
if (-not (Test-Path $JarPath)) {
    throw "Target shaded jar not found at $JarPath. Please run without -SkipBuild."
}

# 2. Resolve ECR Repository URI
if (-not $RepositoryUri) {
    $AccountId = (aws sts get-caller-identity --query "Account" --output text).Trim()
    $RepositoryUri = "$AccountId.dkr.ecr.$Region.amazonaws.com/$StackName-telegram-alert-repo"
}

$FullImageUri = "$RepositoryUri`:$ImageTag"
Write-Host "      Target Image URI: $FullImageUri" -ForegroundColor Green

# 3. Authenticate Docker with Amazon ECR
Write-Host "[2/4] Authenticating Docker with Amazon ECR in $Region..." -ForegroundColor Yellow
$EcrDomain = "$((aws sts get-caller-identity --query 'Account' --output text).Trim()).dkr.ecr.$Region.amazonaws.com"
aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $EcrDomain
if ($LASTEXITCODE -ne 0) {
    throw "ECR authentication failed with exit code $LASTEXITCODE"
}

# 4. Build and Push Docker image
Write-Host "[3/4] Building and pushing Docker image..." -ForegroundColor Yellow
docker build -t $FullImageUri $ScriptDir
if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed with exit code $LASTEXITCODE"
}

docker push $FullImageUri
if ($LASTEXITCODE -ne 0) {
    throw "Docker push failed with exit code $LASTEXITCODE"
}

# 5. Update Lambda function code if function already exists
Write-Host "[4/4] Updating AWS Lambda function code on '$FunctionName'..." -ForegroundColor Yellow
$FunctionExists = (aws lambda get-function --function-name $FunctionName --region $Region 2>&1)
if ($LASTEXITCODE -eq 0) {
    aws lambda update-function-code `
        --function-name $FunctionName `
        --image-uri $FullImageUri `
        --region $Region `
        --no-cli-pager | Out-Null

    Write-Host "      Waiting for Lambda function update to complete..." -ForegroundColor Yellow
    aws lambda wait function-updated `
        --function-name $FunctionName `
        --region $Region

    Write-Host "SUCCESS: Telegram Alert Lambda function '$FunctionName' updated to $FullImageUri" -ForegroundColor Green
} else {
    Write-Host "NOTE: Function '$FunctionName' does not exist yet; deploy CloudFormation stack to provision it." -ForegroundColor Cyan
    Write-Host "SUCCESS: Docker image pushed to $FullImageUri ready for CloudFormation stack creation." -ForegroundColor Green
}

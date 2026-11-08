<#
.SYNOPSIS
    Builds the Docker image, pushes it to Amazon ECR, and deploys the Quarkus Live Trading application to AWS EC2 in eu-west-1.

.DESCRIPTION
    1. Determines the Docker image version {x.y.z} from .\live\target\quarkus-app\app\live-{x.y.z}.jar.
    2. Resolves the Amazon ECR repository URI in the target region (default: eu-west-1).
    3. Authenticates Docker with Amazon ECR.
    4. Builds and pushes the AWS Graviton (linux/arm64) Docker image using Docker Buildx tagged with {x.y.z} and 'latest'.
    5. Deploys the updated container on the running 24/7 EC2 instance via AWS Systems Manager (SSM) without open SSH ports.

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

.EXAMPLE
    .\infra\deploy-live.ps1
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Region = "eu-west-1",

    [Parameter(Mandatory = $false)]
    [string]$BaseStackName = "polymarket-bot-live-base",

    [Parameter(Mandatory = $false)]
    [string]$Ec2StackName = "polymarket-bot-live-ec2",

    [Parameter(Mandatory = $false)]
    [string]$RepositoryUri = "",

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
Write-Host " Polymarket Bot: Build Image & Deploy Live Trading App    " -ForegroundColor Cyan
Write-Host " Region: $Region | Base: $BaseStackName | EC2: $Ec2StackName" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Determine version from .\live\target\quarkus-app\app\live-{x.y.z}.jar
Write-Host "[1/5] Resolving version from .\live\target\quarkus-app\app\live-{x.y.z}.jar..." -ForegroundColor Yellow
$AppDir = Join-Path $RepoRoot "live\target\quarkus-app\app"
if (-not (Test-Path $AppDir)) {
    throw "Directory '$AppDir' not found. Run 'mvnw release:clean release:prepare' before deploying."
}

$JarFiles = @(Get-ChildItem -Path $AppDir -Filter "live-*.jar" -File)
if ($JarFiles.Count -eq 0) {
    throw "No 'live-*.jar' found in '$AppDir'. Run 'mvnw release:clean release:prepare' before deploying."
}
if ($JarFiles.Count -gt 1) {
    throw "Multiple 'live-*.jar' files found in '$AppDir': $($JarFiles.Name -join ', '). Expected a single release JAR."
}

$JarName = $JarFiles[0].Name
if ($JarName -notmatch '^live-(\d+\.\d+\.\d+)\.jar$') {
    throw "JAR file '$JarName' does not match expected release format 'live-{x.y.z}.jar' (e.g. live-1.0.13.jar). Run 'mvnw release:clean release:prepare' first."
}
$ReleaseVersion = $matches[1]
Write-Host "      Detected artifact: $JarName (Version: $ReleaseVersion)" -ForegroundColor Green

# 2. Resolve Amazon ECR Repository URI
if (-not $RepositoryUri) {
    Write-Host "[2/5] Resolving Amazon ECR repository URI..." -ForegroundColor Yellow
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

# 3. Authenticate Docker with Amazon ECR
Write-Host "[3/5] Authenticating Docker with Amazon ECR in $Region..." -ForegroundColor Yellow
$EcrDomain = ($RepositoryUri -split '/')[0]

# Ensure Docker config does not fail due to Windows Credential Manager payload length limits on ECR tokens
$DockerConfigPath = Join-Path $env:USERPROFILE ".docker\config.json"
if (Test-Path $DockerConfigPath) {
    try {
        $dockerConfig = Get-Content $DockerConfigPath -Raw | ConvertFrom-Json
        if ($dockerConfig.credsStore -in @("desktop", "wincred")) {
            Write-Host "      Removing '$($dockerConfig.credsStore)' from credsStore to prevent Windows Credential Manager token buffer overflow..." -ForegroundColor DarkGray
            $dockerConfig.PSObject.Properties.Remove("credsStore")
            $dockerConfig | ConvertTo-Json -Depth 10 | Set-Content $DockerConfigPath
        }
    } catch {
        # Proceed with standard login attempt
    }
}

$ecrPassword = (aws ecr get-login-password --region $Region).Trim()
$ecrPassword | docker login --username AWS --password-stdin $EcrDomain
if ($LASTEXITCODE -ne 0) {
    # Direct fallback: write basic auth token directly to config.json
    Write-Host "      Standard docker login failed; falling back to direct auth token injection..." -ForegroundColor Yellow
    try {
        $authBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("AWS:$ecrPassword"))
        $dockerConfig = Get-Content $DockerConfigPath -Raw | ConvertFrom-Json
        if (-not $dockerConfig.auths) {
            $dockerConfig | Add-Member -Name "auths" -Value ([PSCustomObject]@{}) -MemberType NoteProperty -Force
        }
        $dockerConfig.auths | Add-Member -Name $EcrDomain -Value ([PSCustomObject]@{ auth = $authBase64 }) -MemberType NoteProperty -Force
        $dockerConfig.PSObject.Properties.Remove("credsStore")
        $dockerConfig | ConvertTo-Json -Depth 10 | Set-Content $DockerConfigPath
        Write-Host "      Direct auth token configured successfully for $EcrDomain." -ForegroundColor Green
    } catch {
        throw "ECR authentication failed with exit code $LASTEXITCODE"
    }
}

# 4. Build and Push Multi-Platform ARM64 Docker Images (Docker Buildx)
Write-Host "[4/5] Building and pushing linux/arm64 Docker image via Buildx..." -ForegroundColor Yellow
$VersionedUri = "$RepositoryUri`:$ReleaseVersion"
$LatestUri = "$RepositoryUri`:latest"

Push-Location $RepoRoot
try {
    docker buildx build --platform linux/arm64 --provenance=false `
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

# 5. Deploy Container on EC2 via AWS Systems Manager (SSM)
Write-Host "[5/5] Deploying container on EC2 instance via AWS Systems Manager..." -ForegroundColor Yellow
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

$Commands = @(
    "set -euxo pipefail",
    "export AWS_REGION=$Region",
    'ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)',
    "aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin `"`$ACCOUNT_ID.dkr.ecr.$Region.amazonaws.com`"",
    "docker pull $VersionedUri",
    "sudo /usr/local/bin/start-polymarket-live.sh `"$VersionedUri`"",
    "sleep 3",
    "docker ps --filter name=polymarket-live"
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
        --comment "Deploy Polymarket Live Trading $ReleaseVersion" `
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

# Telegram Alert Lambda Deployment (`infra/telegram_alert`)

## Architecture Overview
- **Base Stack (`infra/telegram-alert-base.yaml`)**: Provisions ECR repository `polymarket-bot-alert-base-telegram-alert-repo` and least-privilege `TelegramAlertRole` with Secrets Manager access to `polymarket-sniper/telegram-credentials`.
- **Container Build (`infra/deploy-telegram-alert.ps1`)**: Runs Maven release in `infra/telegram_alert`, builds the Java 25 shaded JAR Docker image tagged with SemVer + `latest`, pushes to ECR, and updates Lambda function code automatically if already deployed.
- **Function Stack (`infra/telegram-alert-lambda.yaml`)**: Provisions `AWS::Lambda::Function` and attaches `AWS::Logs::SubscriptionFilter` to the unified **Polymarket Bot** CloudWatch Log Group (`/polymarket-bot/polymarket-bot-base/bot`) matching `[FATAL]`, `[ERROR]`, and `*Exception` (`%\[FATAL\]|\[ERROR\]|\w*Exception\W|\w*Exception$%`; `[WARN]` parsing remains in Lambda code for optional reactivation).

---

## Step 1: Provision Alert Base Stack

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-alert-base `
  --template-file infra/telegram-alert-base.yaml `
  --capabilities CAPABILITY_NAMED_IAM
```

## Step 2: Build & Push Container Image (and Update Existing Function)

```powershell
# Auto-increment patch version:
.\infra\deploy-telegram-alert.ps1

# Or explicit SemVer version:
.\infra\deploy-telegram-alert.ps1 -Version "1.0.3"
```

## Step 3: Provision Alert Lambda & Log Subscription Filter

```powershell
aws cloudformation deploy `
  --region eu-west-1 `
  --stack-name polymarket-bot-alert-lambda `
  --template-file infra/telegram-alert-lambda.yaml `
  --parameter-overrides `
      AlertBaseStackName="polymarket-bot-alert-base" `
      BotBaseStackName="polymarket-bot-base" `
      ImageTag="latest"
```

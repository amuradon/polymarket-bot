# Multi-stage build for Polymarket Bot Telegram Alert AWS Lambda (Java 25)
FROM public.ecr.aws/amazoncorretto/amazoncorretto:25 AS runtime

WORKDIR /var/task

# Copy the shaded uber-jar containing all dependencies and the AWS Lambda Runtime Interface Client (RIC)
COPY target/telegram-alert-1.0.0.jar ./telegram-alert.jar

# AWS Lambda Runtime Interface Client bootstrap
ENTRYPOINT [ "/usr/bin/java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=80.0", "-cp", "./telegram-alert.jar", "com.amazonaws.services.lambda.runtime.api.client.AWSLambda" ]

# Default Lambda handler
CMD [ "cz.polymarket.bot.alert.TelegramAlertLambdaHandler::handleRequest" ]

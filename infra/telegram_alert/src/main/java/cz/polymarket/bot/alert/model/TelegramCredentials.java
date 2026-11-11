package cz.polymarket.bot.alert.model;

/**
 * Validated Telegram Bot authentication credentials resolved from AWS Secrets Manager.
 *
 * @param botToken Telegram Bot API token
 * @param chatId   target Telegram chat or channel ID
 */
public record TelegramCredentials(String botToken, String chatId) {
    public TelegramCredentials {
        if (botToken == null || botToken.isBlank()) {
            throw new IllegalArgumentException("botToken must not be blank");
        }
        if (chatId == null || chatId.isBlank()) {
            throw new IllegalArgumentException("chatId must not be blank");
        }
    }
}

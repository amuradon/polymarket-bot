package cz.polymarket.bot.web;

/**
 * Request payload for {@code POST /api/mode} switching the runtime {@code StrategyMode}.
 *
 * @param mode target mode string ({@code "LIVE"}, {@code "PAPER"}, or {@code "OFF"})
 */
public record ModeSwitchRequestDto(String mode) {
}

package cz.polymarket.bot.live.web;

import cz.polymarket.bot.live.engine.LivePosition;

import java.util.List;

/**
 * Diagnostic status DTO for the live trading engine and active Polymarket market.
 *
 * @param strategyName     active strategy name (e.g. {@code UtterParameciumStrategy})
 * @param status           engine execution mode ({@code "LIVE"})
 * @param orderType        configured CLOB order time-in-force (e.g. {@code "FAK"})
 * @param activeMarketSlug currently tracked Polymarket 15m market slug
 * @param yesTokenId       Up (YES) conditional token ID for the active market
 * @param noTokenId        Down (NO) conditional token ID for the active market
 * @param initialCapital   initial live bankroll in USD
 * @param currentBalance   current live balance in USD
 * @param activePosition   most recent active position, or {@code null}
 * @param openPositions    list of all open positions in the active candle
 * @param telemetry        system, 4-core thread, order execution, and WebSocket stream telemetry snapshot
 */
public record LiveStatusDto(
        String strategyName,
        String status,
        String orderType,
        String activeMarketSlug,
        String yesTokenId,
        String noTokenId,
        double initialCapital,
        double currentBalance,
        LivePosition activePosition,
        List<LivePosition> openPositions,
        LiveTelemetryDto telemetry
) {
}

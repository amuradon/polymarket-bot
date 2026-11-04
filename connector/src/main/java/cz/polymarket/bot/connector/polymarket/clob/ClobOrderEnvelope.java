package cz.polymarket.bot.connector.polymarket.clob;

/**
 * Top-level Polymarket CLOB {@code POST /order} JSON request envelope.
 *
 * @param order     signed {@link ClobOrderPayload}
 * @param owner     Polymarket CLOB API key UUID
 * @param orderType order time-in-force ({@code "FAK"}, {@code "FOK"}, or {@code "GTC"})
 * @param deferExec whether to defer execution ({@code false} for immediate execution)
 */
public record ClobOrderEnvelope(
        ClobOrderPayload order,
        String owner,
        String orderType,
        boolean deferExec
) {
}

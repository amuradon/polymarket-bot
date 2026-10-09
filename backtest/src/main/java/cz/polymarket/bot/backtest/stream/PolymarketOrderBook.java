package cz.polymarket.bot.backtest.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.domain.OrderBookQuote;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Reconstructed L2 Order Book for Polymarket binary options.
 * The order book tracks the UP (YES) token; DOWN (NO) depth is derived
 * via binary complementarity P(DOWN) = 1.0 - P(UP).
 */
public class PolymarketOrderBook {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NavigableMap<Double, Double> bids = new TreeMap<>(Collections.reverseOrder());
    private final NavigableMap<Double, Double> asks = new TreeMap<>();
    private double lastTradedPrice = 0.50;
    private long lastUpdateTimeMs = 0L;

    /**
     * Clears all bids, asks, and trading state in the book.
     */
    public void clear() {
        bids.clear();
        asks.clear();
        lastTradedPrice = 0.50;
        lastUpdateTimeMs = 0L;
    }

    /**
     * Replaces the entire book depth from raw JSON snapshot strings.
     *
     * @param bidsJson JSON array string of bid price/size objects
     * @param asksJson JSON array string of ask price/size objects
     * @param timestampMs snapshot timestamp in milliseconds UTC
     */
    public void applySnapshot(String bidsJson, String asksJson, long timestampMs) {
        bids.clear();
        asks.clear();
        this.lastUpdateTimeMs = timestampMs;

        parseBookLevels(bidsJson, bids);
        parseBookLevels(asksJson, asks);
    }

    /**
     * Applies an incremental Level 2 delta update to the book.
     *
     * @param side order side ("BUY" modifies bids, "SELL" modifies asks)
     * @param price price level
     * @param size updated depth size (0 removes level)
     * @param timestampMs delta update timestamp in milliseconds UTC
     */
    public void applyDelta(String side, double price, double size, long timestampMs) {
        this.lastUpdateTimeMs = timestampMs;
        if ("BUY".equalsIgnoreCase(side)) {
            if (size <= 0.0) {
                bids.remove(price);
            } else {
                bids.put(price, size);
            }
        } else if ("SELL".equalsIgnoreCase(side)) {
            if (size <= 0.0) {
                asks.remove(price);
            } else {
                asks.put(price, size);
            }
        }
    }

    /**
     * Applies a reported trade execution to update the last traded price.
     *
     * @param price execution price
     * @param size trade size
     * @param side aggressor side
     * @param timestampMs trade timestamp in milliseconds UTC
     */
    public void applyTrade(double price, double size, String side, long timestampMs) {
        this.lastUpdateTimeMs = timestampMs;
        if (price > 0.0 && price < 1.0) {
            this.lastTradedPrice = price;
        }
    }

    /**
     * Overwrites book top-of-book levels from a discrete {@link OrderBookQuote}.
     *
     * @param quote discrete quote instance
     */
    public void applyQuote(OrderBookQuote quote) {
        if (quote == null) {
            return;
        }
        this.lastUpdateTimeMs = quote.timestampMs();
        this.bids.clear();
        this.asks.clear();
        if (quote.bestBidUp() > 0.0) {
            this.bids.put(quote.bestBidUp(), quote.depth1cDown() > 0.0 ? quote.depth1cDown() : 300.0);
        }
        if (quote.bestAskUp() > 0.0) {
            this.asks.put(quote.bestAskUp(), quote.depth1cUp() > 0.0 ? quote.depth1cUp() : 300.0);
        }
    }

    /**
     * Returns best bid price for UP (YES) token.
     */
    public double getBestBidUp() {
        return bids.isEmpty() ? 0.49 : bids.firstKey();
    }

    /**
     * Returns best ask price for UP (YES) token.
     */
    public double getBestAskUp() {
        return asks.isEmpty() ? 0.50 : asks.firstKey();
    }

    /**
     * Returns derived best bid price for DOWN (NO) token via complementarity: P_bid_down = 1 - P_ask_up.
     */
    public double getBestBidDown() {
        return Math.max(0.001, 1.0 - getBestAskUp());
    }

    /**
     * Returns derived best ask price for DOWN (NO) token via complementarity: P_ask_down = 1 - P_bid_up.
     */
    public double getBestAskDown() {
        return Math.max(0.001, 1.0 - getBestBidUp());
    }

    /**
     * Calculates cumulative liquidity depth within 1 cent of best ask for UP.
     */
    public double getDepth1cUp() {
        double bestAsk = getBestAskUp();
        double sum = 0.0;
        for (Map.Entry<Double, Double> e : asks.entrySet()) {
            if (e.getKey() <= bestAsk + 0.01) {
                sum += e.getValue();
            } else {
                break;
            }
        }
        return sum > 0.0 ? sum : 300.0;
    }

    /**
     * Calculates cumulative liquidity depth within 1 cent of best bid for UP.
     */
    public double getDepth1cDown() {
        double bestBid = getBestBidUp();
        double sum = 0.0;
        for (Map.Entry<Double, Double> e : bids.entrySet()) {
            if (e.getKey() >= bestBid - 0.01) {
                sum += e.getValue();
            } else {
                break;
            }
        }
        return sum > 0.0 ? sum : 300.0;
    }

    /**
     * Converts current book state into a unified {@link OrderBookQuote} snapshot.
     *
     * @param timestampMs current clock timestamp in milliseconds UTC
     * @return constructed OrderBookQuote
     */
    public OrderBookQuote toOrderBookQuote(long timestampMs) {
        double bAskUp = getBestAskUp();
        double bBidUp = getBestBidUp();
        double bAskDown = getBestAskDown();
        double bBidDown = getBestBidDown();
        double depthUp = getDepth1cUp();
        double depthDown = getDepth1cDown();

        FillResult fillUp = simulateFill(true, true, 100.0, 1.0);
        FillResult fillDown = simulateFill(false, true, 100.0, 1.0);

        return new OrderBookQuote(
                bAskUp,
                bBidUp,
                bAskDown,
                bBidDown,
                depthUp,
                depthDown,
                fillUp.avgPrice() > 0 ? fillUp.avgPrice() : bAskUp,
                fillDown.avgPrice() > 0 ? fillDown.avgPrice() : bAskDown,
                timestampMs > 0 ? timestampMs : lastUpdateTimeMs
        );
    }

    /**
     * Simulates order execution against current book depth.
     *
     * @param isUpToken true for UP contract, false for DOWN contract
     * @param isBuy true for BUY (taker buying), false for SELL (taker selling)
     * @param requestedShares desired volume
     * @param limitPrice maximum price for BUY or minimum price for SELL
     * @return execution details including average price, executed shares, and fee
     */
    public FillResult simulateFill(boolean isUpToken, boolean isBuy, double requestedShares, double limitPrice) {
        if (requestedShares <= 0.0) {
            return new FillResult(0.0, 0.0, 0.0);
        }

        double remainingShares = requestedShares;
        double cost = 0.0;
        double executedShares = 0.0;

        if (isUpToken) {
            if (isBuy) {
                // Taker BUY UP matches against asks
                for (Map.Entry<Double, Double> entry : asks.entrySet()) {
                    double price = entry.getKey();
                    double available = entry.getValue();
                    if (price > limitPrice) {
                        break;
                    }
                    double take = Math.min(remainingShares, available);
                    cost += take * price;
                    executedShares += take;
                    remainingShares -= take;
                    if (remainingShares <= 0) break;
                }
            } else {
                // Taker SELL UP matches against bids
                for (Map.Entry<Double, Double> entry : bids.entrySet()) {
                    double price = entry.getKey();
                    double available = entry.getValue();
                    if (price < limitPrice) {
                        break;
                    }
                    double take = Math.min(remainingShares, available);
                    cost += take * price;
                    executedShares += take;
                    remainingShares -= take;
                    if (remainingShares <= 0) break;
                }
            }
        } else {
            // DOWN Token
            if (isBuy) {
                // Taker BUY DOWN matches against UP bids (P_down_ask = 1 - P_up_bid)
                for (Map.Entry<Double, Double> entry : bids.entrySet()) {
                    double downPrice = Math.max(0.001, 1.0 - entry.getKey());
                    double available = entry.getValue();
                    if (downPrice > limitPrice) {
                        break;
                    }
                    double take = Math.min(remainingShares, available);
                    cost += take * downPrice;
                    executedShares += take;
                    remainingShares -= take;
                    if (remainingShares <= 0) break;
                }
            } else {
                // Taker SELL DOWN matches against UP asks (P_down_bid = 1 - P_up_ask)
                for (Map.Entry<Double, Double> entry : asks.entrySet()) {
                    double downPrice = Math.max(0.001, 1.0 - entry.getKey());
                    double available = entry.getValue();
                    if (downPrice < limitPrice) {
                        break;
                    }
                    double take = Math.min(remainingShares, available);
                    cost += take * downPrice;
                    executedShares += take;
                    remainingShares -= take;
                    if (remainingShares <= 0) break;
                }
            }
        }

        // If not enough liquidity in L2 snapshots, fallback to best price for remaining
        if (executedShares < requestedShares) {
            double fallbackPrice = isUpToken
                    ? (isBuy ? getBestAskUp() : getBestBidUp())
                    : (isBuy ? getBestAskDown() : getBestBidDown());
            double remaining = requestedShares - executedShares;
            cost += remaining * fallbackPrice;
            executedShares = requestedShares;
        }

        double avgPrice = executedShares > 0 ? (cost / executedShares) : 0.0;
        double fee = executedShares * 0.07 * avgPrice * Math.max(0.0, 1.0 - avgPrice);

        return new FillResult(avgPrice, executedShares, fee);
    }

    /**
     * Parses raw JSON array string into navigable price-to-size map levels.
     */
    private void parseBookLevels(String json, NavigableMap<Double, Double> map) {
        if (json == null || json.isBlank() || json.equals("null") || json.equals("[]")) {
            return;
        }
        try {
            JsonNode array = MAPPER.readTree(json);
            if (array.isArray()) {
                for (JsonNode item : array) {
                    JsonNode pNode = item.get("price");
                    JsonNode sNode = item.get("size");
                    if (pNode != null && sNode != null) {
                        double p = Double.parseDouble(pNode.asText());
                        double s = Double.parseDouble(sNode.asText());
                        if (s > 0.0) {
                            map.put(p, s);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Execution outcome of a simulated book sweep order.
     *
     * @param avgPrice volume-weighted average fill price
     * @param executedShares total executed contract shares
     * @param fee calculated taker fee in USD
     */
    public record FillResult(double avgPrice, double executedShares, double fee) {}
}

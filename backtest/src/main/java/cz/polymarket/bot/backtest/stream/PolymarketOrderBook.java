package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.OrderBookQuote;
import io.quarkus.logging.Log;

import java.util.Arrays;
import java.util.Locale;

/**
 * High-performance, zero-allocation L2 Order Book for Polymarket binary options.
 * <p>
 * Uses flat primitive tick arrays ({@code 0..10000} representing prices {@code 0.0000..1.0000}
 * at 1-bps resolution) and a custom allocation-free JSON character scanner to process
 * millions of collector {@code clob}, {@code book}, {@code price_change}, and
 * {@code last_trade_price} events without boxing or heap tree allocations.
 * <p>
 * Simultaneously tracks the real-time one-way network/processing latency
 * ({@code ts - serverTimestampMs}) observed on incoming Polymarket CLOB messages.
 */
public class PolymarketOrderBook {

    private static final int MAX_TICK = 10_000;
    private static final double TICK_SCALE = 10_000.0;
    private static final double TICK_INV = 0.0001;
    private static final int ONE_CENT_TICKS = 100;
    private static final double EPS = 1e-9;
    private static final long DEFAULT_INITIAL_ONE_WAY_LATENCY_MS = 10L;

    private final double[] upBids = new double[MAX_TICK + 1];
    private final double[] upAsks = new double[MAX_TICK + 1];
    private final double[] upBidsConsumed = new double[MAX_TICK + 1];
    private final double[] upAsksConsumed = new double[MAX_TICK + 1];
    private int bestUpBidTick = -1;
    private int bestUpAskTick = MAX_TICK + 1;

    private final double[] downBids = new double[MAX_TICK + 1];
    private final double[] downAsks = new double[MAX_TICK + 1];
    private final double[] downBidsConsumed = new double[MAX_TICK + 1];
    private final double[] downAsksConsumed = new double[MAX_TICK + 1];
    private int bestDownBidTick = -1;
    private int bestDownAskTick = MAX_TICK + 1;

    private String upTokenId = "BTC-UP";
    private String downTokenId = "BTC-DOWN";
    private long lastUpdateTimeMs = 0L;
    private long lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;

    /**
     * Clears all bids, asks, consumed liquidity memory, and resets active candle book state.
     */
    public void clear() {
        clearUpBook(true);
        clearDownBook(true);
        lastUpdateTimeMs = 0L;
        lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;
    }

    /**
     * Configures the Polymarket CLOB token IDs for UP and DOWN outcomes for the active market.
     *
     * @param upTokenId token ID for the UP ("Up" / YES) outcome
     * @param downTokenId token ID for the DOWN ("Down" / NO) outcome
     */
    public void setMarketTokenIds(String upTokenId, String downTokenId) {
        this.upTokenId = (upTokenId != null && !upTokenId.isBlank()) ? upTokenId : "BTC-UP";
        this.downTokenId = (downTokenId != null && !downTokenId.isBlank()) ? downTokenId : "BTC-DOWN";
    }

    /**
     * Determines whether a token identifier corresponds to the UP outcome.
     *
     * @param token token identifier or symbolic name
     * @return true if UP token, false if DOWN token
     */
    public boolean isUpToken(String token) {
        if (token == null || token.isBlank()) {
            return true;
        }
        if (token.equals(upTokenId)) {
            return true;
        }
        if (token.equals(downTokenId)) {
            return false;
        }
        return !token.toLowerCase(Locale.ROOT).contains("down");
    }

    /**
     * Returns the most recently observed one-way latency in milliseconds between the
     * Polymarket server timestamp and the local EC2 receipt timestamp ({@code ts}).
     *
     * @return observed one-way latency in milliseconds (at least 1ms)
     */
    public long getLastObservedOneWayLatencyMs() {
        return lastObservedOneWayLatencyMs;
    }

    /**
     * Explicitly updates the observed one-way latency in milliseconds.
     *
     * @param oneWayLatencyMs observed one-way latency in milliseconds
     */
    public void setLastObservedOneWayLatencyMs(long oneWayLatencyMs) {
        this.lastObservedOneWayLatencyMs = Math.max(1L, oneWayLatencyMs);
    }

    /**
     * Updates observed one-way latency from receipt timestamp and server payload timestamp.
     *
     * @param receiptTimestampMs local EC2 capture timestamp in milliseconds UTC
     * @param serverTimestampMs exchange server timestamp in milliseconds UTC
     */
    public void updateObservedLatency(long receiptTimestampMs, long serverTimestampMs) {
        if (receiptTimestampMs > 0L && serverTimestampMs > 0L && receiptTimestampMs >= serverTimestampMs) {
            long diff = receiptTimestampMs - serverTimestampMs;
            if (diff <= 60_000L) {
                this.lastObservedOneWayLatencyMs = Math.max(1L, diff);
            }
        }
    }

    /**
     * Processes a raw Polymarket collector event ({@code clob}, {@code book}, {@code price_change}, {@code last_trade_price})
     * using zero-allocation character scanning.
     *
     * @param event collector CLOB event
     */
    public void applyCollectorEvent(BacktestEvent.PolymarketClobEvent event) {
        if (event == null) {
            return;
        }
        long ts = event.timestampMs();
        this.lastUpdateTimeMs = ts;
        String et = event.eventType();
        String payload = event.payload();
        if (et == null) {
            return;
        }

        switch (et) {
            case "price_change" -> applyPriceChangePayload(payload, ts);
            case "book" -> {
                if (payload != null && !payload.isEmpty()) {
                    applyBookObject(payload, 0, payload.length(), ts);
                }
            }
            case "clob" -> applyClobSnapshotArray(payload, ts);
            case "last_trade_price" -> {
                long serverTs = extractServerTimestampMs(payload, 0, payload != null ? payload.length() : 0);
                updateObservedLatency(ts, serverTs);
                applyTrade(event.price(), event.size(), event.side(), ts);
            }
            default -> {
                long serverTs = extractServerTimestampMs(payload, 0, payload != null ? payload.length() : 0);
                updateObservedLatency(ts, serverTs);
            }
        }
    }

    /**
     * Replaces the UP book depth from raw JSON snapshot strings while preserving consumed liquidity
     * memory for levels that remain non-empty in the snapshot.
     *
     * @param bidsJson JSON array string of bid price/size objects
     * @param asksJson JSON array string of ask price/size objects
     * @param timestampMs snapshot timestamp in milliseconds UTC
     */
    public void applySnapshot(String bidsJson, String asksJson, long timestampMs) {
        clearUpBook(false);
        this.lastUpdateTimeMs = timestampMs;

        if (bidsJson != null && !bidsJson.isEmpty()) {
            parseBookLevelsSlice(bidsJson, 0, bidsJson.length(), true, true);
        }
        if (asksJson != null && !asksJson.isEmpty()) {
            parseBookLevelsSlice(asksJson, 0, asksJson.length(), true, false);
        }
        reconcileBookAfterSnapshot(true);
    }

    /**
     * Applies an incremental Level 2 delta update to the UP book.
     *
     * @param side order side ("BUY" modifies bids, "SELL" modifies asks)
     * @param price price level
     * @param size updated depth size (0 removes level and resets consumed shares at that level)
     * @param timestampMs delta update timestamp in milliseconds UTC
     */
    public void applyDelta(String side, double price, double size, long timestampMs) {
        boolean isBuy = "BUY".equalsIgnoreCase(side);
        if (!isBuy && !"SELL".equalsIgnoreCase(side)) {
            this.lastUpdateTimeMs = timestampMs;
            return;
        }
        applyDeltaForToken(true, isBuy, price, size, timestampMs);
    }

    /**
     * Applies an incremental Level 2 delta update to either the UP or DOWN token book.
     * <p>
     * If {@code size <= 0}, the level completely disappears from the book and its consumed
     * shares memory is reset to {@code 0}. If {@code size < consumed}, consumed shares are
     * clamped to {@code size} so subsequent maker liquidity additions become available.
     *
     * @param isUp true for UP token book, false for DOWN token book
     * @param isBuy true for BUY (bids), false for SELL (asks)
     * @param price price level in (0.0, 1.0)
     * @param size updated depth size (0 removes level)
     * @param timestampMs delta update timestamp in milliseconds UTC
     */
    public void applyDeltaForToken(boolean isUp, boolean isBuy, double price, double size, long timestampMs) {
        this.lastUpdateTimeMs = timestampMs;
        int tick = priceToTick(price);
        if (tick < 0 || tick > MAX_TICK) {
            return;
        }
        double cleanSize = size > 0.0 ? size : 0.0;
        if (isUp) {
            if (isBuy) {
                setBidLevel(upBids, upBidsConsumed, tick, cleanSize, true);
            } else {
                setAskLevel(upAsks, upAsksConsumed, tick, cleanSize, true);
            }
        } else {
            if (isBuy) {
                setBidLevel(downBids, downBidsConsumed, tick, cleanSize, false);
            } else {
                setAskLevel(downAsks, downAsksConsumed, tick, cleanSize, false);
            }
        }
    }

    /**
     * Applies a reported trade execution to update the last update timestamp.
     *
     * @param price execution price
     * @param size trade size
     * @param side aggressor side
     * @param timestampMs trade timestamp in milliseconds UTC
     */
    public void applyTrade(double price, double size, String side, long timestampMs) {
        this.lastUpdateTimeMs = timestampMs;
    }

    /**
     * Returns best bid price for UP (YES) token, or {@link Double#NaN} when the book has no bids.
     *
     * @return best UP bid price
     */
    public double getBestBidUp() {
        return bestUpBidTick >= 0 ? bestUpBidTick * TICK_INV : Double.NaN;
    }

    /**
     * Returns best ask price for UP (YES) token, or {@link Double#NaN} when the book has no asks.
     *
     * @return best UP ask price
     */
    public double getBestAskUp() {
        return bestUpAskTick <= MAX_TICK ? bestUpAskTick * TICK_INV : Double.NaN;
    }

    /**
     * Returns best bid price for DOWN (NO) token from explicit DOWN book if populated,
     * or derived via binary complementarity {@code P_bid_down = 1 - P_ask_up}.
     *
     * @return best DOWN bid price, or {@link Double#NaN}
     */
    public double getBestBidDown() {
        if (bestDownBidTick >= 0) {
            return bestDownBidTick * TICK_INV;
        }
        return bestUpAskTick <= MAX_TICK
                ? Math.max(0.001, (MAX_TICK - bestUpAskTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Returns best ask price for DOWN (NO) token from explicit DOWN book if populated,
     * or derived via binary complementarity {@code P_ask_down = 1 - P_bid_up}.
     *
     * @return best DOWN ask price, or {@link Double#NaN}
     */
    public double getBestAskDown() {
        if (bestDownAskTick <= MAX_TICK) {
            return bestDownAskTick * TICK_INV;
        }
        return bestUpBidTick >= 0
                ? Math.max(0.001, (MAX_TICK - bestUpBidTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Calculates cumulative effective liquidity depth within 1 cent of best ask for UP
     * after subtracting strategy-consumed shares.
     *
     * @return 1-cent effective ask depth for UP
     */
    public double getDepth1cUp() {
        if (bestUpAskTick > MAX_TICK) {
            return 0.0;
        }
        int limitTick = Math.min(MAX_TICK, bestUpAskTick + ONE_CENT_TICKS);
        double sum = 0.0;
        for (int t = bestUpAskTick; t <= limitTick; t++) {
            double eff = upAsks[t] - upAsksConsumed[t];
            if (eff > EPS) {
                sum += eff;
            }
        }
        return sum;
    }

    /**
     * Calculates cumulative effective liquidity depth within 1 cent of best ask for DOWN
     * (or within 1 cent of best bid for UP via complementarity when DOWN book is not explicitly populated)
     * after subtracting strategy-consumed shares.
     *
     * @return 1-cent effective depth for DOWN
     */
    public double getDepth1cDown() {
        if (bestDownAskTick <= MAX_TICK) {
            int limitTick = Math.min(MAX_TICK, bestDownAskTick + ONE_CENT_TICKS);
            double sum = 0.0;
            for (int t = bestDownAskTick; t <= limitTick; t++) {
                double eff = downAsks[t] - downAsksConsumed[t];
                if (eff > EPS) {
                    sum += eff;
                }
            }
            return sum;
        }
        if (bestUpBidTick < 0) {
            return 0.0;
        }
        int limitTick = Math.max(0, bestUpBidTick - ONE_CENT_TICKS);
        double sum = 0.0;
        for (int t = bestUpBidTick; t >= limitTick; t--) {
            double eff = upBids[t] - upBidsConsumed[t];
            if (eff > EPS) {
                sum += eff;
            }
        }
        return sum;
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
     * Read-only simulation of order execution against current effective book depth without mutating
     * consumed liquidity arrays (used for quote slippage inspection).
     *
     * @param isUpToken true for UP contract, false for DOWN contract
     * @param isBuy true for BUY (taker buying), false for SELL (taker selling)
     * @param requestedShares desired volume
     * @param limitPrice maximum price for BUY or minimum price for SELL
     * @return execution details including average price, executed shares, and fee
     */
    public FillResult simulateFill(boolean isUpToken, boolean isBuy, double requestedShares, double limitPrice) {
        return fillOrder(isUpToken, isBuy, requestedShares, limitPrice, false);
    }

    /**
     * Executes an order against current effective book depth and depletes available liquidity at
     * every swept price level (including complementary UP/DOWN mirror levels).
     *
     * @param isUpToken true for UP contract, false for DOWN contract
     * @param isBuy true for BUY (taker buying), false for SELL (taker selling)
     * @param requestedShares desired volume
     * @param limitPrice maximum price for BUY or minimum price for SELL
     * @return execution details including average price, executed shares, and fee
     */
    public FillResult executeFill(boolean isUpToken, boolean isBuy, double requestedShares, double limitPrice) {
        return fillOrder(isUpToken, isBuy, requestedShares, limitPrice, true);
    }

    private FillResult fillOrder(
            boolean isUpToken,
            boolean isBuy,
            double requestedShares,
            double limitPrice,
            boolean consumeLiquidity) {

        if (requestedShares <= 0.0) {
            return new FillResult(0.0, 0.0, 0.0);
        }

        double remainingShares = requestedShares;
        double cost = 0.0;
        double executedShares = 0.0;
        int limitTick = priceToTick(limitPrice);

        if (isUpToken) {
            if (isBuy) {
                if (bestUpAskTick <= MAX_TICK) {
                    int maxTick = Math.min(MAX_TICK, limitTick);
                    for (int t = bestUpAskTick; t <= maxTick; t++) {
                        double available = upAsks[t] - upAsksConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * (t * TICK_INV);
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            upAsksConsumed[t] += take;
                            int compTick = MAX_TICK - t;
                            if (downBids[compTick] > 0.0) {
                                downBidsConsumed[compTick] = Math.min(downBids[compTick], downBidsConsumed[compTick] + take);
                            }
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestUpAsk();
                        advanceBestDownBid();
                    }
                }
            } else {
                if (bestUpBidTick >= 0) {
                    int minTick = Math.max(0, limitTick);
                    for (int t = bestUpBidTick; t >= minTick; t--) {
                        double available = upBids[t] - upBidsConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * (t * TICK_INV);
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            upBidsConsumed[t] += take;
                            int compTick = MAX_TICK - t;
                            if (downAsks[compTick] > 0.0) {
                                downAsksConsumed[compTick] = Math.min(downAsks[compTick], downAsksConsumed[compTick] + take);
                            }
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestUpBid();
                        advanceBestDownAsk();
                    }
                }
            }
        } else {
            if (isBuy) {
                if (bestDownAskTick <= MAX_TICK) {
                    int maxTick = Math.min(MAX_TICK, limitTick);
                    for (int t = bestDownAskTick; t <= maxTick; t++) {
                        double available = downAsks[t] - downAsksConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * (t * TICK_INV);
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            downAsksConsumed[t] += take;
                            int compTick = MAX_TICK - t;
                            if (upBids[compTick] > 0.0) {
                                upBidsConsumed[compTick] = Math.min(upBids[compTick], upBidsConsumed[compTick] + take);
                            }
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestDownAsk();
                        advanceBestUpBid();
                    }
                } else if (bestUpBidTick >= 0) {
                    for (int t = bestUpBidTick; t >= 0; t--) {
                        double available = upBids[t] - upBidsConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double downPrice = Math.max(0.001, (MAX_TICK - t) * TICK_INV);
                        if (downPrice > limitPrice + EPS) {
                            break;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * downPrice;
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            upBidsConsumed[t] += take;
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestUpBid();
                    }
                }
            } else {
                if (bestDownBidTick >= 0) {
                    int minTick = Math.max(0, limitTick);
                    for (int t = bestDownBidTick; t >= minTick; t--) {
                        double available = downBids[t] - downBidsConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * (t * TICK_INV);
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            downBidsConsumed[t] += take;
                            int compTick = MAX_TICK - t;
                            if (upAsks[compTick] > 0.0) {
                                upAsksConsumed[compTick] = Math.min(upAsks[compTick], upAsksConsumed[compTick] + take);
                            }
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestDownBid();
                        advanceBestUpAsk();
                    }
                } else if (bestUpAskTick <= MAX_TICK) {
                    for (int t = bestUpAskTick; t <= MAX_TICK; t++) {
                        double available = upAsks[t] - upAsksConsumed[t];
                        if (available <= EPS) {
                            continue;
                        }
                        double downPrice = Math.max(0.001, (MAX_TICK - t) * TICK_INV);
                        if (downPrice + EPS < limitPrice) {
                            break;
                        }
                        double take = Math.min(remainingShares, available);
                        cost += take * downPrice;
                        executedShares += take;
                        remainingShares -= take;
                        if (consumeLiquidity) {
                            upAsksConsumed[t] += take;
                        }
                        if (remainingShares <= EPS) {
                            break;
                        }
                    }
                    if (consumeLiquidity) {
                        advanceBestUpAsk();
                    }
                }
            }
        }

        double avgPrice = executedShares > 0.0 ? (cost / executedShares) : 0.0;
        double fee = executedShares * 0.07 * avgPrice * Math.max(0.0, 1.0 - avgPrice);

        return new FillResult(avgPrice, executedShares, fee);
    }

    private void clearUpBook(boolean resetConsumed) {
        Arrays.fill(upBids, 0.0);
        Arrays.fill(upAsks, 0.0);
        if (resetConsumed) {
            Arrays.fill(upBidsConsumed, 0.0);
            Arrays.fill(upAsksConsumed, 0.0);
        }
        bestUpBidTick = -1;
        bestUpAskTick = MAX_TICK + 1;
    }

    private void clearDownBook(boolean resetConsumed) {
        Arrays.fill(downBids, 0.0);
        Arrays.fill(downAsks, 0.0);
        if (resetConsumed) {
            Arrays.fill(downBidsConsumed, 0.0);
            Arrays.fill(downAsksConsumed, 0.0);
        }
        bestDownBidTick = -1;
        bestDownAskTick = MAX_TICK + 1;
    }

    private void reconcileBookAfterSnapshot(boolean isUp) {
        if (isUp) {
            bestUpBidTick = -1;
            bestUpAskTick = MAX_TICK + 1;
            for (int t = 0; t <= MAX_TICK; t++) {
                double rawBid = upBids[t];
                if (rawBid <= 0.0) {
                    upBidsConsumed[t] = 0.0;
                } else {
                    if (rawBid < upBidsConsumed[t]) {
                        upBidsConsumed[t] = rawBid;
                    }
                    if (rawBid - upBidsConsumed[t] > EPS) {
                        bestUpBidTick = t;
                    }
                }

                double rawAsk = upAsks[t];
                if (rawAsk <= 0.0) {
                    upAsksConsumed[t] = 0.0;
                } else {
                    if (rawAsk < upAsksConsumed[t]) {
                        upAsksConsumed[t] = rawAsk;
                    }
                    if (bestUpAskTick > MAX_TICK && (rawAsk - upAsksConsumed[t]) > EPS) {
                        bestUpAskTick = t;
                    }
                }
            }
        } else {
            bestDownBidTick = -1;
            bestDownAskTick = MAX_TICK + 1;
            for (int t = 0; t <= MAX_TICK; t++) {
                double rawBid = downBids[t];
                if (rawBid <= 0.0) {
                    downBidsConsumed[t] = 0.0;
                } else {
                    if (rawBid < downBidsConsumed[t]) {
                        downBidsConsumed[t] = rawBid;
                    }
                    if (rawBid - downBidsConsumed[t] > EPS) {
                        bestDownBidTick = t;
                    }
                }

                double rawAsk = downAsks[t];
                if (rawAsk <= 0.0) {
                    downAsksConsumed[t] = 0.0;
                } else {
                    if (rawAsk < downAsksConsumed[t]) {
                        downAsksConsumed[t] = rawAsk;
                    }
                    if (bestDownAskTick > MAX_TICK && (rawAsk - downAsksConsumed[t]) > EPS) {
                        bestDownAskTick = t;
                    }
                }
            }
        }
    }

    private void advanceBestUpBid() {
        while (bestUpBidTick >= 0 && (upBids[bestUpBidTick] - upBidsConsumed[bestUpBidTick]) <= EPS) {
            bestUpBidTick--;
        }
    }

    private void advanceBestUpAsk() {
        while (bestUpAskTick <= MAX_TICK && (upAsks[bestUpAskTick] - upAsksConsumed[bestUpAskTick]) <= EPS) {
            bestUpAskTick++;
        }
    }

    private void advanceBestDownBid() {
        while (bestDownBidTick >= 0 && (downBids[bestDownBidTick] - downBidsConsumed[bestDownBidTick]) <= EPS) {
            bestDownBidTick--;
        }
    }

    private void advanceBestDownAsk() {
        while (bestDownAskTick <= MAX_TICK && (downAsks[bestDownAskTick] - downAsksConsumed[bestDownAskTick]) <= EPS) {
            bestDownAskTick++;
        }
    }

    private void setBidLevel(double[] bids, double[] consumed, int tick, double size, boolean isUp) {
        bids[tick] = size;
        if (size <= 0.0) {
            consumed[tick] = 0.0;
        } else if (size < consumed[tick]) {
            consumed[tick] = size;
        }
        double effective = size - consumed[tick];
        if (isUp) {
            if (effective > EPS) {
                if (tick > bestUpBidTick) {
                    bestUpBidTick = tick;
                }
            } else if (tick == bestUpBidTick) {
                advanceBestUpBid();
            }
        } else {
            if (effective > EPS) {
                if (tick > bestDownBidTick) {
                    bestDownBidTick = tick;
                }
            } else if (tick == bestDownBidTick) {
                advanceBestDownBid();
            }
        }
    }

    private void setAskLevel(double[] asks, double[] consumed, int tick, double size, boolean isUp) {
        asks[tick] = size;
        if (size <= 0.0) {
            consumed[tick] = 0.0;
        } else if (size < consumed[tick]) {
            consumed[tick] = size;
        }
        double effective = size - consumed[tick];
        if (isUp) {
            if (effective > EPS) {
                if (tick < bestUpAskTick) {
                    bestUpAskTick = tick;
                }
            } else if (tick == bestUpAskTick) {
                advanceBestUpAsk();
            }
        } else {
            if (effective > EPS) {
                if (tick < bestDownAskTick) {
                    bestDownAskTick = tick;
                }
            } else if (tick == bestDownAskTick) {
                advanceBestDownAsk();
            }
        }
    }

    private static int priceToTick(double price) {
        if (Double.isNaN(price) || price <= 0.0) {
            return 0;
        }
        if (price >= 1.0) {
            return MAX_TICK;
        }
        return (int) Math.round(price * TICK_SCALE);
    }

    private void applyClobSnapshotArray(String payload, long receiptTimestampMs) {
        if (payload == null || payload.length() <= 2) {
            return;
        }
        int len = payload.length();
        int i = 0;
        while (i < len) {
            int objStart = payload.indexOf('{', i);
            if (objStart < 0) {
                break;
            }
            int objEnd = findMatchingBrace(payload, objStart, len);
            if (objEnd < 0) {
                break;
            }
            applyBookObject(payload, objStart, objEnd + 1, receiptTimestampMs);
            i = objEnd + 1;
        }
    }

    private void applyBookObject(String payload, int objStart, int objEnd, long receiptTimestampMs) {
        long serverTs = extractServerTimestampMs(payload, objStart, objEnd);
        updateObservedLatency(receiptTimestampMs, serverTs);

        boolean isUp = extractAndCheckIsUpAsset(payload, objStart, objEnd);
        if (isUp) {
            clearUpBook(false);
        } else {
            clearDownBook(false);
        }

        int bidsKey = indexOfRange(payload, "\"bids\"", objStart, objEnd);
        if (bidsKey >= 0) {
            int arrStart = indexOfCharRange(payload, '[', bidsKey + 6, objEnd);
            if (arrStart >= 0) {
                int arrEnd = findMatchingBracket(payload, arrStart, objEnd);
                if (arrEnd > arrStart) {
                    parseBookLevelsSlice(payload, arrStart, arrEnd + 1, isUp, true);
                }
            }
        }

        int asksKey = indexOfRange(payload, "\"asks\"", objStart, objEnd);
        if (asksKey >= 0) {
            int arrStart = indexOfCharRange(payload, '[', asksKey + 6, objEnd);
            if (arrStart >= 0) {
                int arrEnd = findMatchingBracket(payload, arrStart, objEnd);
                if (arrEnd > arrStart) {
                    parseBookLevelsSlice(payload, arrStart, arrEnd + 1, isUp, false);
                }
            }
        }

        reconcileBookAfterSnapshot(isUp);
    }

    private void applyPriceChangePayload(String payload, long receiptTimestampMs) {
        if (payload == null || payload.isEmpty()) {
            return;
        }
        int len = payload.length();
        long serverTs = extractServerTimestampMs(payload, 0, len);
        updateObservedLatency(receiptTimestampMs, serverTs);

        int pcKey = payload.indexOf("\"price_changes\"");
        if (pcKey < 0) {
            return;
        }
        int arrStart = payload.indexOf('[', pcKey + 15);
        if (arrStart < 0) {
            return;
        }
        int arrEnd = findMatchingBracket(payload, arrStart, len);
        if (arrEnd < 0) {
            return;
        }

        int i = arrStart + 1;
        while (i < arrEnd) {
            int itemStart = indexOfCharRange(payload, '{', i, arrEnd);
            if (itemStart < 0) {
                break;
            }
            int itemEnd = findMatchingBrace(payload, itemStart, arrEnd);
            if (itemEnd < 0) {
                break;
            }

            boolean isUp = extractAndCheckIsUpAsset(payload, itemStart, itemEnd + 1);
            boolean isBuy = extractIsBuySide(payload, itemStart, itemEnd + 1);
            double price = extractFieldDouble(payload, "\"price\"", itemStart, itemEnd + 1);
            double size = extractFieldDouble(payload, "\"size\"", itemStart, itemEnd + 1);

            if (!Double.isNaN(price) && !Double.isNaN(size)) {
                applyDeltaForToken(isUp, isBuy, price, size, receiptTimestampMs);
            }

            i = itemEnd + 1;
        }
    }

    private void parseBookLevelsSlice(String json, int start, int end, boolean isUp, boolean isBids) {
        int openBracket = indexOfCharRange(json, '[', start, end);
        int closeBracket = openBracket >= 0 ? findMatchingBracket(json, openBracket, end) : -1;
        if (openBracket < 0 || closeBracket < 0) {
            Log.errorf("Invalid JSON array slice while parsing order book %s levels", isBids ? "bids" : "asks");
            return;
        }
        int i = openBracket + 1;
        while (i < closeBracket) {
            int itemStart = indexOfCharRange(json, '{', i, closeBracket);
            if (itemStart < 0) {
                break;
            }
            int itemEnd = indexOfCharRange(json, '}', itemStart + 1, closeBracket);
            if (itemEnd < 0) {
                Log.errorf("Malformed JSON object in order book %s levels", isBids ? "bids" : "asks");
                break;
            }

            double price = extractFieldDouble(json, "\"price\"", itemStart, itemEnd + 1);
            double size = extractFieldDouble(json, "\"size\"", itemStart, itemEnd + 1);
            if (!Double.isNaN(price) && size > 0.0) {
                int tick = priceToTick(price);
                if (tick >= 0 && tick <= MAX_TICK) {
                    if (isUp) {
                        if (isBids) {
                            upBids[tick] = size;
                        } else {
                            upAsks[tick] = size;
                        }
                    } else {
                        if (isBids) {
                            downBids[tick] = size;
                        } else {
                            downAsks[tick] = size;
                        }
                    }
                }
            }
            i = itemEnd + 1;
        }
    }

    private boolean extractAndCheckIsUpAsset(String s, int start, int end) {
        int keyIdx = indexOfRange(s, "\"asset_id\"", start, end);
        if (keyIdx < 0) {
            return true;
        }
        int colonIdx = indexOfCharRange(s, ':', keyIdx + 10, end);
        if (colonIdx < 0) {
            return true;
        }
        int q1 = indexOfCharRange(s, '"', colonIdx + 1, end);
        if (q1 < 0) {
            return true;
        }
        int q2 = indexOfCharRange(s, '"', q1 + 1, end);
        if (q2 < 0) {
            return true;
        }
        int valLen = q2 - (q1 + 1);
        if (upTokenId != null && valLen == upTokenId.length() && s.regionMatches(q1 + 1, upTokenId, 0, valLen)) {
            return true;
        }
        if (downTokenId != null && valLen == downTokenId.length() && s.regionMatches(q1 + 1, downTokenId, 0, valLen)) {
            return false;
        }
        return indexOfIgnoreCaseRange(s, "down", q1 + 1, q2) < 0;
    }

    private static boolean extractIsBuySide(String s, int start, int end) {
        int keyIdx = indexOfRange(s, "\"side\"", start, end);
        if (keyIdx < 0) {
            return true;
        }
        int colonIdx = indexOfCharRange(s, ':', keyIdx + 6, end);
        if (colonIdx < 0) {
            return true;
        }
        int q1 = indexOfCharRange(s, '"', colonIdx + 1, end);
        if (q1 < 0 || q1 + 1 >= end) {
            return true;
        }
        char c = s.charAt(q1 + 1);
        return c == 'B' || c == 'b';
    }

    private static double extractFieldDouble(String s, String fieldKey, int start, int end) {
        int keyIdx = indexOfRange(s, fieldKey, start, end);
        if (keyIdx < 0) {
            return Double.NaN;
        }
        int colonIdx = indexOfCharRange(s, ':', keyIdx + fieldKey.length(), end);
        if (colonIdx < 0) {
            return Double.NaN;
        }
        int valStart = colonIdx + 1;
        while (valStart < end) {
            char c = s.charAt(valStart);
            if (c == ' ' || c == '\t' || c == '"') {
                valStart++;
            } else {
                break;
            }
        }
        int valEnd = valStart;
        while (valEnd < end) {
            char c = s.charAt(valEnd);
            if ((c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E') {
                valEnd++;
            } else {
                break;
            }
        }
        return parseFastDouble(s, valStart, valEnd);
    }

    private static long extractServerTimestampMs(String s, int start, int end) {
        if (s == null || start >= end) {
            return 0L;
        }
        int keyIdx = indexOfRange(s, "\"timestamp\"", start, end);
        if (keyIdx < 0) {
            return 0L;
        }
        int colonIdx = indexOfCharRange(s, ':', keyIdx + 11, end);
        if (colonIdx < 0) {
            return 0L;
        }
        int valStart = colonIdx + 1;
        while (valStart < end) {
            char c = s.charAt(valStart);
            if (c == ' ' || c == '\t' || c == '"') {
                valStart++;
            } else {
                break;
            }
        }
        int valEnd = valStart;
        while (valEnd < end) {
            char c = s.charAt(valEnd);
            if (c >= '0' && c <= '9') {
                valEnd++;
            } else {
                break;
            }
        }
        return parseFastLong(s, valStart, valEnd);
    }

    /**
     * Parses a decimal number directly from a character slice without allocating substrings.
     *
     * @param s source string
     * @param start inclusive start index
     * @param end exclusive end index
     * @return parsed double value, or {@link Double#NaN} if empty
     */
    public static double parseFastDouble(String s, int start, int end) {
        if (s == null || start >= end) {
            return Double.NaN;
        }
        boolean negative = false;
        int i = start;
        char c0 = s.charAt(i);
        if (c0 == '-') {
            negative = true;
            i++;
        } else if (c0 == '+') {
            i++;
        }
        if (i >= end) {
            return Double.NaN;
        }
        long intPart = 0L;
        long fracPart = 0L;
        long fracDiv = 1L;
        boolean inFrac = false;
        for (; i < end; i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                int digit = c - '0';
                if (!inFrac) {
                    intPart = intPart * 10L + digit;
                } else if (fracDiv < 1_000_000_000_000L) {
                    fracPart = fracPart * 10L + digit;
                    fracDiv *= 10L;
                }
            } else if (c == '.') {
                inFrac = true;
            } else if (c == 'e' || c == 'E') {
                return Double.parseDouble(s.substring(start, end));
            }
        }
        double result = (double) intPart + ((double) fracPart / (double) fracDiv);
        return negative ? -result : result;
    }

    /**
     * Parses an unsigned integer directly from a character slice without allocating substrings.
     *
     * @param s source string
     * @param start inclusive start index
     * @param end exclusive end index
     * @return parsed long value
     */
    public static long parseFastLong(String s, int start, int end) {
        if (s == null || start >= end) {
            return 0L;
        }
        long val = 0L;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                val = val * 10L + (c - '0');
            }
        }
        return val;
    }

    private static int findMatchingBrace(String s, int openIdx, int limit) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = openIdx; i < limit; i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == openIdx || s.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private static int findMatchingBracket(String s, int openIdx, int limit) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = openIdx; i < limit; i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == openIdx || s.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '[') {
                    depth++;
                } else if (c == ']') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private static int indexOfRange(String s, String target, int start, int end) {
        int idx = s.indexOf(target, start);
        return (idx >= 0 && idx + target.length() <= end) ? idx : -1;
    }

    private static int indexOfCharRange(String s, char ch, int start, int end) {
        int idx = s.indexOf(ch, start);
        return (idx >= 0 && idx < end) ? idx : -1;
    }

    private static int indexOfIgnoreCaseRange(String s, String lowerTarget, int start, int end) {
        int tLen = lowerTarget.length();
        int max = end - tLen;
        for (int i = start; i <= max; i++) {
            if (s.regionMatches(true, i, lowerTarget, 0, tLen)) {
                return i;
            }
        }
        return -1;
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

package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.OrderBookQuote;
import io.quarkus.logging.Log;

import java.util.Locale;

/**
 * High-performance, L1d-cache-friendly L2 Order Book for Polymarket binary options.
 * <p>
 * Uses flat primitive tick arrays ({@code 0..10000} representing prices {@code 0.0000..1.0000}
 * at 1-bps resolution) paired with compact 64-bit active-tick and consumed-tick bitsets
 * ({@code long[157]} = {@code 1.25 KB}) and dirty-flag quote caching to apply typed CLOB frames
 * with zero JSON parsing, zero full-array zeroing sweeps, and {@code O(1)} BBO jumps.
 * <p>
 * Simultaneously tracks the real-time one-way network/processing latency
 * ({@code ts - serverTimestampMs}) observed on incoming Polymarket CLOB frames.
 */
public class PolymarketOrderBook {

    private static final int MAX_TICK = 10_000;
    private static final int MASK_WORDS = (MAX_TICK >>> 6) + 1;
    private static final double TICK_SCALE = 10_000.0;
    private static final double TICK_INV = 0.0001;
    private static final int ONE_CENT_TICKS = 100;
    private static final double QUOTE_SLIPPAGE_SHARES = 100.0;
    private static final double EPS = 1e-9;
    private static final long DEFAULT_INITIAL_ONE_WAY_LATENCY_MS = 10L;

    private final double[] upBids = new double[MAX_TICK + 1];
    private final double[] upAsks = new double[MAX_TICK + 1];
    private final double[] upBidsConsumed = new double[MAX_TICK + 1];
    private final double[] upAsksConsumed = new double[MAX_TICK + 1];
    private final long[] upBidMask = new long[MASK_WORDS];
    private final long[] upAskMask = new long[MASK_WORDS];
    private final long[] upBidConsumedMask = new long[MASK_WORDS];
    private final long[] upAskConsumedMask = new long[MASK_WORDS];
    private int bestUpBidTick = -1;
    private int bestUpAskTick = MAX_TICK + 1;

    private final double[] downBids = new double[MAX_TICK + 1];
    private final double[] downAsks = new double[MAX_TICK + 1];
    private final double[] downBidsConsumed = new double[MAX_TICK + 1];
    private final double[] downAsksConsumed = new double[MAX_TICK + 1];
    private final long[] downBidMask = new long[MASK_WORDS];
    private final long[] downAskMask = new long[MASK_WORDS];
    private final long[] downBidConsumedMask = new long[MASK_WORDS];
    private final long[] downAskConsumedMask = new long[MASK_WORDS];
    private int bestDownBidTick = -1;
    private int bestDownAskTick = MAX_TICK + 1;

    // Dirty-flag quote cache for depth1c and 100-share slippage metrics
    private boolean upAskCacheDirty = true;
    private int upAskWatchMaxTick = MAX_TICK;
    private double cachedDepth1cUp = 0.0;
    private double cachedFillUpAvgPrice = 0.0;

    private boolean downCacheDirty = true;
    private boolean downUsesUpBidsComplement = true;
    private int downAskWatchMaxTick = MAX_TICK;
    private int upBidWatchMinTick = 0;
    private double cachedDepth1cDown = 0.0;
    private double cachedFillDownAvgPrice = 0.0;

    private String upTokenId = "BTC-UP";
    private String downTokenId = "BTC-DOWN";
    private long lastUpdateTimeMs = 0L;
    private long lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;

    /**
     * Clears all bids, asks, consumed liquidity memory, and resets active candle book state.
     */
    public void clear() {
        clearSideInternal(true, true);
        clearSideInternal(false, true);
        lastUpdateTimeMs = 0L;
        lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;
        invalidateQuoteCache();
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
     * Applies a typed Polymarket CLOB frame ({@code book} snapshot, {@code price_change} delta batch,
     * or {@code last_trade_price} latency sample) directly from primitive arrays.
     *
     * @param frame typed CLOB WebSocket frame event
     */
    public void applyClobFrame(BacktestEvent.PolymarketClobFrameEvent frame) {
        if (frame == null) {
            return;
        }
        long ts = frame.timestampMs();
        this.lastUpdateTimeMs = ts;
        updateObservedLatency(ts, frame.serverTimestampMs());

        byte code = frame.eventCode();
        if (code == BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK) {
            boolean isUp = frame.bookIsUp();
            clearSideForSnapshot(isUp);
            int n = frame.levelCount();
            boolean[] bidFlags = frame.isBidFlags();
            int[] ticks = frame.ticks();
            double[] sizes = frame.sizes();
            for (int i = 0; i < n; i++) {
                setSnapshotLevel(isUp, bidFlags[i], ticks[i], sizes[i]);
            }
            finishBookSnapshot(isUp);
        } else if (code == BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE) {
            int n = frame.levelCount();
            boolean[] upFlags = frame.isUpFlags();
            boolean[] bidFlags = frame.isBidFlags();
            int[] ticks = frame.ticks();
            double[] sizes = frame.sizes();
            for (int i = 0; i < n; i++) {
                applyTickDelta(upFlags[i], bidFlags[i], ticks[i], sizes[i], ts);
            }
        }
    }

    /**
     * Clears only the active raw levels on the specified token side prior to populating a new
     * {@code book} snapshot, preserving consumed liquidity memory for reconciliation in
     * {@link #finishBookSnapshot(boolean)}.
     *
     * @param isUp true for UP book, false for DOWN book
     */
    public void clearSideForSnapshot(boolean isUp) {
        clearSideInternal(isUp, false);
        if (isUp) {
            upAskCacheDirty = true;
            if (downUsesUpBidsComplement) {
                downCacheDirty = true;
            }
        } else {
            downCacheDirty = true;
        }
    }

    /**
     * Populates a single raw bid or ask level during a {@code book} snapshot before calling
     * {@link #finishBookSnapshot(boolean)}.
     *
     * @param isUp true for UP book, false for DOWN book
     * @param isBid true for bid level, false for ask level
     * @param tick integer price tick in {@code [0, 10000]}
     * @param size share depth at this tick
     */
    public void setSnapshotLevel(boolean isUp, boolean isBid, int tick, double size) {
        if (tick < 0 || tick > MAX_TICK || Double.isNaN(size)) {
            Log.errorf("Invalid order book level: tick=%d, size=%s", tick, size);
            return;
        }
        int w = tick >>> 6;
        long bitMask = 1L << (tick & 63);
        if (isUp) {
            if (isBid) {
                if (size > 0.0) {
                    upBids[tick] = size;
                    upBidMask[w] |= bitMask;
                } else {
                    upBids[tick] = 0.0;
                    upBidMask[w] &= ~bitMask;
                }
            } else {
                if (size > 0.0) {
                    upAsks[tick] = size;
                    upAskMask[w] |= bitMask;
                } else {
                    upAsks[tick] = 0.0;
                    upAskMask[w] &= ~bitMask;
                }
            }
        } else {
            if (isBid) {
                if (size > 0.0) {
                    downBids[tick] = size;
                    downBidMask[w] |= bitMask;
                } else {
                    downBids[tick] = 0.0;
                    downBidMask[w] &= ~bitMask;
                }
            } else {
                if (size > 0.0) {
                    downAsks[tick] = size;
                    downAskMask[w] |= bitMask;
                } else {
                    downAsks[tick] = 0.0;
                    downAskMask[w] &= ~bitMask;
                }
            }
        }
    }

    /**
     * Reconciles strategy-consumed liquidity memory against newly populated snapshot levels
     * and recomputes BBO pointers via {@code O(1)} bitset scans.
     *
     * @param isUp true for UP book, false for DOWN book
     */
    public void finishBookSnapshot(boolean isUp) {
        if (isUp) {
            reconcileConsumed(upBids, upBidsConsumed, upBidConsumedMask);
            reconcileConsumed(upAsks, upAsksConsumed, upAskConsumedMask);
            bestUpBidTick = findNextBidFrom(MAX_TICK, upBids, upBidMask, upBidsConsumed);
            bestUpAskTick = findNextAskFrom(0, upAsks, upAskMask, upAsksConsumed);
            upAskCacheDirty = true;
            if (downUsesUpBidsComplement) {
                downCacheDirty = true;
            }
        } else {
            reconcileConsumed(downBids, downBidsConsumed, downBidConsumedMask);
            reconcileConsumed(downAsks, downAsksConsumed, downAskConsumedMask);
            bestDownBidTick = findNextBidFrom(MAX_TICK, downBids, downBidMask, downBidsConsumed);
            bestDownAskTick = findNextAskFrom(0, downAsks, downAskMask, downAsksConsumed);
            downCacheDirty = true;
        }
    }

    /**
     * Applies an incremental Level 2 tick delta update directly to the UP or DOWN token book.
     * <p>
     * If {@code size <= 0}, the level is removed and its consumed shares memory is reset to {@code 0}.
     * If {@code size < consumed}, consumed shares are clamped to {@code size} so subsequent maker
     * liquidity additions become available.
     *
     * @param isUp true for UP token book, false for DOWN token book
     * @param isBid true for BUY (bids), false for SELL (asks)
     * @param tick integer price tick in {@code [0, 10000]}
     * @param size updated depth size ({@code 0.0} removes level)
     * @param timestampMs delta receipt timestamp in milliseconds UTC
     */
    public void applyTickDelta(boolean isUp, boolean isBid, int tick, double size, long timestampMs) {
        if (tick < 0 || tick > MAX_TICK || Double.isNaN(size)) {
            Log.errorf("Invalid order book level: tick=%d, size=%s", tick, size);
            return;
        }
        this.lastUpdateTimeMs = timestampMs;
        double cleanSize = size > 0.0 ? size : 0.0;
        if (isUp) {
            if (isBid) {
                setBidLevel(upBids, upBidMask, upBidsConsumed, upBidConsumedMask, tick, cleanSize, true);
                if (downUsesUpBidsComplement && tick >= upBidWatchMinTick) {
                    downCacheDirty = true;
                }
            } else {
                setAskLevel(upAsks, upAskMask, upAsksConsumed, upAskConsumedMask, tick, cleanSize, true);
                if (tick <= upAskWatchMaxTick) {
                    upAskCacheDirty = true;
                }
            }
        } else {
            if (isBid) {
                setBidLevel(downBids, downBidMask, downBidsConsumed, downBidConsumedMask, tick, cleanSize, false);
            } else {
                setAskLevel(downAsks, downAskMask, downAsksConsumed, downAskConsumedMask, tick, cleanSize, false);
                if (tick <= downAskWatchMaxTick) {
                    downCacheDirty = true;
                }
            }
        }
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
        if (upAskCacheDirty) {
            refreshUpAskQuoteCache();
        }
        return cachedDepth1cUp;
    }

    /**
     * Calculates cumulative effective liquidity depth within 1 cent of best ask for DOWN
     * (or within 1 cent of best bid for UP via complementarity when DOWN book is not explicitly populated)
     * after subtracting strategy-consumed shares.
     *
     * @return 1-cent effective depth for DOWN
     */
    public double getDepth1cDown() {
        if (downCacheDirty) {
            refreshDownQuoteCache();
        }
        return cachedDepth1cDown;
    }

    /**
     * Converts current book state into a unified {@link OrderBookQuote} snapshot, reusing cached
     * 1-cent depth and 100-share slippage metrics whenever mutations occurred outside the active
     * BBO/slippage tick window.
     *
     * @param timestampMs current clock timestamp in milliseconds UTC
     * @return constructed OrderBookQuote
     */
    public OrderBookQuote toOrderBookQuote(long timestampMs) {
        if (upAskCacheDirty) {
            refreshUpAskQuoteCache();
        }
        if (downCacheDirty) {
            refreshDownQuoteCache();
        }

        double bAskUp = getBestAskUp();
        double bBidUp = getBestBidUp();
        double bAskDown = getBestAskDown();
        double bBidDown = getBestBidDown();

        return new OrderBookQuote(
                bAskUp,
                bBidUp,
                bAskDown,
                bBidDown,
                cachedDepth1cUp,
                cachedDepth1cDown,
                cachedFillUpAvgPrice > 0.0 ? cachedFillUpAvgPrice : bAskUp,
                cachedFillDownAvgPrice > 0.0 ? cachedFillDownAvgPrice : bAskDown,
                timestampMs > 0L ? timestampMs : lastUpdateTimeMs
        );
    }

    /**
     * Read-only simulation of order execution against current effective book depth without mutating
     * consumed liquidity arrays.
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
                    if (bestUpAskTick <= maxTick) {
                        int w = bestUpAskTick >>> 6;
                        int maxW = maxTick >>> 6;
                        long bits = upAskMask[w] & (-1L << (bestUpAskTick & 63));
                        outer:
                        while (true) {
                            while (bits != 0L) {
                                int bit = Long.numberOfTrailingZeros(bits);
                                int t = (w << 6) + bit;
                                if (t > maxTick) {
                                    break outer;
                                }
                                bits &= (bits - 1L);
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
                                    upAskConsumedMask[w] |= (1L << bit);
                                    int compTick = MAX_TICK - t;
                                    if (downBids[compTick] > 0.0) {
                                        downBidsConsumed[compTick] = Math.min(downBids[compTick], downBidsConsumed[compTick] + take);
                                        downBidConsumedMask[compTick >>> 6] |= (1L << (compTick & 63));
                                    }
                                }
                                if (remainingShares <= EPS) {
                                    break outer;
                                }
                            }
                            if (++w > maxW) {
                                break;
                            }
                            bits = upAskMask[w];
                        }
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestUpAsk();
                        advanceBestDownBid();
                        invalidateQuoteCache();
                    }
                }
            } else {
                if (bestUpBidTick >= 0) {
                    int minTick = Math.max(0, limitTick);
                    if (bestUpBidTick >= minTick) {
                        int w = bestUpBidTick >>> 6;
                        int minW = minTick >>> 6;
                        long bits = upBidMask[w] & (-1L >>> (63 - (bestUpBidTick & 63)));
                        outer:
                        while (true) {
                            while (bits != 0L) {
                                int bit = 63 - Long.numberOfLeadingZeros(bits);
                                int t = (w << 6) + bit;
                                if (t < minTick) {
                                    break outer;
                                }
                                bits &= ~(1L << bit);
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
                                    upBidConsumedMask[w] |= (1L << bit);
                                    int compTick = MAX_TICK - t;
                                    if (downAsks[compTick] > 0.0) {
                                        downAsksConsumed[compTick] = Math.min(downAsks[compTick], downAsksConsumed[compTick] + take);
                                        downAskConsumedMask[compTick >>> 6] |= (1L << (compTick & 63));
                                    }
                                }
                                if (remainingShares <= EPS) {
                                    break outer;
                                }
                            }
                            if (--w < minW) {
                                break;
                            }
                            bits = upBidMask[w];
                        }
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestUpBid();
                        advanceBestDownAsk();
                        invalidateQuoteCache();
                    }
                }
            }
        } else {
            if (isBuy) {
                if (bestDownAskTick <= MAX_TICK) {
                    int maxTick = Math.min(MAX_TICK, limitTick);
                    if (bestDownAskTick <= maxTick) {
                        int w = bestDownAskTick >>> 6;
                        int maxW = maxTick >>> 6;
                        long bits = downAskMask[w] & (-1L << (bestDownAskTick & 63));
                        outer:
                        while (true) {
                            while (bits != 0L) {
                                int bit = Long.numberOfTrailingZeros(bits);
                                int t = (w << 6) + bit;
                                if (t > maxTick) {
                                    break outer;
                                }
                                bits &= (bits - 1L);
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
                                    downAskConsumedMask[w] |= (1L << bit);
                                    int compTick = MAX_TICK - t;
                                    if (upBids[compTick] > 0.0) {
                                        upBidsConsumed[compTick] = Math.min(upBids[compTick], upBidsConsumed[compTick] + take);
                                        upBidConsumedMask[compTick >>> 6] |= (1L << (compTick & 63));
                                    }
                                }
                                if (remainingShares <= EPS) {
                                    break outer;
                                }
                            }
                            if (++w > maxW) {
                                break;
                            }
                            bits = downAskMask[w];
                        }
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestDownAsk();
                        advanceBestUpBid();
                        invalidateQuoteCache();
                    }
                } else if (bestUpBidTick >= 0) {
                    int w = bestUpBidTick >>> 6;
                    long bits = upBidMask[w] & (-1L >>> (63 - (bestUpBidTick & 63)));
                    outer:
                    while (true) {
                        while (bits != 0L) {
                            int bit = 63 - Long.numberOfLeadingZeros(bits);
                            int t = (w << 6) + bit;
                            bits &= ~(1L << bit);
                            double available = upBids[t] - upBidsConsumed[t];
                            if (available <= EPS) {
                                continue;
                            }
                            double downPrice = Math.max(0.001, (MAX_TICK - t) * TICK_INV);
                            if (downPrice > limitPrice + EPS) {
                                break outer;
                            }
                            double take = Math.min(remainingShares, available);
                            cost += take * downPrice;
                            executedShares += take;
                            remainingShares -= take;
                            if (consumeLiquidity) {
                                upBidsConsumed[t] += take;
                                upBidConsumedMask[w] |= (1L << bit);
                            }
                            if (remainingShares <= EPS) {
                                break outer;
                            }
                        }
                        if (--w < 0) {
                            break;
                        }
                        bits = upBidMask[w];
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestUpBid();
                        invalidateQuoteCache();
                    }
                }
            } else {
                if (bestDownBidTick >= 0) {
                    int minTick = Math.max(0, limitTick);
                    if (bestDownBidTick >= minTick) {
                        int w = bestDownBidTick >>> 6;
                        int minW = minTick >>> 6;
                        long bits = downBidMask[w] & (-1L >>> (63 - (bestDownBidTick & 63)));
                        outer:
                        while (true) {
                            while (bits != 0L) {
                                int bit = 63 - Long.numberOfLeadingZeros(bits);
                                int t = (w << 6) + bit;
                                if (t < minTick) {
                                    break outer;
                                }
                                bits &= ~(1L << bit);
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
                                    downBidConsumedMask[w] |= (1L << bit);
                                    int compTick = MAX_TICK - t;
                                    if (upAsks[compTick] > 0.0) {
                                        upAsksConsumed[compTick] = Math.min(upAsks[compTick], upAsksConsumed[compTick] + take);
                                        upAskConsumedMask[compTick >>> 6] |= (1L << (compTick & 63));
                                    }
                                }
                                if (remainingShares <= EPS) {
                                    break outer;
                                }
                            }
                            if (--w < minW) {
                                break;
                            }
                            bits = downBidMask[w];
                        }
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestDownBid();
                        advanceBestUpAsk();
                        invalidateQuoteCache();
                    }
                } else if (bestUpAskTick <= MAX_TICK) {
                    int w = bestUpAskTick >>> 6;
                    long bits = upAskMask[w] & (-1L << (bestUpAskTick & 63));
                    outer:
                    while (true) {
                        while (bits != 0L) {
                            int bit = Long.numberOfTrailingZeros(bits);
                            int t = (w << 6) + bit;
                            bits &= (bits - 1L);
                            double available = upAsks[t] - upAsksConsumed[t];
                            if (available <= EPS) {
                                continue;
                            }
                            double downPrice = Math.max(0.001, (MAX_TICK - t) * TICK_INV);
                            if (downPrice + EPS < limitPrice) {
                                break outer;
                            }
                            double take = Math.min(remainingShares, available);
                            cost += take * downPrice;
                            executedShares += take;
                            remainingShares -= take;
                            if (consumeLiquidity) {
                                upAsksConsumed[t] += take;
                                upAskConsumedMask[w] |= (1L << bit);
                            }
                            if (remainingShares <= EPS) {
                                break outer;
                            }
                        }
                        if (++w >= MASK_WORDS) {
                            break;
                        }
                        bits = upAskMask[w];
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
                        advanceBestUpAsk();
                        invalidateQuoteCache();
                    }
                }
            }
        }

        double avgPrice = executedShares > 0.0 ? (cost / executedShares) : 0.0;
        double fee = executedShares * 0.07 * avgPrice * Math.max(0.0, 1.0 - avgPrice);

        return new FillResult(avgPrice, executedShares, fee);
    }

    private void refreshUpAskQuoteCache() {
        if (bestUpAskTick > MAX_TICK) {
            cachedDepth1cUp = 0.0;
            cachedFillUpAvgPrice = 0.0;
            upAskWatchMaxTick = MAX_TICK;
            upAskCacheDirty = false;
            return;
        }

        int depthLimitTick = Math.min(MAX_TICK, bestUpAskTick + ONE_CENT_TICKS);
        double depthSum = 0.0;
        double remainingShares = QUOTE_SLIPPAGE_SHARES;
        double cost = 0.0;
        double executedShares = 0.0;
        int fillEndTick = bestUpAskTick;

        int w = bestUpAskTick >>> 6;
        long bits = upAskMask[w] & (-1L << (bestUpAskTick & 63));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                int t = (w << 6) + bit;
                if (t > depthLimitTick && remainingShares <= EPS) {
                    break outer;
                }
                bits &= (bits - 1L);
                double eff = upAsks[t] - upAsksConsumed[t];
                if (eff <= EPS) {
                    continue;
                }
                if (t <= depthLimitTick) {
                    depthSum += eff;
                }
                if (remainingShares > EPS) {
                    double take = Math.min(remainingShares, eff);
                    cost += take * (t * TICK_INV);
                    executedShares += take;
                    remainingShares -= take;
                    fillEndTick = t;
                }
            }
            if ((w << 6) + 63 >= depthLimitTick && remainingShares <= EPS) {
                break;
            }
            if (++w >= MASK_WORDS) {
                break;
            }
            bits = upAskMask[w];
        }

        cachedDepth1cUp = depthSum;
        cachedFillUpAvgPrice = executedShares > 0.0 ? (cost / executedShares) : 0.0;
        upAskWatchMaxTick = remainingShares <= EPS
                ? Math.max(depthLimitTick, fillEndTick)
                : MAX_TICK;
        upAskCacheDirty = false;
    }

    private void refreshDownQuoteCache() {
        if (bestDownAskTick <= MAX_TICK) {
            downUsesUpBidsComplement = false;
            int depthLimitTick = Math.min(MAX_TICK, bestDownAskTick + ONE_CENT_TICKS);
            double depthSum = 0.0;
            double remainingShares = QUOTE_SLIPPAGE_SHARES;
            double cost = 0.0;
            double executedShares = 0.0;
            int fillEndTick = bestDownAskTick;

            int w = bestDownAskTick >>> 6;
            long bits = downAskMask[w] & (-1L << (bestDownAskTick & 63));
            outer:
            while (true) {
                while (bits != 0L) {
                    int bit = Long.numberOfTrailingZeros(bits);
                    int t = (w << 6) + bit;
                    if (t > depthLimitTick && remainingShares <= EPS) {
                        break outer;
                    }
                    bits &= (bits - 1L);
                    double eff = downAsks[t] - downAsksConsumed[t];
                    if (eff <= EPS) {
                        continue;
                    }
                    if (t <= depthLimitTick) {
                        depthSum += eff;
                    }
                    if (remainingShares > EPS) {
                        double take = Math.min(remainingShares, eff);
                        cost += take * (t * TICK_INV);
                        executedShares += take;
                        remainingShares -= take;
                        fillEndTick = t;
                    }
                }
                if ((w << 6) + 63 >= depthLimitTick && remainingShares <= EPS) {
                    break;
                }
                if (++w >= MASK_WORDS) {
                    break;
                }
                bits = downAskMask[w];
            }

            cachedDepth1cDown = depthSum;
            cachedFillDownAvgPrice = executedShares > 0.0 ? (cost / executedShares) : 0.0;
            downAskWatchMaxTick = remainingShares <= EPS
                    ? Math.max(depthLimitTick, fillEndTick)
                    : MAX_TICK;
            downCacheDirty = false;
            return;
        }

        downUsesUpBidsComplement = true;
        downAskWatchMaxTick = MAX_TICK;
        if (bestUpBidTick < 0) {
            cachedDepth1cDown = 0.0;
            cachedFillDownAvgPrice = 0.0;
            upBidWatchMinTick = 0;
            downCacheDirty = false;
            return;
        }

        int depthFloorTick = Math.max(0, bestUpBidTick - ONE_CENT_TICKS);
        double depthSum = 0.0;
        double remainingShares = QUOTE_SLIPPAGE_SHARES;
        double cost = 0.0;
        double executedShares = 0.0;
        int fillFloorTick = bestUpBidTick;

        int w = bestUpBidTick >>> 6;
        long bits = upBidMask[w] & (-1L >>> (63 - (bestUpBidTick & 63)));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = 63 - Long.numberOfLeadingZeros(bits);
                int t = (w << 6) + bit;
                if (t < depthFloorTick && remainingShares <= EPS) {
                    break outer;
                }
                bits &= ~(1L << bit);
                double eff = upBids[t] - upBidsConsumed[t];
                if (eff <= EPS) {
                    continue;
                }
                if (t >= depthFloorTick) {
                    depthSum += eff;
                }
                if (remainingShares > EPS) {
                    double downPrice = Math.max(0.001, (MAX_TICK - t) * TICK_INV);
                    double take = Math.min(remainingShares, eff);
                    cost += take * downPrice;
                    executedShares += take;
                    remainingShares -= take;
                    fillFloorTick = t;
                }
            }
            if ((w << 6) <= depthFloorTick && remainingShares <= EPS) {
                break;
            }
            if (--w < 0) {
                break;
            }
            bits = upBidMask[w];
        }

        cachedDepth1cDown = depthSum;
        cachedFillDownAvgPrice = executedShares > 0.0 ? (cost / executedShares) : 0.0;
        upBidWatchMinTick = remainingShares <= EPS
                ? Math.min(depthFloorTick, fillFloorTick)
                : 0;
        downCacheDirty = false;
    }

    private void invalidateQuoteCache() {
        upAskCacheDirty = true;
        downCacheDirty = true;
    }

    private void clearSideInternal(boolean isUp, boolean resetConsumed) {
        if (isUp) {
            clearMaskedArray(upBids, upBidMask);
            clearMaskedArray(upAsks, upAskMask);
            if (resetConsumed) {
                clearMaskedArray(upBidsConsumed, upBidConsumedMask);
                clearMaskedArray(upAsksConsumed, upAskConsumedMask);
            }
            bestUpBidTick = -1;
            bestUpAskTick = MAX_TICK + 1;
        } else {
            clearMaskedArray(downBids, downBidMask);
            clearMaskedArray(downAsks, downAskMask);
            if (resetConsumed) {
                clearMaskedArray(downBidsConsumed, downBidConsumedMask);
                clearMaskedArray(downAsksConsumed, downAskConsumedMask);
            }
            bestDownBidTick = -1;
            bestDownAskTick = MAX_TICK + 1;
        }
    }

    private static void clearMaskedArray(double[] values, long[] mask) {
        for (int w = 0; w < MASK_WORDS; w++) {
            long bits = mask[w];
            if (bits != 0L) {
                int base = w << 6;
                while (bits != 0L) {
                    int bit = Long.numberOfTrailingZeros(bits);
                    values[base + bit] = 0.0;
                    bits &= (bits - 1L);
                }
                mask[w] = 0L;
            }
        }
    }

    private static void reconcileConsumed(double[] raw, double[] consumed, long[] consumedMask) {
        for (int w = 0; w < MASK_WORDS; w++) {
            long cBits = consumedMask[w];
            if (cBits != 0L) {
                int base = w << 6;
                while (cBits != 0L) {
                    int bit = Long.numberOfTrailingZeros(cBits);
                    int t = base + bit;
                    double r = raw[t];
                    if (r <= 0.0) {
                        consumed[t] = 0.0;
                        consumedMask[w] &= ~(1L << bit);
                    } else if (r < consumed[t]) {
                        consumed[t] = r;
                    }
                    cBits &= (cBits - 1L);
                }
            }
        }
    }

    private static int findNextBidFrom(int startTick, double[] bids, long[] bidMask, double[] consumed) {
        if (startTick < 0) {
            return -1;
        }
        int start = Math.min(MAX_TICK, startTick);
        int w = start >>> 6;
        int maxBit = start & 63;
        long bits = bidMask[w] & (-1L >>> (63 - maxBit));
        while (true) {
            while (bits != 0L) {
                int bit = 63 - Long.numberOfLeadingZeros(bits);
                int t = (w << 6) + bit;
                if (bids[t] - consumed[t] > EPS) {
                    return t;
                }
                bits &= ~(1L << bit);
            }
            if (--w < 0) {
                return -1;
            }
            bits = bidMask[w];
        }
    }

    private static int findNextAskFrom(int startTick, double[] asks, long[] askMask, double[] consumed) {
        if (startTick > MAX_TICK) {
            return MAX_TICK + 1;
        }
        int start = Math.max(0, startTick);
        int w = start >>> 6;
        int minBit = start & 63;
        long bits = askMask[w] & (-1L << minBit);
        while (true) {
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                int t = (w << 6) + bit;
                if (asks[t] - consumed[t] > EPS) {
                    return t;
                }
                bits &= (bits - 1L);
            }
            if (++w >= MASK_WORDS) {
                return MAX_TICK + 1;
            }
            bits = askMask[w];
        }
    }

    private void advanceBestUpBid() {
        bestUpBidTick = findNextBidFrom(bestUpBidTick, upBids, upBidMask, upBidsConsumed);
    }

    private void advanceBestUpAsk() {
        bestUpAskTick = findNextAskFrom(bestUpAskTick, upAsks, upAskMask, upAsksConsumed);
    }

    private void advanceBestDownBid() {
        bestDownBidTick = findNextBidFrom(bestDownBidTick, downBids, downBidMask, downBidsConsumed);
    }

    private void advanceBestDownAsk() {
        bestDownAskTick = findNextAskFrom(bestDownAskTick, downAsks, downAskMask, downAsksConsumed);
    }

    private void setBidLevel(
            double[] bids,
            long[] bidMask,
            double[] consumed,
            long[] consumedMask,
            int tick,
            double size,
            boolean isUp) {

        int w = tick >>> 6;
        long bitMask = 1L << (tick & 63);
        bids[tick] = size;
        if (size <= 0.0) {
            bidMask[w] &= ~bitMask;
            if (consumed[tick] != 0.0) {
                consumed[tick] = 0.0;
                consumedMask[w] &= ~bitMask;
            }
        } else {
            bidMask[w] |= bitMask;
            if (size < consumed[tick]) {
                consumed[tick] = size;
            }
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

    private void setAskLevel(
            double[] asks,
            long[] askMask,
            double[] consumed,
            long[] consumedMask,
            int tick,
            double size,
            boolean isUp) {

        int w = tick >>> 6;
        long bitMask = 1L << (tick & 63);
        asks[tick] = size;
        if (size <= 0.0) {
            askMask[w] &= ~bitMask;
            if (consumed[tick] != 0.0) {
                consumed[tick] = 0.0;
                consumedMask[w] &= ~bitMask;
            }
        } else {
            askMask[w] |= bitMask;
            if (size < consumed[tick]) {
                consumed[tick] = size;
            }
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

    /**
     * Execution outcome of a simulated book sweep order.
     *
     * @param avgPrice volume-weighted average fill price
     * @param executedShares total executed contract shares
     * @param fee calculated taker fee in USD
     */
    public record FillResult(double avgPrice, double executedShares, double fee) {}
}

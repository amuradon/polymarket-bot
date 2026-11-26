package cz.polymarket.bot.orderbook;

import cz.polymarket.bot.domain.OrderBookQuote;
import io.quarkus.logging.Log;

import java.util.Locale;

/**
 * Canonical L1d-cache-friendly Level 2 Order Book for Polymarket binary options, shared identically
 * across {@code LIVE}, {@code PAPER}, and {@code BACKTEST} runtimes.
 * <p>
 * Represents prices on a 1-bps integer tick grid ({@code 0..10_000} corresponding to {@code 0.0000..1.0000})
 * backed by compact 64-bit active-tick and consumed-tick bitsets ({@code long[157]}), symmetric binary
 * complementarity ({@code P_up + P_down = 1.0}), per-side monotonic server timestamp guards, non-fabricating
 * BBO crossed-level pruning, dirty-flag 1-cent depth caching, and zero-allocation on-demand VWAP sweep queries.
 */
public class PolymarketOrderBook {

    /** Maximum 1-bps price tick ({@code 10_000 = $1.0000}). */
    public static final int MAX_TICK = 10_000;

    /** Default Polymarket crypto parabolic taker fee rate ({@code 0.07}, i.e. {@code 1.75%} max at {@code p = 0.50}). */
    public static final double DEFAULT_TAKER_FEE_RATE = 0.07;

    private static final int MASK_WORDS = (MAX_TICK >>> 6) + 1;
    private static final double TICK_SCALE = 10_000.0;
    private static final double TICK_INV = 0.0001;
    private static final int ONE_CENT_TICKS = 100;
    private static final double MIN_COMPLEMENT_PRICE = 0.0001;
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

    // Symmetric dirty-flag caches for 1-cent ask depth
    private boolean upCacheDirty = true;
    private boolean upUsesDownBidsComplement = false;
    private int upAskWatchMaxTick = MAX_TICK;
    private int downBidWatchMinTick = 0;
    private double cachedDepth1cUp = 0.0;

    private boolean downCacheDirty = true;
    private boolean downUsesUpBidsComplement = false;
    private int downAskWatchMaxTick = MAX_TICK;
    private int upBidWatchMinTick = 0;
    private double cachedDepth1cDown = 0.0;

    private String upTokenId = "BTC-UP";
    private String downTokenId = "BTC-DOWN";
    private long lastUpdateTimeMs = 0L;
    private long lastUpServerTimestampMs = -1L;
    private long lastDownServerTimestampMs = -1L;
    private long lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;

    /**
     * Clears all bids, asks, consumed liquidity memory, and per-side server timestamp watermarks.
     */
    public void clear() {
        clearSideInternal(true, true);
        clearSideInternal(false, true);
        lastUpdateTimeMs = 0L;
        lastUpServerTimestampMs = -1L;
        lastDownServerTimestampMs = -1L;
        lastObservedOneWayLatencyMs = DEFAULT_INITIAL_ONE_WAY_LATENCY_MS;
        invalidateQuoteCache();
    }

    /**
     * Resets only strategy-consumed liquidity memory while keeping raw exchange book levels intact.
     */
    public void clearConsumedLiquidity() {
        clearMaskedArray(upBidsConsumed, upBidConsumedMask);
        clearMaskedArray(upAsksConsumed, upAskConsumedMask);
        clearMaskedArray(downBidsConsumed, downBidConsumedMask);
        clearMaskedArray(downAsksConsumed, downAskConsumedMask);
        bestUpBidTick = findNextBidFrom(MAX_TICK, upBids, upBidMask, upBidsConsumed);
        bestUpAskTick = findNextAskFrom(0, upAsks, upAskMask, upAsksConsumed);
        bestDownBidTick = findNextBidFrom(MAX_TICK, downBids, downBidMask, downBidsConsumed);
        bestDownAskTick = findNextAskFrom(0, downAsks, downAskMask, downAsksConsumed);
        invalidateQuoteCache();
    }

    /**
     * Configures the Polymarket CLOB token IDs for UP and DOWN outcomes for the active market.
     *
     * @param upTokenId   token ID for the UP ("Up" / YES) outcome
     * @param downTokenId token ID for the DOWN ("Down" / NO) outcome
     */
    public void setMarketTokenIds(String upTokenId, String downTokenId) {
        this.upTokenId = (upTokenId != null && !upTokenId.isBlank()) ? upTokenId : "BTC-UP";
        this.downTokenId = (downTokenId != null && !downTokenId.isBlank()) ? downTokenId : "BTC-DOWN";
    }

    /**
     * Returns the configured UP token identifier.
     *
     * @return UP token ID
     */
    public String getUpTokenId() {
        return upTokenId;
    }

    /**
     * Returns the configured DOWN token identifier.
     *
     * @return DOWN token ID
     */
    public String getDownTokenId() {
        return downTokenId;
    }

    /**
     * Determines whether a token identifier corresponds to the UP outcome.
     *
     * @param token token identifier or symbolic name
     * @return {@code true} if UP token, {@code false} if DOWN token
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
     * Enforces per-side monotonic server timestamps so delayed redundant WebSocket frames on one leg
     * cannot overwrite newer state on the same token side, while never dropping frames belonging to
     * the opposite binary token.
     *
     * @param isUp              {@code true} for UP token frame, {@code false} for DOWN token frame
     * @param serverTimestampMs exchange payload timestamp in epoch milliseconds ({@code <= 0} bypasses check)
     * @return {@code true} if the frame should be applied, {@code false} if stale for this token side
     */
    public boolean tryAcceptServerTimestamp(boolean isUp, long serverTimestampMs) {
        if (serverTimestampMs <= 0L) {
            return true;
        }
        if (isUp) {
            if (serverTimestampMs < lastUpServerTimestampMs) {
                return false;
            }
            lastUpServerTimestampMs = serverTimestampMs;
            return true;
        } else {
            if (serverTimestampMs < lastDownServerTimestampMs) {
                return false;
            }
            lastDownServerTimestampMs = serverTimestampMs;
            return true;
        }
    }

    /**
     * Returns the timestamp of the most recent book mutation in epoch milliseconds UTC.
     *
     * @return last book update timestamp in ms
     */
    public long getLastUpdateTimeMs() {
        return lastUpdateTimeMs;
    }

    /**
     * Returns the most recently observed one-way latency in milliseconds between the
     * Polymarket server timestamp and the local receipt timestamp ({@code >= 1 ms}).
     *
     * @return observed one-way latency in milliseconds
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
     * Updates observed one-way latency from local receipt timestamp and server payload timestamp.
     *
     * @param receiptTimestampMs local capture timestamp in milliseconds UTC
     * @param serverTimestampMs  exchange server timestamp in milliseconds UTC
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
     * Converts a decimal contract price in {@code [0.0, 1.0]} to an integer 1-bps tick in {@code [0, 10_000]}.
     *
     * @param price decimal price
     * @return clamped 1-bps integer tick
     */
    public static int priceToTick(double price) {
        if (!Double.isFinite(price) || price <= 0.0) {
            return 0;
        }
        if (price >= 1.0) {
            return MAX_TICK;
        }
        return (int) Math.round(price * TICK_SCALE);
    }

    /**
     * Clears only the active raw levels on the specified token side prior to populating a new
     * {@code book} snapshot, preserving consumed liquidity memory for reconciliation in
     * {@link #finishBookSnapshot(boolean)}.
     *
     * @param isUp {@code true} for UP book, {@code false} for DOWN book
     */
    public void clearSideForSnapshot(boolean isUp) {
        clearSideInternal(isUp, false);
        if (isUp) {
            upCacheDirty = true;
            if (downUsesUpBidsComplement) {
                downCacheDirty = true;
            }
        } else {
            downCacheDirty = true;
            if (upUsesDownBidsComplement) {
                upCacheDirty = true;
            }
        }
    }

    /**
     * Populates a single raw bid or ask level during a {@code book} snapshot before calling
     * {@link #finishBookSnapshot(boolean)}.
     *
     * @param isUp  {@code true} for UP book, {@code false} for DOWN book
     * @param isBid {@code true} for bid level, {@code false} for ask level
     * @param tick  integer price tick in {@code [0, 10_000]}
     * @param size  share depth at this tick
     */
    public void setSnapshotLevel(boolean isUp, boolean isBid, int tick, double size) {
        if (tick < 0 || tick > MAX_TICK || !Double.isFinite(size)) {
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
     * @param isUp {@code true} for UP book, {@code false} for DOWN book
     */
    public void finishBookSnapshot(boolean isUp) {
        finishBookSnapshot(isUp, 0L);
    }

    /**
     * Reconciles strategy-consumed liquidity memory against newly populated snapshot levels,
     * updates {@code lastUpdateTimeMs} if positive, and recomputes BBO pointers via {@code O(1)} bitset scans.
     *
     * @param isUp        {@code true} for UP book, {@code false} for DOWN book
     * @param timestampMs snapshot receipt timestamp in epoch milliseconds UTC ({@code > 0} updates {@code lastUpdateTimeMs})
     */
    public void finishBookSnapshot(boolean isUp, long timestampMs) {
        if (timestampMs > 0L) {
            this.lastUpdateTimeMs = timestampMs;
        }
        if (isUp) {
            reconcileConsumed(upBids, upBidsConsumed, upBidConsumedMask);
            reconcileConsumed(upAsks, upAsksConsumed, upAskConsumedMask);
            bestUpBidTick = findNextBidFrom(MAX_TICK, upBids, upBidMask, upBidsConsumed);
            bestUpAskTick = findNextAskFrom(0, upAsks, upAskMask, upAsksConsumed);
            upCacheDirty = true;
            if (downUsesUpBidsComplement || bestDownAskTick > MAX_TICK) {
                downCacheDirty = true;
            }
        } else {
            reconcileConsumed(downBids, downBidsConsumed, downBidConsumedMask);
            reconcileConsumed(downAsks, downAsksConsumed, downAskConsumedMask);
            bestDownBidTick = findNextBidFrom(MAX_TICK, downBids, downBidMask, downBidsConsumed);
            bestDownAskTick = findNextAskFrom(0, downAsks, downAskMask, downAsksConsumed);
            downCacheDirty = true;
            if (upUsesDownBidsComplement || bestUpAskTick > MAX_TICK) {
                upCacheDirty = true;
            }
        }
    }

    /**
     * Applies an incremental Level 2 tick delta update directly to the UP or DOWN token book.
     * <p>
     * If {@code size <= 0}, the level is removed and its consumed shares memory is reset to {@code 0}.
     * If {@code size < consumed}, consumed shares are clamped to {@code size} so subsequent maker
     * liquidity additions become available. Also prunes any opposite-side crossed raw levels when
     * a new positive level crosses old stale depth.
     *
     * @param isUp        {@code true} for UP token book, {@code false} for DOWN token book
     * @param isBid       {@code true} for BUY (bids), {@code false} for SELL (asks)
     * @param tick        integer price tick in {@code [0, 10_000]}
     * @param size        updated depth size ({@code 0.0} removes level)
     * @param timestampMs delta receipt timestamp in milliseconds UTC
     */
    public void applyTickDelta(boolean isUp, boolean isBid, int tick, double size, long timestampMs) {
        if (tick < 0 || tick > MAX_TICK || !Double.isFinite(size)) {
            Log.errorf("Invalid order book level: tick=%d, size=%s", tick, size);
            return;
        }
        if (timestampMs > 0L) {
            this.lastUpdateTimeMs = timestampMs;
        }
        double cleanSize = size > 0.0 ? size : 0.0;
        if (isUp) {
            if (isBid) {
                setBidLevel(upBids, upBidMask, upBidsConsumed, upBidConsumedMask, tick, cleanSize, true);
                if (cleanSize > 0.0 && bestUpAskTick <= tick) {
                    pruneRawAsksBelowTick(upAsks, upAskMask, upAsksConsumed, upAskConsumedMask, tick + 1, true);
                }
                if (downUsesUpBidsComplement && tick >= upBidWatchMinTick) {
                    downCacheDirty = true;
                } else if (bestDownAskTick > MAX_TICK) {
                    downCacheDirty = true;
                }
            } else {
                setAskLevel(upAsks, upAskMask, upAsksConsumed, upAskConsumedMask, tick, cleanSize, true);
                if (cleanSize > 0.0 && bestUpBidTick >= tick) {
                    pruneRawBidsAboveTick(upBids, upBidMask, upBidsConsumed, upBidConsumedMask, tick - 1, true);
                }
                if (!upUsesDownBidsComplement && tick <= upAskWatchMaxTick) {
                    upCacheDirty = true;
                } else if (upUsesDownBidsComplement) {
                    upCacheDirty = true;
                }
            }
        } else {
            if (isBid) {
                setBidLevel(downBids, downBidMask, downBidsConsumed, downBidConsumedMask, tick, cleanSize, false);
                if (cleanSize > 0.0 && bestDownAskTick <= tick) {
                    pruneRawAsksBelowTick(downAsks, downAskMask, downAsksConsumed, downAskConsumedMask, tick + 1, false);
                }
                if (upUsesDownBidsComplement && tick >= downBidWatchMinTick) {
                    upCacheDirty = true;
                } else if (bestUpAskTick > MAX_TICK) {
                    upCacheDirty = true;
                }
            } else {
                setAskLevel(downAsks, downAskMask, downAsksConsumed, downAskConsumedMask, tick, cleanSize, false);
                if (cleanSize > 0.0 && bestDownBidTick >= tick) {
                    pruneRawBidsAboveTick(downBids, downBidMask, downBidsConsumed, downBidConsumedMask, tick - 1, false);
                }
                if (!downUsesUpBidsComplement && tick <= downAskWatchMaxTick) {
                    downCacheDirty = true;
                } else if (downUsesUpBidsComplement) {
                    downCacheDirty = true;
                }
            }
        }
    }

    /**
     * Applies top-of-book hint ticks ({@code best_bid} / {@code best_ask} carried on Polymarket
     * {@code price_change} or {@code best_bid_ask} events) strictly by pruning stale raw levels
     * that violate the exchange-reported BBO, <b>never fabricating synthetic share depth</b>.
     *
     * @param isUp        {@code true} for UP token book, {@code false} for DOWN token book
     * @param bestBidTick exchange-reported best bid tick in {@code [0, 10_000]}, or {@code -1} if absent
     * @param bestAskTick exchange-reported best ask tick in {@code [0, 10_000]}, or {@code -1} if absent
     * @param timestampMs event receipt timestamp in epoch milliseconds UTC
     */
    public void applyBboHint(boolean isUp, int bestBidTick, int bestAskTick, long timestampMs) {
        if (timestampMs > 0L) {
            this.lastUpdateTimeMs = timestampMs;
        }
        if (isUp) {
            if (bestBidTick >= 0 && bestBidTick <= MAX_TICK) {
                pruneRawBidsAboveTick(upBids, upBidMask, upBidsConsumed, upBidConsumedMask, bestBidTick, true);
                pruneRawAsksBelowTick(upAsks, upAskMask, upAsksConsumed, upAskConsumedMask, bestBidTick + 1, true);
            }
            if (bestAskTick >= 0 && bestAskTick <= MAX_TICK) {
                pruneRawAsksBelowTick(upAsks, upAskMask, upAsksConsumed, upAskConsumedMask, bestAskTick, true);
                pruneRawBidsAboveTick(upBids, upBidMask, upBidsConsumed, upBidConsumedMask, bestAskTick - 1, true);
            }
        } else {
            if (bestBidTick >= 0 && bestBidTick <= MAX_TICK) {
                pruneRawBidsAboveTick(downBids, downBidMask, downBidsConsumed, downBidConsumedMask, bestBidTick, false);
                pruneRawAsksBelowTick(downAsks, downAskMask, downAsksConsumed, downAskConsumedMask, bestBidTick + 1, false);
            }
            if (bestAskTick >= 0 && bestAskTick <= MAX_TICK) {
                pruneRawAsksBelowTick(downAsks, downAskMask, downAsksConsumed, downAskConsumedMask, bestAskTick, false);
                pruneRawBidsAboveTick(downBids, downBidMask, downBidsConsumed, downBidConsumedMask, bestAskTick - 1, false);
            }
        }
    }

    /**
     * Returns best bid price for UP (YES) token from explicit UP book if populated,
     * or derived via symmetric binary complementarity {@code P_bid_up = 1 - P_ask_down}.
     *
     * @return best UP bid price, or {@link Double#NaN} when neither UP bids nor DOWN asks exist
     */
    public double getBestBidUp() {
        if (bestUpBidTick >= 0) {
            return bestUpBidTick * TICK_INV;
        }
        return bestDownAskTick <= MAX_TICK
                ? Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - bestDownAskTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Returns best ask price for UP (YES) token from explicit UP book if populated,
     * or derived via symmetric binary complementarity {@code P_ask_up = 1 - P_bid_down}.
     *
     * @return best UP ask price, or {@link Double#NaN} when neither UP asks nor DOWN bids exist
     */
    public double getBestAskUp() {
        if (bestUpAskTick <= MAX_TICK) {
            return bestUpAskTick * TICK_INV;
        }
        return bestDownBidTick >= 0
                ? Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - bestDownBidTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Returns best bid price for DOWN (NO) token from explicit DOWN book if populated,
     * or derived via symmetric binary complementarity {@code P_bid_down = 1 - P_ask_up}.
     *
     * @return best DOWN bid price, or {@link Double#NaN} when neither DOWN bids nor UP asks exist
     */
    public double getBestBidDown() {
        if (bestDownBidTick >= 0) {
            return bestDownBidTick * TICK_INV;
        }
        return bestUpAskTick <= MAX_TICK
                ? Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - bestUpAskTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Returns best ask price for DOWN (NO) token from explicit DOWN book if populated,
     * or derived via symmetric binary complementarity {@code P_ask_down = 1 - P_bid_up}.
     *
     * @return best DOWN ask price, or {@link Double#NaN} when neither DOWN asks nor UP bids exist
     */
    public double getBestAskDown() {
        if (bestDownAskTick <= MAX_TICK) {
            return bestDownAskTick * TICK_INV;
        }
        return bestUpBidTick >= 0
                ? Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - bestUpBidTick) * TICK_INV)
                : Double.NaN;
    }

    /**
     * Returns top-of-book effective bid size for UP (or complementary DOWN ask size if UP bids empty).
     *
     * @return top-of-book UP bid size in shares
     */
    public double getBestBidSizeUp() {
        if (bestUpBidTick >= 0) {
            return Math.max(0.0, upBids[bestUpBidTick] - upBidsConsumed[bestUpBidTick]);
        }
        if (bestDownAskTick <= MAX_TICK) {
            return Math.max(0.0, downAsks[bestDownAskTick] - downAsksConsumed[bestDownAskTick]);
        }
        return 0.0;
    }

    /**
     * Returns top-of-book effective ask size for UP (or complementary DOWN bid size if UP asks empty).
     *
     * @return top-of-book UP ask size in shares
     */
    public double getBestAskSizeUp() {
        if (bestUpAskTick <= MAX_TICK) {
            return Math.max(0.0, upAsks[bestUpAskTick] - upAsksConsumed[bestUpAskTick]);
        }
        if (bestDownBidTick >= 0) {
            return Math.max(0.0, downBids[bestDownBidTick] - downBidsConsumed[bestDownBidTick]);
        }
        return 0.0;
    }

    /**
     * Returns top-of-book effective bid size for DOWN (or complementary UP ask size if DOWN bids empty).
     *
     * @return top-of-book DOWN bid size in shares
     */
    public double getBestBidSizeDown() {
        if (bestDownBidTick >= 0) {
            return Math.max(0.0, downBids[bestDownBidTick] - downBidsConsumed[bestDownBidTick]);
        }
        if (bestUpAskTick <= MAX_TICK) {
            return Math.max(0.0, upAsks[bestUpAskTick] - upAsksConsumed[bestUpAskTick]);
        }
        return 0.0;
    }

    /**
     * Returns top-of-book effective ask size for DOWN (or complementary UP bid size if DOWN asks empty).
     *
     * @return top-of-book DOWN ask size in shares
     */
    public double getBestAskSizeDown() {
        if (bestDownAskTick <= MAX_TICK) {
            return Math.max(0.0, downAsks[bestDownAskTick] - downAsksConsumed[bestDownAskTick]);
        }
        if (bestUpBidTick >= 0) {
            return Math.max(0.0, upBids[bestUpBidTick] - upBidsConsumed[bestUpBidTick]);
        }
        return 0.0;
    }

    /**
     * Calculates cumulative effective liquidity depth within 1 cent of best ask for UP
     * (falling back symmetrically to DOWN bids when explicit UP asks are empty).
     *
     * @return 1-cent effective ask depth for UP in shares
     */
    public double getDepth1cUp() {
        if (upCacheDirty) {
            refreshUpQuoteCache();
        }
        return cachedDepth1cUp;
    }

    /**
     * Calculates cumulative effective liquidity depth within 1 cent of best ask for DOWN
     * (falling back symmetrically to UP bids when explicit DOWN asks are empty).
     *
     * @return 1-cent effective ask depth for DOWN in shares
     */
    public double getDepth1cDown() {
        if (downCacheDirty) {
            refreshDownQuoteCache();
        }
        return cachedDepth1cDown;
    }

    /**
     * Checks whether both UP and DOWN sides currently have finite, strictly positive bid and ask prices,
     * allowing safe construction of an {@link OrderBookQuote}.
     *
     * @return {@code true} if a complete two-sided quote can be emitted
     */
    public boolean hasValidTwoSidedQuote() {
        double askUp = getBestAskUp();
        double bidUp = getBestBidUp();
        double askDown = getBestAskDown();
        double bidDown = getBestBidDown();
        return Double.isFinite(askUp) && askUp > 0.0
                && Double.isFinite(bidUp) && bidUp > 0.0
                && Double.isFinite(askDown) && askDown > 0.0
                && Double.isFinite(bidDown) && bidDown > 0.0;
    }

    /**
     * Converts current book state into a unified {@link OrderBookQuote} snapshot using cached 1-cent depths,
     * or returns {@code null} if {@link #hasValidTwoSidedQuote()} is {@code false}.
     *
     * @param timestampMs current clock timestamp in milliseconds UTC
     * @return constructed {@link OrderBookQuote}, or {@code null} when incomplete
     */
    public OrderBookQuote toOrderBookQuoteOrNull(long timestampMs) {
        double bAskUp = getBestAskUp();
        double bBidUp = getBestBidUp();
        double bAskDown = getBestAskDown();
        double bBidDown = getBestBidDown();
        if (!Double.isFinite(bAskUp) || bAskUp <= 0.0
                || !Double.isFinite(bBidUp) || bBidUp <= 0.0
                || !Double.isFinite(bAskDown) || bAskDown <= 0.0
                || !Double.isFinite(bBidDown) || bBidDown <= 0.0) {
            return null;
        }
        if (upCacheDirty) {
            refreshUpQuoteCache();
        }
        if (downCacheDirty) {
            refreshDownQuoteCache();
        }
        return new OrderBookQuote(
                bAskUp,
                bBidUp,
                bAskDown,
                bBidDown,
                cachedDepth1cUp,
                cachedDepth1cDown,
                timestampMs > 0L ? timestampMs : lastUpdateTimeMs
        );
    }

    /**
     * Converts current book state into a unified {@link OrderBookQuote} snapshot, throwing
     * {@link IllegalArgumentException} if two-sided BBO prices are not yet populated.
     *
     * @param timestampMs current clock timestamp in milliseconds UTC
     * @return constructed {@link OrderBookQuote}
     */
    public OrderBookQuote toOrderBookQuote(long timestampMs) {
        if (upCacheDirty) {
            refreshUpQuoteCache();
        }
        if (downCacheDirty) {
            refreshDownQuoteCache();
        }
        return new OrderBookQuote(
                getBestAskUp(),
                getBestBidUp(),
                getBestAskDown(),
                getBestBidDown(),
                cachedDepth1cUp,
                cachedDepth1cDown,
                timestampMs > 0L ? timestampMs : lastUpdateTimeMs
        );
    }

    /**
     * Zero-allocation query returning the total effective share quantity fillable for a marketable
     * {@code BUY} order at or below {@code limitPrice}.
     *
     * @param isUpToken  {@code true} for UP contract, {@code false} for DOWN contract
     * @param limitPrice maximum acceptable execution price in {@code (0.0, 1.0]}
     * @return fillable share quantity ({@code >= 0.0})
     */
    public double estimateBuyFillableShares(boolean isUpToken, double limitPrice) {
        if (!Double.isFinite(limitPrice) || limitPrice <= 0.0) {
            return 0.0;
        }
        int limitTick = priceToTick(limitPrice);
        if (isUpToken) {
            if (bestUpAskTick <= MAX_TICK) {
                return sumAskDepthUpTo(bestUpAskTick, Math.min(MAX_TICK, limitTick), upAsks, upAskMask, upAsksConsumed);
            }
            if (bestDownBidTick >= 0) {
                int minCompBidTick = Math.max(0, MAX_TICK - limitTick);
                return sumBidDepthDownTo(bestDownBidTick, minCompBidTick, downBids, downBidMask, downBidsConsumed);
            }
            return 0.0;
        } else {
            if (bestDownAskTick <= MAX_TICK) {
                return sumAskDepthUpTo(bestDownAskTick, Math.min(MAX_TICK, limitTick), downAsks, downAskMask, downAsksConsumed);
            }
            if (bestUpBidTick >= 0) {
                int minCompBidTick = Math.max(0, MAX_TICK - limitTick);
                return sumBidDepthDownTo(bestUpBidTick, minCompBidTick, upBids, upBidMask, upBidsConsumed);
            }
            return 0.0;
        }
    }

    /**
     * Zero-allocation query returning the volume-weighted average fill price (VWAP) for a marketable
     * {@code BUY} order of {@code requestedShares} capped at {@code limitPrice}, or {@link Double#NaN}
     * if zero shares are available at or below {@code limitPrice}.
     *
     * @param isUpToken       {@code true} for UP contract, {@code false} for DOWN contract
     * @param requestedShares desired share quantity
     * @param limitPrice      maximum acceptable execution price in {@code (0.0, 1.0]}
     * @return estimated VWAP fill price, or {@link Double#NaN} if unfillable
     */
    public double estimateBuyFillVwap(boolean isUpToken, double requestedShares, double limitPrice) {
        if (!Double.isFinite(requestedShares) || requestedShares <= 0.0
                || !Double.isFinite(limitPrice) || limitPrice <= 0.0) {
            return Double.NaN;
        }
        int limitTick = priceToTick(limitPrice);
        if (isUpToken) {
            if (bestUpAskTick <= MAX_TICK) {
                return computeAskVwap(bestUpAskTick, Math.min(MAX_TICK, limitTick), requestedShares, upAsks, upAskMask, upAsksConsumed);
            }
            if (bestDownBidTick >= 0) {
                return computeComplementBidVwap(bestDownBidTick, limitPrice, requestedShares, downBids, downBidMask, downBidsConsumed);
            }
            return Double.NaN;
        } else {
            if (bestDownAskTick <= MAX_TICK) {
                return computeAskVwap(bestDownAskTick, Math.min(MAX_TICK, limitTick), requestedShares, downAsks, downAskMask, downAsksConsumed);
            }
            if (bestUpBidTick >= 0) {
                return computeComplementBidVwap(bestUpBidTick, limitPrice, requestedShares, upBids, upBidMask, upBidsConsumed);
            }
            return Double.NaN;
        }
    }

    /**
     * Read-only simulation of order execution against current effective book depth without mutating
     * consumed liquidity arrays.
     *
     * @param isUpToken       {@code true} for UP contract, {@code false} for DOWN contract
     * @param isBuy           {@code true} for BUY (taker buying), {@code false} for SELL (taker selling)
     * @param requestedShares desired volume
     * @param limitPrice      maximum price for BUY or minimum price for SELL
     * @return execution details including average price, executed shares, and fee
     */
    public FillResult simulateFill(boolean isUpToken, boolean isBuy, double requestedShares, double limitPrice) {
        return fillOrder(isUpToken, isBuy, requestedShares, limitPrice, false);
    }

    /**
     * Executes an order against current effective book depth and depletes available liquidity at
     * every swept price level (including complementary UP/DOWN mirror levels).
     *
     * @param isUpToken       {@code true} for UP contract, {@code false} for DOWN contract
     * @param isBuy           {@code true} for BUY (taker buying), {@code false} for SELL (taker selling)
     * @param requestedShares desired volume
     * @param limitPrice      maximum price for BUY or minimum price for SELL
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

        if (!Double.isFinite(requestedShares) || requestedShares <= 0.0) {
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
                } else if (bestDownBidTick >= 0) {
                    int w = bestDownBidTick >>> 6;
                    long bits = downBidMask[w] & (-1L >>> (63 - (bestDownBidTick & 63)));
                    outer:
                    while (true) {
                        while (bits != 0L) {
                            int bit = 63 - Long.numberOfLeadingZeros(bits);
                            int t = (w << 6) + bit;
                            bits &= ~(1L << bit);
                            double available = downBids[t] - downBidsConsumed[t];
                            if (available <= EPS) {
                                continue;
                            }
                            double upPrice = Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - t) * TICK_INV);
                            if (upPrice > limitPrice + EPS) {
                                break outer;
                            }
                            double take = Math.min(remainingShares, available);
                            cost += take * upPrice;
                            executedShares += take;
                            remainingShares -= take;
                            if (consumeLiquidity) {
                                downBidsConsumed[t] += take;
                                downBidConsumedMask[w] |= (1L << bit);
                            }
                            if (remainingShares <= EPS) {
                                break outer;
                            }
                        }
                        if (--w < 0) {
                            break;
                        }
                        bits = downBidMask[w];
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
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
                } else if (bestDownAskTick <= MAX_TICK) {
                    int w = bestDownAskTick >>> 6;
                    long bits = downAskMask[w] & (-1L << (bestDownAskTick & 63));
                    outer:
                    while (true) {
                        while (bits != 0L) {
                            int bit = Long.numberOfTrailingZeros(bits);
                            int t = (w << 6) + bit;
                            bits &= (bits - 1L);
                            double available = downAsks[t] - downAsksConsumed[t];
                            if (available <= EPS) {
                                continue;
                            }
                            double upPrice = Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - t) * TICK_INV);
                            if (upPrice + EPS < limitPrice) {
                                break outer;
                            }
                            double take = Math.min(remainingShares, available);
                            cost += take * upPrice;
                            executedShares += take;
                            remainingShares -= take;
                            if (consumeLiquidity) {
                                downAsksConsumed[t] += take;
                                downAskConsumedMask[w] |= (1L << bit);
                            }
                            if (remainingShares <= EPS) {
                                break outer;
                            }
                        }
                        if (++w >= MASK_WORDS) {
                            break;
                        }
                        bits = downAskMask[w];
                    }
                    if (consumeLiquidity && executedShares > 0.0) {
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
                            double downPrice = Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - t) * TICK_INV);
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
                            double downPrice = Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - t) * TICK_INV);
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
        double fee = executedShares * DEFAULT_TAKER_FEE_RATE * avgPrice * Math.max(0.0, 1.0 - avgPrice);
        return new FillResult(avgPrice, executedShares, fee);
    }

    private static double sumAskDepthUpTo(
            int startTick,
            int maxTick,
            double[] asks,
            long[] askMask,
            double[] consumed) {
        if (startTick > maxTick || startTick > MAX_TICK) {
            return 0.0;
        }
        double sum = 0.0;
        int w = startTick >>> 6;
        int maxW = maxTick >>> 6;
        long bits = askMask[w] & (-1L << (startTick & 63));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                int t = (w << 6) + bit;
                if (t > maxTick) {
                    break outer;
                }
                bits &= (bits - 1L);
                double eff = asks[t] - consumed[t];
                if (eff > EPS) {
                    sum += eff;
                }
            }
            if (++w > maxW) {
                break;
            }
            bits = askMask[w];
        }
        return sum;
    }

    private static double sumBidDepthDownTo(
            int startTick,
            int minTick,
            double[] bids,
            long[] bidMask,
            double[] consumed) {
        if (startTick < minTick || startTick < 0) {
            return 0.0;
        }
        double sum = 0.0;
        int w = startTick >>> 6;
        int minW = minTick >>> 6;
        long bits = bidMask[w] & (-1L >>> (63 - (startTick & 63)));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = 63 - Long.numberOfLeadingZeros(bits);
                int t = (w << 6) + bit;
                if (t < minTick) {
                    break outer;
                }
                bits &= ~(1L << bit);
                double eff = bids[t] - consumed[t];
                if (eff > EPS) {
                    sum += eff;
                }
            }
            if (--w < minW) {
                break;
            }
            bits = bidMask[w];
        }
        return sum;
    }

    private static double computeAskVwap(
            int startTick,
            int maxTick,
            double requestedShares,
            double[] asks,
            long[] askMask,
            double[] consumed) {
        if (startTick > maxTick || startTick > MAX_TICK) {
            return Double.NaN;
        }
        double remaining = requestedShares;
        double cost = 0.0;
        double executed = 0.0;
        int w = startTick >>> 6;
        int maxW = maxTick >>> 6;
        long bits = askMask[w] & (-1L << (startTick & 63));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                int t = (w << 6) + bit;
                if (t > maxTick) {
                    break outer;
                }
                bits &= (bits - 1L);
                double eff = asks[t] - consumed[t];
                if (eff <= EPS) {
                    continue;
                }
                double take = Math.min(remaining, eff);
                cost += take * (t * TICK_INV);
                executed += take;
                remaining -= take;
                if (remaining <= EPS) {
                    break outer;
                }
            }
            if (++w > maxW) {
                break;
            }
            bits = askMask[w];
        }
        return executed > EPS ? (cost / executed) : Double.NaN;
    }

    private static double computeComplementBidVwap(
            int startBidTick,
            double limitPrice,
            double requestedShares,
            double[] bids,
            long[] bidMask,
            double[] consumed) {
        if (startBidTick < 0) {
            return Double.NaN;
        }
        double remaining = requestedShares;
        double cost = 0.0;
        double executed = 0.0;
        int w = startBidTick >>> 6;
        long bits = bidMask[w] & (-1L >>> (63 - (startBidTick & 63)));
        outer:
        while (true) {
            while (bits != 0L) {
                int bit = 63 - Long.numberOfLeadingZeros(bits);
                int t = (w << 6) + bit;
                bits &= ~(1L << bit);
                double eff = bids[t] - consumed[t];
                if (eff <= EPS) {
                    continue;
                }
                double compAskPrice = Math.max(MIN_COMPLEMENT_PRICE, (MAX_TICK - t) * TICK_INV);
                if (compAskPrice > limitPrice + EPS) {
                    break outer;
                }
                double take = Math.min(remaining, eff);
                cost += take * compAskPrice;
                executed += take;
                remaining -= take;
                if (remaining <= EPS) {
                    break outer;
                }
            }
            if (--w < 0) {
                break;
            }
            bits = bidMask[w];
        }
        return executed > EPS ? (cost / executed) : Double.NaN;
    }

    private void refreshUpQuoteCache() {
        if (bestUpAskTick <= MAX_TICK) {
            upUsesDownBidsComplement = false;
            int depthLimitTick = Math.min(MAX_TICK, bestUpAskTick + ONE_CENT_TICKS);
            cachedDepth1cUp = sumAskDepthUpTo(bestUpAskTick, depthLimitTick, upAsks, upAskMask, upAsksConsumed);
            upAskWatchMaxTick = depthLimitTick;
            upCacheDirty = false;
            return;
        }
        upUsesDownBidsComplement = true;
        upAskWatchMaxTick = MAX_TICK;
        if (bestDownBidTick < 0) {
            cachedDepth1cUp = 0.0;
            downBidWatchMinTick = 0;
            upCacheDirty = false;
            return;
        }
        int depthFloorTick = Math.max(0, bestDownBidTick - ONE_CENT_TICKS);
        cachedDepth1cUp = sumBidDepthDownTo(bestDownBidTick, depthFloorTick, downBids, downBidMask, downBidsConsumed);
        downBidWatchMinTick = depthFloorTick;
        upCacheDirty = false;
    }

    private void refreshDownQuoteCache() {
        if (bestDownAskTick <= MAX_TICK) {
            downUsesUpBidsComplement = false;
            int depthLimitTick = Math.min(MAX_TICK, bestDownAskTick + ONE_CENT_TICKS);
            cachedDepth1cDown = sumAskDepthUpTo(bestDownAskTick, depthLimitTick, downAsks, downAskMask, downAsksConsumed);
            downAskWatchMaxTick = depthLimitTick;
            downCacheDirty = false;
            return;
        }
        downUsesUpBidsComplement = true;
        downAskWatchMaxTick = MAX_TICK;
        if (bestUpBidTick < 0) {
            cachedDepth1cDown = 0.0;
            upBidWatchMinTick = 0;
            downCacheDirty = false;
            return;
        }
        int depthFloorTick = Math.max(0, bestUpBidTick - ONE_CENT_TICKS);
        cachedDepth1cDown = sumBidDepthDownTo(bestUpBidTick, depthFloorTick, upBids, upBidMask, upBidsConsumed);
        upBidWatchMinTick = depthFloorTick;
        downCacheDirty = false;
    }

    private void invalidateQuoteCache() {
        upCacheDirty = true;
        downCacheDirty = true;
    }

    private void pruneRawBidsAboveTick(
            double[] bids,
            long[] bidMask,
            double[] consumed,
            long[] consumedMask,
            int maxAllowedTick,
            boolean isUp) {
        int currentBest = isUp ? bestUpBidTick : bestDownBidTick;
        if (currentBest <= maxAllowedTick) {
            return;
        }
        int minKill = Math.max(0, maxAllowedTick + 1);
        int w = currentBest >>> 6;
        int minW = minKill >>> 6;
        boolean modified = false;
        while (w >= minW) {
            long wordMask = (w == minW) ? (-1L << (minKill & 63)) : -1L;
            long toClear = bidMask[w] & wordMask;
            if (toClear != 0L) {
                modified = true;
                int base = w << 6;
                long rem = toClear;
                while (rem != 0L) {
                    int bit = Long.numberOfTrailingZeros(rem);
                    int t = base + bit;
                    bids[t] = 0.0;
                    consumed[t] = 0.0;
                    rem &= (rem - 1L);
                }
                bidMask[w] &= ~toClear;
                consumedMask[w] &= ~toClear;
            }
            w--;
        }
        if (modified) {
            if (isUp) {
                bestUpBidTick = findNextBidFrom(maxAllowedTick, bids, bidMask, consumed);
            } else {
                bestDownBidTick = findNextBidFrom(maxAllowedTick, bids, bidMask, consumed);
            }
            invalidateQuoteCache();
        }
    }

    private void pruneRawAsksBelowTick(
            double[] asks,
            long[] askMask,
            double[] consumed,
            long[] consumedMask,
            int minAllowedTick,
            boolean isUp) {
        int currentBest = isUp ? bestUpAskTick : bestDownAskTick;
        if (currentBest >= minAllowedTick || currentBest > MAX_TICK) {
            return;
        }
        int maxKill = Math.min(MAX_TICK, minAllowedTick - 1);
        if (maxKill < 0) {
            return;
        }
        int w = currentBest >>> 6;
        int maxW = maxKill >>> 6;
        boolean modified = false;
        while (w <= maxW) {
            long wordMask = (w == maxW) ? (-1L >>> (63 - (maxKill & 63))) : -1L;
            long toClear = askMask[w] & wordMask;
            if (toClear != 0L) {
                modified = true;
                int base = w << 6;
                long rem = toClear;
                while (rem != 0L) {
                    int bit = Long.numberOfTrailingZeros(rem);
                    int t = base + bit;
                    asks[t] = 0.0;
                    consumed[t] = 0.0;
                    rem &= (rem - 1L);
                }
                askMask[w] &= ~toClear;
                consumedMask[w] &= ~toClear;
            }
            w++;
        }
        if (modified) {
            if (isUp) {
                bestUpAskTick = findNextAskFrom(minAllowedTick, asks, askMask, consumed);
            } else {
                bestDownAskTick = findNextAskFrom(minAllowedTick, asks, askMask, consumed);
            }
            invalidateQuoteCache();
        }
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

    /**
     * Execution outcome of a simulated L2 book sweep order.
     *
     * @param avgPrice       volume-weighted average fill price
     * @param executedShares total executed contract shares
     * @param fee            calculated taker fee in USD
     */
    public record FillResult(double avgPrice, double executedShares, double fee) {}
}

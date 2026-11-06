package cz.polymarket.bot.connector.polymarket.stream;

import java.util.Arrays;
import java.util.Optional;

/**
 * Stateful in-memory L2 orderbook tracker for a binary Polymarket market (Up / YES token and Down / NO token).
 * <p>
 * Processes full {@code book} snapshots, incremental {@code price_change} level updates, and
 * {@code best_bid_ask} top-of-book updates using zero-allocation primitive millicent arrays ({@code 1..999}),
 * producing normalized {@link PolymarketBboQuote} snapshots without boxing or {@code TreeMap} churn.
 */
public class PolymarketLocalBook {

    private static final int BOOK_LEVELS = 1000;

    private final String yesTokenId;
    private final String noTokenId;

    // Indexed by price in millicents (1..999, where 540 = $0.540), value = size in shares
    private final double[] yesBids = new double[BOOK_LEVELS];
    private final double[] yesAsks = new double[BOOK_LEVELS];
    private final double[] noBids = new double[BOOK_LEVELS];
    private final double[] noAsks = new double[BOOK_LEVELS];

    private int bestYesBidMc = 0;
    private int bestYesAskMc = BOOK_LEVELS;
    private int bestNoBidMc = 0;
    private int bestNoAskMc = BOOK_LEVELS;

    /**
     * Constructs a local orderbook tracker for the given Up ({@code yesTokenId}) and Down ({@code noTokenId}) tokens.
     *
     * @param yesTokenId Up / YES conditional token ID
     * @param noTokenId  Down / NO conditional token ID
     */
    public PolymarketLocalBook(String yesTokenId, String noTokenId) {
        this.yesTokenId = yesTokenId != null ? yesTokenId.trim() : "";
        this.noTokenId = noTokenId != null ? noTokenId.trim() : "";
    }

    /**
     * Returns the tracked Up (YES) token ID.
     *
     * @return Up token ID
     */
    public String getYesTokenId() {
        return yesTokenId;
    }

    /**
     * Returns the tracked Down (NO) token ID.
     *
     * @return Down token ID
     */
    public String getNoTokenId() {
        return noTokenId;
    }

    /**
     * Clears all bids and asks for the specified {@code assetId} before applying a full {@code book} snapshot.
     *
     * @param assetId conditional token ID (or blank to default to {@code yesTokenId})
     */
    public synchronized void clearAssetBook(String assetId) {
        if (isNoAsset(assetId)) {
            Arrays.fill(noBids, 0.0);
            Arrays.fill(noAsks, 0.0);
            bestNoBidMc = 0;
            bestNoAskMc = BOOK_LEVELS;
        } else if (isYesAsset(assetId)) {
            Arrays.fill(yesBids, 0.0);
            Arrays.fill(yesAsks, 0.0);
            bestYesBidMc = 0;
            bestYesAskMc = BOOK_LEVELS;
        }
    }

    /**
     * Updates a single price level on the specified side and asset.
     * If {@code size <= 0.0}, the price level is removed.
     *
     * @param assetId conditional token ID
     * @param isBid   {@code true} for bid/BUY, {@code false} for ask/SELL
     * @param price   price in {@code (0.0, 1.0)}
     * @param size    share quantity at this level
     */
    public synchronized void updateLevel(String assetId, boolean isBid, double price, double size) {
        int priceMc = toPriceMc(price);
        if (priceMc <= 0 || priceMc >= BOOK_LEVELS) {
            return;
        }
        if (isNoAsset(assetId)) {
            if (isBid) {
                bestNoBidMc = applyBidLevel(noBids, bestNoBidMc, priceMc, size);
            } else {
                bestNoAskMc = applyAskLevel(noAsks, bestNoAskMc, priceMc, size);
            }
        } else if (isYesAsset(assetId)) {
            if (isBid) {
                bestYesBidMc = applyBidLevel(yesBids, bestYesBidMc, priceMc, size);
            } else {
                bestYesAskMc = applyAskLevel(yesAsks, bestYesAskMc, priceMc, size);
            }
        }
    }

    /**
     * Applies a direct {@code best_bid_ask} top-of-book override for {@code assetId}, pruning any stale crossed levels.
     *
     * @param assetId conditional token ID
     * @param bestBid best bid price in {@code (0.0, 1.0)}
     * @param bestAsk best ask price in {@code (0.0, 1.0)}
     */
    public synchronized void applyBestBidAsk(String assetId, double bestBid, double bestAsk) {
        int bidMc = toPriceMc(bestBid);
        int askMc = toPriceMc(bestAsk);
        if (isNoAsset(assetId)) {
            if (bidMc > 0 && bidMc < BOOK_LEVELS) {
                bestNoBidMc = pruneAndSetBestBid(noBids, bestNoBidMc, bidMc);
            }
            if (askMc > 0 && askMc < BOOK_LEVELS) {
                bestNoAskMc = pruneAndSetBestAsk(noAsks, bestNoAskMc, askMc);
            }
        } else if (isYesAsset(assetId)) {
            if (bidMc > 0 && bidMc < BOOK_LEVELS) {
                bestYesBidMc = pruneAndSetBestBid(yesBids, bestYesBidMc, bidMc);
            }
            if (askMc > 0 && askMc < BOOK_LEVELS) {
                bestYesAskMc = pruneAndSetBestAsk(yesAsks, bestYesAskMc, askMc);
            }
        }
    }

    /**
     * Builds the current {@link PolymarketBboQuote} snapshot if valid top-of-book prices are available.
     * If only one token's book has been populated so far, complements the other token using binary parity
     * ({@code bidDown = 1 - askUp}, {@code askDown = 1 - bidUp}).
     *
     * @param timestampMs local receipt timestamp in epoch milliseconds
     * @return current {@link PolymarketBboQuote} if available
     */
    public synchronized Optional<PolymarketBboQuote> snapshotQuote(long timestampMs) {
        boolean hasYes = bestYesBidMc > 0 && bestYesAskMc < BOOK_LEVELS;
        boolean hasNo = bestNoBidMc > 0 && bestNoAskMc < BOOK_LEVELS;
        if (!hasYes && !hasNo) {
            return Optional.empty();
        }

        double bestBidUp;
        double bestAskUp;
        double askSizeUp;
        double bestBidDown;
        double bestAskDown;
        double askSizeDown;

        if (hasYes) {
            bestBidUp = bestYesBidMc / 1000.0;
            bestAskUp = bestYesAskMc / 1000.0;
            askSizeUp = yesAsks[bestYesAskMc];
        } else {
            bestBidUp = Math.max(0.001, (BOOK_LEVELS - bestNoAskMc) / 1000.0);
            bestAskUp = Math.min(0.999, (BOOK_LEVELS - bestNoBidMc) / 1000.0);
            askSizeUp = noBids[bestNoBidMc];
        }

        if (hasNo) {
            bestBidDown = bestNoBidMc / 1000.0;
            bestAskDown = bestNoAskMc / 1000.0;
            askSizeDown = noAsks[bestNoAskMc];
        } else {
            bestBidDown = Math.max(0.001, (BOOK_LEVELS - bestYesAskMc) / 1000.0);
            bestAskDown = Math.min(0.999, (BOOK_LEVELS - bestYesBidMc) / 1000.0);
            askSizeDown = yesBids[bestYesBidMc];
        }

        double midUp = (bestBidUp + bestAskUp) * 0.5;
        double midDown = (bestBidDown + bestAskDown) * 0.5;

        return Optional.of(new PolymarketBboQuote(
                bestAskUp,
                bestBidUp,
                bestAskDown,
                bestBidDown,
                askSizeUp,
                askSizeDown,
                midUp,
                midDown,
                timestampMs
        ));
    }

    private static int applyBidLevel(double[] bids, int currentBestBidMc, int priceMc, double size) {
        if (size <= 1e-9 || !Double.isFinite(size)) {
            bids[priceMc] = 0.0;
            if (priceMc == currentBestBidMc) {
                for (int p = priceMc - 1; p >= 1; p--) {
                    if (bids[p] > 0.0) {
                        return p;
                    }
                }
                return 0;
            }
            return currentBestBidMc;
        }
        bids[priceMc] = size;
        return Math.max(currentBestBidMc, priceMc);
    }

    private static int applyAskLevel(double[] asks, int currentBestAskMc, int priceMc, double size) {
        if (size <= 1e-9 || !Double.isFinite(size)) {
            asks[priceMc] = 0.0;
            if (priceMc == currentBestAskMc) {
                for (int p = priceMc + 1; p < BOOK_LEVELS; p++) {
                    if (asks[p] > 0.0) {
                        return p;
                    }
                }
                return BOOK_LEVELS;
            }
            return currentBestAskMc;
        }
        asks[priceMc] = size;
        return Math.min(currentBestAskMc, priceMc);
    }

    private static int pruneAndSetBestBid(double[] bids, int currentBestBidMc, int bidMc) {
        if (currentBestBidMc == bidMc && bids[bidMc] > 0.0) {
            return bidMc;
        }
        int upper = Math.min(BOOK_LEVELS - 1, Math.max(currentBestBidMc, bidMc));
        for (int p = bidMc + 1; p <= upper; p++) {
            bids[p] = 0.0;
        }
        if (bids[bidMc] <= 0.0) {
            bids[bidMc] = 100.0;
        }
        return bidMc;
    }

    private static int pruneAndSetBestAsk(double[] asks, int currentBestAskMc, int askMc) {
        if (currentBestAskMc == askMc && asks[askMc] > 0.0) {
            return askMc;
        }
        int lower = Math.max(1, Math.min(currentBestAskMc, askMc));
        for (int p = lower; p < askMc; p++) {
            asks[p] = 0.0;
        }
        if (asks[askMc] <= 0.0) {
            asks[askMc] = 100.0;
        }
        return askMc;
    }

    private boolean isYesAsset(String assetId) {
        if (assetId == null || assetId.isBlank() || yesTokenId.isEmpty()) {
            return true;
        }
        return yesTokenId.equals(assetId.trim());
    }

    private boolean isNoAsset(String assetId) {
        if (assetId == null || assetId.isBlank() || noTokenId.isEmpty()) {
            return false;
        }
        return noTokenId.equals(assetId.trim());
    }

    private static int toPriceMc(double price) {
        if (!Double.isFinite(price) || price <= 0.0 || price >= 1.0) {
            return 0;
        }
        return (int) Math.round(price * 1000.0);
    }
}

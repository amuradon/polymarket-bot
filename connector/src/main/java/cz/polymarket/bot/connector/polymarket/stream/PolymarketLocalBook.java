package cz.polymarket.bot.connector.polymarket.stream;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Stateful in-memory L2 orderbook tracker for a binary Polymarket market (Up / YES token and Down / NO token).
 * <p>
 * Processes full {@code book} snapshots, incremental {@code price_change} level updates, and
 * {@code best_bid_ask} top-of-book updates, producing normalized {@link PolymarketBboQuote} snapshots.
 */
public class PolymarketLocalBook {

    private final String yesTokenId;
    private final String noTokenId;

    // Keyed by price in millicents (1..999, where 540 = $0.540), value = size in shares
    private final NavigableMap<Integer, Double> yesBids = new TreeMap<>();
    private final NavigableMap<Integer, Double> yesAsks = new TreeMap<>();
    private final NavigableMap<Integer, Double> noBids = new TreeMap<>();
    private final NavigableMap<Integer, Double> noAsks = new TreeMap<>();

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
            noBids.clear();
            noAsks.clear();
        } else if (isYesAsset(assetId)) {
            yesBids.clear();
            yesAsks.clear();
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
        if (priceMc <= 0 || priceMc >= 1000) {
            return;
        }
        NavigableMap<Integer, Double> targetMap = selectMap(assetId, isBid);
        if (targetMap == null) {
            return;
        }
        if (size <= 1e-9 || !Double.isFinite(size)) {
            targetMap.remove(priceMc);
        } else {
            targetMap.put(priceMc, size);
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
        NavigableMap<Integer, Double> bids = selectMap(assetId, true);
        NavigableMap<Integer, Double> asks = selectMap(assetId, false);
        if (bids == null || asks == null) {
            return;
        }
        int bidMc = toPriceMc(bestBid);
        int askMc = toPriceMc(bestAsk);
        if (bidMc > 0 && bidMc < 1000) {
            bids.tailMap(bidMc, false).clear();
            bids.putIfAbsent(bidMc, 100.0);
        }
        if (askMc > 0 && askMc < 1000) {
            asks.headMap(askMc, false).clear();
            asks.putIfAbsent(askMc, 100.0);
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
        var bestYesBidEntry = yesBids.lastEntry();
        var bestYesAskEntry = yesAsks.firstEntry();
        var bestNoBidEntry = noBids.lastEntry();
        var bestNoAskEntry = noAsks.firstEntry();

        boolean hasYes = bestYesBidEntry != null && bestYesAskEntry != null;
        boolean hasNo = bestNoBidEntry != null && bestNoAskEntry != null;
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
            bestBidUp = bestYesBidEntry.getKey() / 1000.0;
            bestAskUp = bestYesAskEntry.getKey() / 1000.0;
            askSizeUp = bestYesAskEntry.getValue();
        } else {
            bestBidUp = Math.max(0.001, (1000 - bestNoAskEntry.getKey()) / 1000.0);
            bestAskUp = Math.min(0.999, (1000 - bestNoBidEntry.getKey()) / 1000.0);
            askSizeUp = bestNoBidEntry.getValue();
        }

        if (hasNo) {
            bestBidDown = bestNoBidEntry.getKey() / 1000.0;
            bestAskDown = bestNoAskEntry.getKey() / 1000.0;
            askSizeDown = bestNoAskEntry.getValue();
        } else {
            bestBidDown = Math.max(0.001, (1000 - bestYesAskEntry.getKey()) / 1000.0);
            bestAskDown = Math.min(0.999, (1000 - bestYesBidEntry.getKey()) / 1000.0);
            askSizeDown = bestYesBidEntry.getValue();
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

    private NavigableMap<Integer, Double> selectMap(String assetId, boolean isBid) {
        if (isNoAsset(assetId)) {
            return isBid ? noBids : noAsks;
        }
        if (isYesAsset(assetId)) {
            return isBid ? yesBids : yesAsks;
        }
        return null;
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

package cz.polymarket.bot.connector.polymarket.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link PolymarketStreamParser} and {@link ClobBookSink} verifying PolyBolt v2
 * snapshot/live tick parsing, 1-bps CLOB L2 streaming, per-side server timestamp filtering,
 * and non-fabricating BBO crossed-level pruning.
 */
class PolymarketStreamParserTest {

    private final PolymarketStreamParser parser = new PolymarketStreamParser(new ObjectMapper(), "BTCUSDT");

    /**
     * Minimal self-contained 1-bps test sink inside {@code connector} tests (since {@code connector}
     * never depends on {@code common}).
     */
    private static final class RecordingBookSink implements ClobBookSink {
        private final String upToken;
        private final String downToken;
        private final byte[] upBytes;
        private final byte[] downBytes;
        private final double[] upBids = new double[10_001];
        private final double[] upAsks = new double[10_001];
        private final double[] downBids = new double[10_001];
        private final double[] downAsks = new double[10_001];
        private long lastUpServerTs = -1L;
        private long lastDownServerTs = -1L;
        private long lastLatencyMs = 0L;

        RecordingBookSink(String upToken, String downToken) {
            this.upToken = upToken;
            this.downToken = downToken;
            this.upBytes = upToken.getBytes(StandardCharsets.UTF_8);
            this.downBytes = downToken.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getUpTokenBytes() {
            return upBytes;
        }

        @Override
        public byte[] getDownTokenBytes() {
            return downBytes;
        }

        @Override
        public int resolveTokenSide(String assetId) {
            if (upToken.equals(assetId)) return 1;
            if (downToken.equals(assetId)) return 0;
            return -1;
        }

        @Override
        public boolean tryAcceptServerTimestamp(boolean isUp, long serverTimestampMs) {
            if (serverTimestampMs <= 0L) return true;
            if (isUp) {
                if (serverTimestampMs < lastUpServerTs) return false;
                lastUpServerTs = serverTimestampMs;
                return true;
            } else {
                if (serverTimestampMs < lastDownServerTs) return false;
                lastDownServerTs = serverTimestampMs;
                return true;
            }
        }

        @Override
        public void updateObservedLatency(long receiptTimestampMs, long serverTimestampMs) {
            if (receiptTimestampMs >= serverTimestampMs && serverTimestampMs > 0L) {
                lastLatencyMs = receiptTimestampMs - serverTimestampMs;
            }
        }

        @Override
        public void onBookSnapshotStart(boolean isUp) {
            if (isUp) {
                Arrays.fill(upBids, 0.0);
                Arrays.fill(upAsks, 0.0);
            } else {
                Arrays.fill(downBids, 0.0);
                Arrays.fill(downAsks, 0.0);
            }
        }

        @Override
        public void onBookSnapshotLevel(boolean isUp, boolean isBid, int tick, double size) {
            onPriceChangeLevel(isUp, isBid, tick, size, 0L);
        }

        @Override
        public void onBookSnapshotEnd(boolean isUp, long recvTimestampMs) {
        }

        @Override
        public void onPriceChangeLevel(boolean isUp, boolean isBid, int tick, double size, long recvTimestampMs) {
            double[] arr = isUp ? (isBid ? upBids : upAsks) : (isBid ? downBids : downAsks);
            arr[tick] = size > 0.0 ? size : 0.0;
        }

        @Override
        public void onBboHint(boolean isUp, int bestBidTick, int bestAskTick, long recvTimestampMs) {
            double[] bids = isUp ? upBids : downBids;
            double[] asks = isUp ? upAsks : downAsks;
            if (bestBidTick >= 0) {
                for (int t = bestBidTick + 1; t <= 10_000; t++) bids[t] = 0.0;
            }
            if (bestAskTick >= 0) {
                for (int t = 0; t < bestAskTick; t++) asks[t] = 0.0;
            }
        }

        double bestBid(boolean isUp) {
            double[] b = isUp ? upBids : downBids;
            for (int t = 10_000; t >= 0; t--) if (b[t] > 0.0) return t * 0.0001;
            return Double.NaN;
        }

        double bestAsk(boolean isUp) {
            double[] a = isUp ? upAsks : downAsks;
            for (int t = 0; t <= 10_000; t++) if (a[t] > 0.0) return t * 0.0001;
            return Double.NaN;
        }

        double askSizeAt(boolean isUp, int tick) {
            return (isUp ? upAsks : downAsks)[tick];
        }
    }

    @Test
    void shouldParsePolyBoltInitialSnapshotAndLiveTwapAndSpotFrames() {
        String snapshotJson = """
                {
                  "channel": "price.crypto",
                  "snapshot": true,
                  "payload": {
                    "source": "chainlink",
                    "symbol": "btcusd",
                    "data": [
                      {"full_accuracy_value": "74200.12345678", "value": 74200.12, "timestamp": 1792741500000},
                      {"full_accuracy_value": "74205.50000000", "value": 74205.50, "timestamp": 1792741501000}
                    ]
                  }
                }
                """;

        List<PolyBoltTick> snapshotTicks = parser.parsePolyBoltFrame(snapshotJson, 1792741502000L);
        assertThat(snapshotTicks).hasSize(2);
        assertThat(snapshotTicks.get(0).isTwap()).isFalse();
        assertThat(snapshotTicks.get(0).price()).isCloseTo(74200.12345678, within(1e-8));
        assertThat(snapshotTicks.get(0).timestampMs()).isEqualTo(1792741500000L);
        assertThat(snapshotTicks.get(1).price()).isCloseTo(74205.50, within(1e-6));

        String liveTwapJson = """
                {
                  "v": 1,
                  "channel": "price.crypto.twap",
                  "seq": 10,
                  "ts": 1792741510000,
                  "payload": {
                    "symbol": "btcusd",
                    "source": "chainlink",
                    "window_seconds": 60,
                    "value": 74210.25,
                    "full_accuracy_value": "74210.25678900",
                    "timestamp": 1792741510000
                  }
                }
                """;

        List<PolyBoltTick> twapTicks = parser.parsePolyBoltFrame(liveTwapJson, 1792741510050L);
        assertThat(twapTicks).hasSize(1);
        PolyBoltTick twap = twapTicks.getFirst();
        assertThat(twap.isTwap()).isTrue();
        assertThat(twap.windowSeconds()).isEqualTo(60);
        assertThat(twap.price()).isCloseTo(74210.256789, within(1e-8));
        assertThat(twap.timestampMs()).isEqualTo(1792741510050L);
        assertThat(twap.oracleTimestampMs()).isEqualTo(1792741510000L);

        assertThat(parser.parsePolyBoltFrame("{\"op\":\"authed\",\"rid\":\"a1\"}", 1000L)).isEmpty();
    }

    @Test
    void shouldStream1BpsBookSnapshotsPriceChangesAndPruneBboWithoutFabricating100Shares() {
        RecordingBookSink sink = new RecordingBookSink("token_up_1", "token_down_1");

        String bookFrame = """
                [
                  {
                    "event_type": "book",
                    "asset_id": "token_up_1",
                    "bids": [{"price": "0.525", "size": "120"}, {"price": "0.54", "size": "80"}],
                    "asks": [{"price": "0.57", "size": "90"}, {"price": "0.56", "size": "60"}],
                    "timestamp": "1792741504985"
                  },
                  {
                    "event_type": "book",
                    "asset_id": "token_down_1",
                    "bids": [{"price": "0.43", "size": "70"}, {"price": "0.44", "size": "60"}],
                    "asks": [{"price": "0.46", "size": "80"}, {"price": "0.48", "size": "120"}],
                    "timestamp": "1792741504982"
                  }
                ]
                """;

        // Note: token_down_1 has server_ts 4982 < token_up_1's 4985 — BOTH must be accepted!
        assertThat(parser.applyClobFrame(bookFrame, 1792741505000L, sink)).isTrue();
        assertThat(sink.bestBid(true)).isCloseTo(0.54, within(1e-6));
        assertThat(sink.bestAsk(true)).isCloseTo(0.56, within(1e-6));
        assertThat(sink.bestBid(false)).isCloseTo(0.44, within(1e-6));
        assertThat(sink.bestAsk(false)).isCloseTo(0.46, within(1e-6));
        assertThat(sink.askSizeAt(true, 5600)).isCloseTo(60.0, within(1e-6));
        assertThat(sink.lastLatencyMs).isEqualTo(18L);

        // best_bid_ask moves best_ask to 0.58: stale 0.56 & 0.57 asks are pruned, and 0.58 is NOT fabricated with 100 shares!
        String bestBidAskFrame = """
                {
                  "event_type": "best_bid_ask",
                  "asset_id": "token_up_1",
                  "best_bid": "0.54",
                  "best_ask": "0.58",
                  "timestamp": "1792741508000"
                }
                """;
        assertThat(parser.applyClobFrame(bestBidAskFrame, 1792741508010L, sink)).isTrue();
        assertThat(sink.askSizeAt(true, 5600)).isZero();
        assertThat(sink.askSizeAt(true, 5700)).isZero();
        assertThat(sink.askSizeAt(true, 5800)).isZero();

        // Stale frame on UP (server_ts 1792741507500 < 1792741508000) is rejected before mutating book
        String staleUpFrame = """
                {
                  "bids": [{"price": "0.50", "size": "999"}],
                  "asks": [{"price": "0.51", "size": "999"}],
                  "event_type": "book",
                  "asset_id": "token_up_1",
                  "timestamp": "1792741507500"
                }
                """;
        assertThat(parser.applyClobFrame(staleUpFrame, 1792741508020L, sink)).isFalse();
        assertThat(sink.askSizeAt(true, 5100)).isZero();

        // Meanwhile DOWN frame with server_ts 1792741507600 (> DOWN's 4982, even though < UP's 8000) MUST succeed!
        String validDownDelta = """
                {
                  "event_type": "price_change",
                  "price_changes": [
                    {"asset_id": "token_down_1", "price": "0.455", "size": "140", "side": "SELL"}
                  ],
                  "timestamp": "1792741507600"
                }
                """;
        assertThat(parser.applyClobFrame(validDownDelta, 1792741508025L, sink)).isTrue();
        assertThat(sink.bestAsk(false)).isCloseTo(0.455, within(1e-6));
    }

    @Test
    void shouldFastForwardOlderFramesBeforeFullDualBookSnapshotInBatch() {
        RecordingBookSink sink = new RecordingBookSink("token_up_batch", "token_down_batch");

        TimestampedFrame obsolete1 = new TimestampedFrame(
                "{\"event_type\":\"best_bid_ask\",\"asset_id\":\"token_up_batch\",\"best_bid\":\"0.20\",\"best_ask\":\"0.21\"}",
                1000L
        );
        TimestampedFrame obsolete2 = new TimestampedFrame(
                "{\"event_type\":\"best_bid_ask\",\"asset_id\":\"token_down_batch\",\"best_bid\":\"0.79\",\"best_ask\":\"0.80\"}",
                1001L
        );
        TimestampedFrame fullBookSnapshot = new TimestampedFrame(
                """
                [
                  {
                    "bids": [{"price": "0.60", "size": "200"}],
                    "asks": [{"price": "0.62", "size": "150"}],
                    "event_type": "book",
                    "asset_id": "token_up_batch"
                  },
                  {
                    "bids": [{"price": "0.38", "size": "200"}],
                    "asks": [{"price": "0.40", "size": "150"}],
                    "asset_id": "token_down_batch",
                    "event_type": "book"
                  }
                ]
                """,
                1002L
        );
        TimestampedFrame postSnapshotDelta = new TimestampedFrame(
                """
                {
                  "event_type": "price_change",
                  "price_changes": [
                    {"asset_id": "token_up_batch", "price": "0.61", "size": "300", "side": "SELL", "best_bid": "0.60", "best_ask": "0.61"}
                  ]
                }
                """,
                1003L
        );

        PolymarketStreamParser.ClobBatchResult result = parser.applyClobFrameBatch(
                List.of(obsolete1, obsolete2, fullBookSnapshot, postSnapshotDelta),
                sink
        );

        assertThat(result.updated()).isTrue();
        assertThat(result.fastForwardedFramesCount()).isEqualTo(2);
        assertThat(result.appliedFramesCount()).isEqualTo(2);
        assertThat(result.lastUpdatedRecvTs()).isEqualTo(1003L);
        assertThat(sink.bestBid(true)).isCloseTo(0.60, within(1e-6));
        assertThat(sink.bestAsk(true)).isCloseTo(0.61, within(1e-6));
        assertThat(sink.bestBid(false)).isCloseTo(0.38, within(1e-6));
        assertThat(sink.bestAsk(false)).isCloseTo(0.40, within(1e-6));
    }

    @Test
    void shouldParseUserChannelBuyAndSellTradeEventsAndSortPolyBoltSnapshots() {
        String singleBuyTrade = """
                {
                  "event_type": "trade",
                  "id": "trade-buy-1",
                  "asset_id": "token_up_1",
                  "taker_order_id": "0xorder-buy-1",
                  "side": "BUY",
                  "size": "100.0",
                  "price": "0.55",
                  "fee_rate_bps": "700",
                  "status": "MATCHED",
                  "trader_side": "TAKER",
                  "timestamp": "1792741510000"
                }
                """;
        List<PolymarketUserTradeEvent> buyEvents = parser.parseUserTradeEvents(singleBuyTrade);
        assertThat(buyEvents).hasSize(1);
        assertThat(buyEvents.getFirst().orderId()).isEqualTo("0xorder-buy-1");

        String descendingSnapshotJson = """
                {
                  "channel": "price.crypto.twap",
                  "snapshot": true,
                  "payload": {
                    "source": "chainlink",
                    "symbol": "btcusd",
                    "window_seconds": 60,
                    "data": [
                      {"full_accuracy_value": "91015.00000000", "value": 91015.00, "timestamp": 1794240011000},
                      {"full_accuracy_value": "91005.00000000", "value": 91005.00, "timestamp": 1794240000000}
                    ]
                  }
                }
                """;
        List<PolyBoltTick> ticks = parser.parsePolyBoltFrame(descendingSnapshotJson, 1794240011797L);
        assertThat(ticks).extracting(PolyBoltTick::oracleTimestampMs)
                .containsExactly(1794240000000L, 1794240011000L);
    }
}

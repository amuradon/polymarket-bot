package cz.polymarket.bot.backtest.indicator;

import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BacktestIndicator} adapter for {@link FastSpotMicroIndicator}.
 * <p>
 * Computes and caches daily fast direct-USD micro-VWAP prices, 1s 3-venue signed OFI, and 500ms
 * 3-venue synchronized directional impulses coalesced into causal {@code coalesceWindowMs}
 * buckets in typed 5-column Zstandard-compressed Parquet files
 * ({@code ts}, {@code seq}, {@code fast_direct_usd}, {@code signed_ofi_1s}, {@code sync_move_500_usd}).
 */
@ApplicationScoped
public class FastSpotMicroBacktestIndicator implements BacktestIndicator {

    private static final AtomicLong STAGING_TABLE_SEQ = new AtomicLong(0L);
    private static final int INITIAL_BUFFER_CAPACITY = 32_768;

    private final FastSpotMicroIndicatorConfig config;

    /**
     * Constructs a {@link FastSpotMicroBacktestIndicator} with the injected configuration bean.
     *
     * @param config indicator configuration
     */
    @Inject
    public FastSpotMicroBacktestIndicator(FastSpotMicroIndicatorConfig config) {
        this.config = FastSpotMicroIndicatorConfig.unwrap(config);
    }

    @Override
    public String getName() {
        return FastSpotMicroIndicator.INDICATOR_NAME;
    }

    @Override
    public Map<String, Object> getConfigurationParameters() {
        return config.toParameterMap();
    }

    @Override
    public long getSpotWarmupMs() {
        return config.spotWarmupMs();
    }

    @Override
    public long getUsdtPegWarmupMs() {
        return config.krakenUsdtWarmupMs();
    }

    /**
     * Returns the underlying indicator configuration.
     *
     * @return {@link FastSpotMicroIndicatorConfig}
     */
    public FastSpotMicroIndicatorConfig getConfig() {
        return config;
    }

    @Override
    public ComputationSession createComputationSession(String cryptoSymbol) {
        return new FastSpotMicroComputationSession(config, cryptoSymbol);
    }

    @Override
    public DailyIndicatorTrack readCachedTrack(Connection connection, Path parquetFile, String cryptoSymbol)
            throws SQLException {
        String normPath = parquetFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        String sql = "SELECT ts, seq, fast_direct_usd, signed_ofi_1s, sync_move_500_usd " +
                "FROM read_parquet('" + normPath + "') ORDER BY ts ASC, seq ASC";

        long[] timestampsMs = new long[INITIAL_BUFFER_CAPACITY];
        long[] sequences = new long[INITIAL_BUFFER_CAPACITY];
        double[] fastDirectUsds = new double[INITIAL_BUFFER_CAPACITY];
        double[] signedOfis = new double[INITIAL_BUFFER_CAPACITY];
        double[] syncMoves = new double[INITIAL_BUFFER_CAPACITY];
        int count = 0;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                if (count == timestampsMs.length) {
                    int newCap = timestampsMs.length << 1;
                    timestampsMs = Arrays.copyOf(timestampsMs, newCap);
                    sequences = Arrays.copyOf(sequences, newCap);
                    fastDirectUsds = Arrays.copyOf(fastDirectUsds, newCap);
                    signedOfis = Arrays.copyOf(signedOfis, newCap);
                    syncMoves = Arrays.copyOf(syncMoves, newCap);
                }
                timestampsMs[count] = rs.getLong(1);
                sequences[count] = rs.getLong(2);
                fastDirectUsds[count] = rs.getDouble(3);
                signedOfis[count] = rs.getDouble(4);
                syncMoves[count] = rs.getDouble(5);
                count++;
            }
        }

        return new FastSpotMicroDailyTrack(
                cryptoSymbol,
                count,
                Arrays.copyOf(timestampsMs, count),
                Arrays.copyOf(sequences, count),
                Arrays.copyOf(fastDirectUsds, count),
                Arrays.copyOf(signedOfis, count),
                Arrays.copyOf(syncMoves, count)
        );
    }

    /**
     * Columnar daily track of {@link BacktestEvent.CalculatedFastSpotMicroEvent} records.
     */
    static final class FastSpotMicroDailyTrack implements DailyIndicatorTrack {

        private final String cryptoSymbol;
        private final int count;
        private final long[] timestampsMs;
        private final long[] sequences;
        private final double[] fastDirectUsds;
        private final double[] signedOfis;
        private final double[] syncMoves;

        FastSpotMicroDailyTrack(
                String cryptoSymbol,
                int count,
                long[] timestampsMs,
                long[] sequences,
                double[] fastDirectUsds,
                double[] signedOfis,
                double[] syncMoves) {
            this.cryptoSymbol = cryptoSymbol;
            this.count = count;
            this.timestampsMs = timestampsMs;
            this.sequences = sequences;
            this.fastDirectUsds = fastDirectUsds;
            this.signedOfis = signedOfis;
            this.syncMoves = syncMoves;
        }

        @Override
        public String indicatorName() {
            return FastSpotMicroIndicator.INDICATOR_NAME;
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public long timestampMs(int index) {
            return timestampsMs[index];
        }

        @Override
        public BacktestEvent eventAt(int index) {
            return new BacktestEvent.CalculatedFastSpotMicroEvent(
                    timestampsMs[index],
                    sequences[index],
                    cryptoSymbol,
                    fastDirectUsds[index],
                    signedOfis[index],
                    syncMoves[index]
            );
        }
    }

    private static final class FastSpotMicroComputationSession implements ComputationSession {

        private final FastSpotMicroIndicator indicator;
        private final String cryptoSymbol;
        private final long coalesceWindowMs;

        private long[] timestampsMs = new long[INITIAL_BUFFER_CAPACITY];
        private long[] sequences = new long[INITIAL_BUFFER_CAPACITY];
        private double[] fastDirectUsds = new double[INITIAL_BUFFER_CAPACITY];
        private double[] signedOfis = new double[INITIAL_BUFFER_CAPACITY];
        private double[] syncMoves = new double[INITIAL_BUFFER_CAPACITY];
        private int count = 0;
        private long seq = 0L;

        private long activeBucket = -1L;
        private double pendingFastUsd = Double.NaN;
        private double pendingOfi = 0.0;
        private double pendingSync = 0.0;

        FastSpotMicroComputationSession(FastSpotMicroIndicatorConfig config, String cryptoSymbol) {
            this.indicator = new FastSpotMicroIndicator(config);
            this.cryptoSymbol = cryptoSymbol;
            this.coalesceWindowMs = config.coalesceWindowMs();
        }

        @Override
        public void onUsdtUsdTrade(long timestampMs, double usdtUsdPrice) {
            indicator.onUsdtUsdTrade(timestampMs, usdtUsdPrice);
        }

        @Override
        public void onSpotVenueTrade(
                SpotTwapIndicator.SpotVenue venue,
                long timestampMs,
                double rawPrice,
                double quantity,
                boolean isTakerBuy,
                boolean recordOutput) {
            double fastUsd = indicator.onVenueTrade(venue, timestampMs, rawPrice, quantity, isTakerBuy);
            if (!recordOutput || !Double.isFinite(fastUsd) || fastUsd <= 0.0) {
                return;
            }
            long bucket = timestampMs / coalesceWindowMs;
            if (bucket != activeBucket) {
                flushPendingBucket();
                activeBucket = bucket;
            }
            pendingFastUsd = fastUsd;
            pendingOfi = indicator.getLastSignedOfi1s();
            pendingSync = indicator.getLastSyncMove500Usd();
        }

        private void flushPendingBucket() {
            if (activeBucket < 0L || !Double.isFinite(pendingFastUsd) || pendingFastUsd <= 0.0) {
                return;
            }
            if (count == timestampsMs.length) {
                int newCap = timestampsMs.length << 1;
                timestampsMs = Arrays.copyOf(timestampsMs, newCap);
                sequences = Arrays.copyOf(sequences, newCap);
                fastDirectUsds = Arrays.copyOf(fastDirectUsds, newCap);
                signedOfis = Arrays.copyOf(signedOfis, newCap);
                syncMoves = Arrays.copyOf(syncMoves, newCap);
            }
            timestampsMs[count] = activeBucket * coalesceWindowMs + (coalesceWindowMs - 1L);
            sequences[count] = ++seq;
            fastDirectUsds[count] = pendingFastUsd;
            signedOfis[count] = pendingOfi;
            syncMoves[count] = pendingSync;
            count++;
            activeBucket = -1L;
            pendingFastUsd = Double.NaN;
        }

        @Override
        public DailyIndicatorTrack writeParquetAndBuildTrack(Connection connection, Path targetParquetFile)
                throws SQLException, IOException {
            flushPendingBucket();

            Path parentDir = targetParquetFile.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }

            long tableId = STAGING_TABLE_SEQ.incrementAndGet();
            String tableName = "tmp_fast_spot_micro_cache_" + tableId;
            Path tempFile = targetParquetFile.resolveSibling(
                    targetParquetFile.getFileName().toString() + ".tmp." + tableId);

            DuckDBConnection duckConn = connection.isWrapperFor(DuckDBConnection.class)
                    ? connection.unwrap(DuckDBConnection.class)
                    : (DuckDBConnection) connection;

            try {
                try (Statement stmt = duckConn.createStatement()) {
                    stmt.execute("CREATE TABLE " + tableName +
                            " (ts BIGINT, seq BIGINT, fast_direct_usd DOUBLE, signed_ofi_1s DOUBLE, sync_move_500_usd DOUBLE)");
                    if (count > 0) {
                        try (DuckDBAppender appender = duckConn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
                            for (int i = 0; i < count; i++) {
                                appender.beginRow();
                                appender.append(timestampsMs[i]);
                                appender.append(sequences[i]);
                                appender.append(fastDirectUsds[i]);
                                appender.append(signedOfis[i]);
                                appender.append(syncMoves[i]);
                                appender.endRow();
                            }
                            appender.flush();
                        }
                    }
                    String normTemp = tempFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
                    stmt.execute("COPY " + tableName + " TO '" + normTemp + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
                } finally {
                    try (Statement stmt = duckConn.createStatement()) {
                        stmt.execute("DROP TABLE IF EXISTS " + tableName);
                    } catch (SQLException ignored) {
                    }
                }

                Files.move(tempFile, targetParquetFile, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(tempFile);
            }

            return new FastSpotMicroDailyTrack(
                    cryptoSymbol,
                    count,
                    Arrays.copyOf(timestampsMs, count),
                    Arrays.copyOf(sequences, count),
                    Arrays.copyOf(fastDirectUsds, count),
                    Arrays.copyOf(signedOfis, count),
                    Arrays.copyOf(syncMoves, count)
            );
        }
    }
}

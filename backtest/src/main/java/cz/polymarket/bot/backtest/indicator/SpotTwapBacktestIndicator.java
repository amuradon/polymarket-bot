package cz.polymarket.bot.backtest.indicator;

import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapIndicatorConfig;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.strategy.TradingStrategy;
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
 * {@link BacktestIndicator} adapter for {@link SpotTwapIndicator}.
 * <p>
 * Computes and caches daily multi-exchange USD spot price and 60-second rolling TWAP values
 * into typed 4-column Zstandard-compressed Parquet files ({@code ts}, {@code seq}, {@code spot_price}, {@code twap_60s_price}).
 */
@ApplicationScoped
public class SpotTwapBacktestIndicator implements BacktestIndicator {

    private static final AtomicLong STAGING_TABLE_SEQ = new AtomicLong(0L);
    private static final int INITIAL_BUFFER_CAPACITY = 16_384;

    private final SpotTwapIndicatorConfig config;

    /**
     * Constructs a {@link SpotTwapBacktestIndicator} with default calibrated indicator settings.
     */
    public SpotTwapBacktestIndicator() {
        this(SpotTwapIndicatorConfig.defaults());
    }

    /**
     * Constructs a {@link SpotTwapBacktestIndicator} with the injected configuration bean.
     *
     * @param config indicator configuration
     */
    @Inject
    public SpotTwapBacktestIndicator(SpotTwapIndicatorConfig config) {
        this.config = SpotTwapIndicatorConfig.unwrap(config);
    }

    @Override
    public String getName() {
        return SpotTwapIndicator.INDICATOR_NAME;
    }

    @Override
    public Map<String, Object> getConfigurationParameters() {
        return config.toParameterMap();
    }

    @Override
    public boolean isConsumedBy(TradingStrategy strategy) {
        return strategy instanceof SpotTwapListener;
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
     * @return {@link SpotTwapIndicatorConfig}
     */
    public SpotTwapIndicatorConfig getConfig() {
        return config;
    }

    @Override
    public ComputationSession createComputationSession(String cryptoSymbol) {
        return new SpotTwapComputationSession(config, cryptoSymbol);
    }

    @Override
    public DailyIndicatorTrack readCachedTrack(Connection connection, Path parquetFile, String cryptoSymbol)
            throws SQLException {
        String normPath = parquetFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        String sql = "SELECT ts, seq, spot_price, twap_60s_price FROM read_parquet('" + normPath + "') ORDER BY ts ASC, seq ASC";

        long[] timestampsMs = new long[INITIAL_BUFFER_CAPACITY];
        long[] sequences = new long[INITIAL_BUFFER_CAPACITY];
        double[] spotPrices = new double[INITIAL_BUFFER_CAPACITY];
        double[] twap60sPrices = new double[INITIAL_BUFFER_CAPACITY];
        int count = 0;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                if (count == timestampsMs.length) {
                    int newCap = timestampsMs.length << 1;
                    timestampsMs = Arrays.copyOf(timestampsMs, newCap);
                    sequences = Arrays.copyOf(sequences, newCap);
                    spotPrices = Arrays.copyOf(spotPrices, newCap);
                    twap60sPrices = Arrays.copyOf(twap60sPrices, newCap);
                }
                timestampsMs[count] = rs.getLong(1);
                sequences[count] = rs.getLong(2);
                spotPrices[count] = rs.getDouble(3);
                twap60sPrices[count] = rs.getDouble(4);
                count++;
            }
        }

        return new SpotTwapDailyTrack(
                cryptoSymbol,
                count,
                Arrays.copyOf(timestampsMs, count),
                Arrays.copyOf(sequences, count),
                Arrays.copyOf(spotPrices, count),
                Arrays.copyOf(twap60sPrices, count)
        );
    }

    /**
     * Persistable and queryable columnar daily track of {@link BacktestEvent.CalculatedSpotTwapEvent} records.
     */
    static final class SpotTwapDailyTrack implements DailyIndicatorTrack {

        private final String cryptoSymbol;
        private final int count;
        private final long[] timestampsMs;
        private final long[] sequences;
        private final double[] spotPrices;
        private final double[] twap60sPrices;

        SpotTwapDailyTrack(
                String cryptoSymbol,
                int count,
                long[] timestampsMs,
                long[] sequences,
                double[] spotPrices,
                double[] twap60sPrices) {
            this.cryptoSymbol = cryptoSymbol;
            this.count = count;
            this.timestampsMs = timestampsMs;
            this.sequences = sequences;
            this.spotPrices = spotPrices;
            this.twap60sPrices = twap60sPrices;
        }

        @Override
        public String indicatorName() {
            return SpotTwapIndicator.INDICATOR_NAME;
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
            return new BacktestEvent.CalculatedSpotTwapEvent(
                    timestampsMs[index],
                    sequences[index],
                    cryptoSymbol,
                    spotPrices[index],
                    twap60sPrices[index]
            );
        }
    }

    private static final class SpotTwapComputationSession implements ComputationSession {

        private final SpotTwapIndicator indicator;
        private final String cryptoSymbol;

        private long[] timestampsMs = new long[INITIAL_BUFFER_CAPACITY];
        private long[] sequences = new long[INITIAL_BUFFER_CAPACITY];
        private double[] spotPrices = new double[INITIAL_BUFFER_CAPACITY];
        private double[] twap60sPrices = new double[INITIAL_BUFFER_CAPACITY];
        private int count = 0;
        private long seq = 0L;

        SpotTwapComputationSession(SpotTwapIndicatorConfig config, String cryptoSymbol) {
            this.indicator = new SpotTwapIndicator(config);
            this.cryptoSymbol = cryptoSymbol;
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
            double twap60s = indicator.onVenueTrade(venue, timestampMs, rawPrice, quantity);
            if (!recordOutput) {
                return;
            }
            double spotUsd = indicator.getLastSpotPriceUsd();
            if (Double.isNaN(spotUsd) || spotUsd <= 0.0 || Double.isNaN(twap60s)) {
                return;
            }
            if (count == timestampsMs.length) {
                int newCap = timestampsMs.length << 1;
                timestampsMs = Arrays.copyOf(timestampsMs, newCap);
                sequences = Arrays.copyOf(sequences, newCap);
                spotPrices = Arrays.copyOf(spotPrices, newCap);
                twap60sPrices = Arrays.copyOf(twap60sPrices, newCap);
            }
            timestampsMs[count] = timestampMs;
            sequences[count] = ++seq;
            spotPrices[count] = spotUsd;
            twap60sPrices[count] = twap60s;
            count++;
        }

        @Override
        public DailyIndicatorTrack writeParquetAndBuildTrack(Connection connection, Path targetParquetFile)
                throws SQLException, IOException {
            Path parentDir = targetParquetFile.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }

            long tableId = STAGING_TABLE_SEQ.incrementAndGet();
            String tableName = "tmp_spot_twap_cache_" + tableId;
            Path tempFile = targetParquetFile.resolveSibling(
                    targetParquetFile.getFileName().toString() + ".tmp." + tableId);

            DuckDBConnection duckConn = connection.isWrapperFor(DuckDBConnection.class)
                    ? connection.unwrap(DuckDBConnection.class)
                    : (DuckDBConnection) connection;

            try {
                try (Statement stmt = duckConn.createStatement()) {
                    stmt.execute("CREATE TABLE " + tableName +
                            " (ts BIGINT, seq BIGINT, spot_price DOUBLE, twap_60s_price DOUBLE)");
                    if (count > 0) {
                        try (DuckDBAppender appender = duckConn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
                            for (int i = 0; i < count; i++) {
                                appender.beginRow();
                                appender.append(timestampsMs[i]);
                                appender.append(sequences[i]);
                                appender.append(spotPrices[i]);
                                appender.append(twap60sPrices[i]);
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

            return new SpotTwapDailyTrack(
                    cryptoSymbol,
                    count,
                    Arrays.copyOf(timestampsMs, count),
                    Arrays.copyOf(sequences, count),
                    Arrays.copyOf(spotPrices, count),
                    Arrays.copyOf(twap60sPrices, count)
            );
        }
    }
}

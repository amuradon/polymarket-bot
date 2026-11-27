package cz.polymarket.bot.backtest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.ComparisonResponse;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.EquityGranularity;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.RunDetailDto;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.RunSummaryDto;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.TradesPageResponse;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class BacktestHistoryRepositoryTest {

    private static final PerformanceMetricsCalculator CALC = new PerformanceMetricsCalculator();

    @Test
    @DisplayName("Should persist runs in DuckDB across reopen, support rename/delete, and rebase time-slices from zero")
    void shouldPersistCompareRebaseAndQueryTradesAcrossReopen(@TempDir Path tempDir) {
        Path dbFile = tempDir.resolve("history.duckdb");

        long day1T1 = 1785542400L; // 2026-08-01T00:00:00Z
        long day1T2 = day1T1 + 900L;
        long day2T1 = 1785628800L; // 2026-08-02T00:00:00Z
        long day3T1 = 1785715200L; // 2026-08-03T00:00:00Z

        TradeRecord r1t1 = new TradeRecord("2026-08-01T00:10:00Z", day1T1, TradeDirection.UP, 0.50, 1.00, 100.0, 200.0, 2.0, 98.0, "Resolution", 0.70, 0.50, 0.20, true, 10098.0);
        TradeRecord r1t2 = new TradeRecord("2026-08-02T00:10:00Z", day2T1, TradeDirection.DOWN, 0.60, 0.00, 120.0, 200.0, 2.0, -122.0, "Resolution", 0.65, 0.60, 0.05, false, 9976.0);
        TradeRecord r1t3 = new TradeRecord("2026-08-03T00:10:00Z", day3T1, TradeDirection.UP, 0.40, 1.00, 80.0, 200.0, 2.0, 118.0, "Resolution", 0.75, 0.40, 0.35, true, 10094.0);

        List<MarketEquityPoint> r1Points = List.of(
                new MarketEquityPoint(day1T1, "2026-08-01", 1, 1, 98.0, 2.0, 0.09, true, 10098.0),
                new MarketEquityPoint(day1T2, "2026-08-01", 0, 0, 0.0, 0.0, 0.25, true, 10098.0),
                new MarketEquityPoint(day2T1, "2026-08-02", 1, 0, -122.0, 2.0, 0.36, true, 9976.0),
                new MarketEquityPoint(day3T1, "2026-08-03", 1, 1, 118.0, 2.0, 0.04, true, 10094.0)
        );
        BacktestRunConfig cfg1 = new BacktestRunConfig(
                "OraclePulseLockStrategy", "BTCUSDT", "2026-08-01", "2026-08-03",
                10000.0, 220L, 100L, 50L, 50L,
                Map.of("minTauMs", 2000L, "maxSpread", 0.026),
                Map.of()
        );
        BacktestResult res1 = new BacktestResult(
                "run-1", "Oracle Default", "OraclePulseLockStrategy", "BTCUSDT",
                Instant.parse("2026-08-04T10:00:00Z"), 150L, 4, cfg1,
                CALC.calculate(List.of(r1t1, r1t2, r1t3), 10000.0, 0.185, 3.0),
                r1Points, List.of(r1t1, r1t2, r1t3), false, 0
        );

        TradeRecord r2t1 = new TradeRecord("2026-08-02T00:10:00Z", day2T1, TradeDirection.UP, 0.50, 1.00, 100.0, 200.0, 2.0, 98.0, "Resolution", 0.72, 0.50, 0.22, true, 10098.0);
        List<MarketEquityPoint> r2Points = List.of(
                new MarketEquityPoint(day1T1, "2026-08-01", 0, 0, 0.0, 0.0, 0.25, true, 10000.0),
                new MarketEquityPoint(day1T2, "2026-08-01", 0, 0, 0.0, 0.0, 0.25, true, 10000.0),
                new MarketEquityPoint(day2T1, "2026-08-02", 1, 1, 98.0, 2.0, 0.09, true, 10098.0),
                new MarketEquityPoint(day3T1, "2026-08-03", 0, 0, 0.0, 0.0, 0.25, true, 10098.0)
        );
        BacktestRunConfig cfg2 = new BacktestRunConfig(
                "OraclePulseLockStrategy", "BTCUSDT", "2026-08-01", "2026-08-03",
                10000.0, 250L, 100L, 50L, 50L,
                Map.of("minTauMs", 5000L, "maxSpread", 0.026),
                Map.of("minTauMs", 5000L)
        );
        BacktestResult res2 = new BacktestResult(
                "run-2", "Oracle Tau 5s", "OraclePulseLockStrategy", "BTCUSDT",
                Instant.parse("2026-08-04T11:00:00Z"), 120L, 4, cfg2,
                CALC.calculate(List.of(r2t1), 10000.0, 0.21, 3.0),
                r2Points, List.of(r2t1), false, 0
        );

        try (BacktestHistoryRepository repo = new BacktestHistoryRepository(dbFile, new ObjectMapper(), CALC)) {
            repo.saveRun(res1);
            repo.saveRun(res2);
        }

        // Reopen DB from disk to simulate restart persistence
        try (BacktestHistoryRepository reopened = new BacktestHistoryRepository(dbFile, new ObjectMapper(), CALC)) {
            List<RunSummaryDto> runs = reopened.listRuns(null, null);
            assertThat(runs).hasSize(2);
            assertThat(runs.get(0).runId()).isEqualTo("run-2");
            assertThat(runs.get(1).runId()).isEqualTo("run-1");

            RunDetailDto detail = reopened.getRun("run-2").orElseThrow();
            assertThat(detail.config().orderGatewayDelayMs()).isEqualTo(250L);
            assertThat(detail.config().strategyOverrides()).containsEntry("minTauMs", 5000);

            // MARKET granularity across full range includes 0-trade markets (4 points)
            ComparisonResponse fullMarket = reopened.compareRuns(
                    List.of("run-1", "run-2"), EquityGranularity.MARKET, null, null);
            assertThat(fullMarket.series()).hasSize(2);
            assertThat(fullMarket.series().get(0).points()).hasSize(4);
            assertThat(fullMarket.series().get(0).points().get(1).tradesCount()).isZero();
            assertThat(fullMarket.series().get(0).points().get(1).cumulativePnlUsd()).isCloseTo(98.0, within(1e-6));

            // Parameter diff highlights orderGatewayDelayMs and minTauMs, while maxSpread is identical
            assertThat(fullMarket.parameterDiffs())
                    .anySatisfy(row -> {
                        assertThat(row.parameter()).isEqualTo("orderGatewayDelayMs");
                        assertThat(row.differs()).isTrue();
                    })
                    .anySatisfy(row -> {
                        assertThat(row.parameter()).isEqualTo("minTauMs");
                        assertThat(row.differs()).isTrue();
                    })
                    .anySatisfy(row -> {
                        assertThat(row.parameter()).isEqualTo("maxSpread");
                        assertThat(row.differs()).isFalse();
                    });

            // Time-slice [2026-08-02 .. 2026-08-03] MUST rebase equity from 0.0 and recalculate slice metrics!
            ComparisonResponse slicedDay = reopened.compareRuns(
                    List.of("run-1", "run-2"), EquityGranularity.DAY, "2026-08-02", "2026-08-03");
            var run1Slice = slicedDay.series().get(0);
            assertThat(run1Slice.points()).hasSize(2);
            // Day 2 PnL is -122.0 (rebased from 0, NOT 98 - 122 = -24!)
            assertThat(run1Slice.points().get(0).cumulativePnlUsd()).isCloseTo(-122.0, within(1e-6));
            // Day 3 PnL is +118.0 -> cumulative slice PnL = -4.0
            assertThat(run1Slice.points().get(1).cumulativePnlUsd()).isCloseTo(-4.0, within(1e-6));
            assertThat(run1Slice.sliceMetrics().totalTrades()).isEqualTo(2);
            assertThat(run1Slice.sliceMetrics().winningTrades()).isEqualTo(1);
            assertThat(run1Slice.sliceMetrics().totalNetPnl()).isCloseTo(-4.0, within(1e-6));

            // Trade query inside slice with filter
            TradesPageResponse tradesPage = reopened.queryTrades(
                    "run-1", "2026-08-02", "2026-08-03", "UP", "WIN", null, 50, 0).orElseThrow();
            assertThat(tradesPage.totalCount()).isEqualTo(1);
            assertThat(tradesPage.trades().get(0).netPnl()).isCloseTo(118.0, within(1e-6));

            // Rename and delete
            assertThat(reopened.updateLabel("run-1", "Renamed Oracle")).isTrue();
            assertThat(reopened.getRun("run-1").orElseThrow().summary().label()).isEqualTo("Renamed Oracle");
            assertThat(reopened.deleteRun("run-2")).isTrue();
            assertThat(reopened.listRuns(null, null)).hasSize(1);
        }
    }

    @Test
    @DisplayName("Should match configHash across Integer/Long param types and merge appended/backfilled/overwritten days chronologically")
    void shouldMatchConfigHashAndMergeDaysChronologically(@TempDir Path tempDir) {
        String hashWithInt = BacktestRunConfig.computeConfigHash(
                "OraclePulseLockStrategy", "BTCUSDT", 10000.0, 220L, 100L, 50L, 50L,
                Map.of("minTauMs", 2000, "maxSpread", 0.026));
        String hashWithLong = BacktestRunConfig.computeConfigHash(
                "OraclePulseLockStrategy", "BTCUSDT", 10000.0, 220L, 100L, 50L, 50L,
                Map.of("maxSpread", 0.026, "minTauMs", 2000L));
        assertThat(hashWithInt).isEqualTo(hashWithLong);

        Path dbFile = tempDir.resolve("merge-history.duckdb");
        long day2T1 = 1785628800L; // 2026-08-02
        long day3T1 = 1785715200L; // 2026-08-03
        long day1T1 = 1785542400L; // 2026-08-01 (backfilled later)

        TradeRecord day2Trade = new TradeRecord("2026-08-02T00:10:00Z", day2T1, TradeDirection.UP, 0.50, 1.00, 100.0, 200.0, 2.0, 100.0, "Resolution", 0.70, 0.50, 0.20, true, 10100.0);
        BacktestRunConfig initialCfg = new BacktestRunConfig(
                "OraclePulseLockStrategy", "BTCUSDT", "2026-08-02", "2026-08-02",
                10000.0, 220L, 100L, 50L, 50L,
                Map.of("minTauMs", 2000, "maxSpread", 0.026), Map.of()
        );
        BacktestResult initialRes = new BacktestResult(
                "run-merge", "OraclePulseLockStrategy (2026-08-02 .. 2026-08-02)", "OraclePulseLockStrategy", "BTCUSDT",
                Instant.parse("2026-08-02T23:00:00Z"), 50L, 1, initialCfg,
                CALC.calculate(List.of(day2Trade), 10000.0, 0.09, 1.0),
                List.of(new MarketEquityPoint(day2T1, "2026-08-02", 1, 1, 100.0, 2.0, 0.09, true, 10100.0)),
                List.of(day2Trade), false, 0
        );

        try (BacktestHistoryRepository repo = new BacktestHistoryRepository(dbFile, new ObjectMapper(), CALC)) {
            repo.saveRun(initialRes);
            assertThat(repo.findLatestRunByConfigHash(hashWithLong)).isPresent()
                    .get().extracting(RunSummaryDto::runId).isEqualTo("run-merge");
            assertThat(repo.getStoredDatesForRun("run-merge")).containsExactly("2026-08-02");

            // Append day 3 AND backfill day 1
            TradeRecord day1Trade = new TradeRecord("2026-08-01T00:10:00Z", day1T1, TradeDirection.UP, 0.50, 1.00, 100.0, 200.0, 2.0, 50.0, "Resolution", 0.70, 0.50, 0.20, true, 10050.0);
            TradeRecord day3Trade = new TradeRecord("2026-08-03T00:10:00Z", day3T1, TradeDirection.DOWN, 0.50, 0.00, 40.0, 80.0, 1.0, -40.0, "Resolution", 0.60, 0.50, 0.10, false, 9960.0);
            BacktestResult merged = repo.mergeRunDays(
                    "run-merge",
                    null,
                    initialCfg,
                    List.of(
                            new MarketEquityPoint(day3T1, "2026-08-03", 1, 0, -40.0, 1.0, 0.36, true, 9960.0),
                            new MarketEquityPoint(day1T1, "2026-08-01", 1, 1, 50.0, 2.0, 0.04, true, 10050.0)
                    ),
                    List.of(day3Trade, day1Trade),
                    java.util.Set.of(),
                    80L,
                    1
            );

            assertThat(merged.mergedIntoExisting()).isTrue();
            assertThat(merged.skippedDays()).isEqualTo(1);
            assertThat(merged.label()).isEqualTo("OraclePulseLockStrategy (2026-08-01 .. 2026-08-03)");
            assertThat(merged.config().startDate()).isEqualTo("2026-08-01");
            assertThat(merged.config().endDate()).isEqualTo("2026-08-03");
            // Chronological balance replay: 10000 + 50 (day1) + 100 (day2) - 40 (day3) = 10110
            assertThat(merged.trades()).hasSize(3);
            assertThat(merged.trades().get(0).balanceAfterTrade()).isCloseTo(10050.0, within(1e-6));
            assertThat(merged.trades().get(1).balanceAfterTrade()).isCloseTo(10150.0, within(1e-6));
            assertThat(merged.trades().get(2).balanceAfterTrade()).isCloseTo(10110.0, within(1e-6));
            assertThat(merged.metrics().finalBalance()).isCloseTo(10110.0, within(1e-6));
            assertThat(repo.getStoredDatesForRun("run-merge")).containsExactlyInAnyOrder("2026-08-01", "2026-08-02", "2026-08-03");

            // Overwrite day 2 with -20 netPnL
            TradeRecord newDay2Trade = new TradeRecord("2026-08-02T00:10:00Z", day2T1, TradeDirection.DOWN, 0.50, 0.00, 20.0, 40.0, 1.0, -20.0, "Resolution", 0.55, 0.50, 0.05, false, 9980.0);
            BacktestResult overwritten = repo.mergeRunDays(
                    "run-merge",
                    null,
                    initialCfg,
                    List.of(new MarketEquityPoint(day2T1, "2026-08-02", 1, 0, -20.0, 1.0, 0.25, true, 9980.0)),
                    List.of(newDay2Trade),
                    java.util.Set.of("2026-08-02"),
                    30L,
                    0
            );
            assertThat(overwritten.trades()).hasSize(3);
            // 10000 + 50 (day1) - 20 (day2 overwritten) - 40 (day3) = 9990
            assertThat(overwritten.metrics().finalBalance()).isCloseTo(9990.0, within(1e-6));
        }
    }

    @Test
    @DisplayName("Should automatically drop legacy secondary ART indexes and cleanly merge/delete runs with 600+ trades")
    void shouldDropLegacyArtIndexesAndDeleteHighTradeCountRunsCleanly(@TempDir Path tempDir) throws Exception {
        Path dbFile = tempDir.resolve("legacy-indexed-history.duckdb");

        // Simulate a legacy DB that still carries secondary ART indexes idx_bt_points_run and idx_bt_trades_run
        // as well as the removed latency_factor column
        try (BacktestHistoryRepository bootstrap = new BacktestHistoryRepository(dbFile, new ObjectMapper(), CALC)) {
            assertThat(bootstrap.listRuns(null, null)).isEmpty();
        }
        try (Connection rawConn = DriverManager.getConnection("jdbc:duckdb:" + dbFile);
             Statement stmt = rawConn.createStatement()) {
            stmt.execute("ALTER TABLE backtest_runs ADD COLUMN latency_factor DOUBLE DEFAULT 1.0");
            stmt.execute("CREATE INDEX idx_bt_points_run ON backtest_market_points (run_id, market_idx)");
            stmt.execute("CREATE INDEX idx_bt_trades_run ON backtest_trades (run_id, trade_idx)");
        }

        long baseEpoch = 1785542400L; // 2026-08-01T00:00:00Z
        List<MarketEquityPoint> points600 = new ArrayList<>(600);
        List<TradeRecord> trades600 = new ArrayList<>(600);
        double bal1 = 10000.0;
        for (int i = 0; i < 600; i++) {
            long tStart = baseEpoch + i * 300L;
            String dateUtc = Instant.ofEpochSecond(tStart).toString().substring(0, 10);
            double pnl = (i % 2 == 0) ? 4.0 : -3.0;
            bal1 += pnl;
            trades600.add(new TradeRecord(
                    Instant.ofEpochSecond(tStart + 60L).toString(),
                    tStart,
                    (i % 2 == 0) ? TradeDirection.UP : TradeDirection.DOWN,
                    0.50,
                    (i % 2 == 0) ? 1.0 : 0.0,
                    10.0,
                    20.0,
                    0.2,
                    pnl,
                    "Resolution",
                    0.65,
                    0.50,
                    0.15,
                    pnl > 0,
                    bal1
            ));
            points600.add(new MarketEquityPoint(tStart, dateUtc, 1, pnl > 0 ? 1 : 0, pnl, 0.2, 0.12, true, bal1));
        }

        List<MarketEquityPoint> points350 = points600.subList(0, 350);
        List<TradeRecord> trades350 = trades600.subList(0, 350);

        BacktestRunConfig cfg = new BacktestRunConfig(
                "OraclePulseLockStrategy", "BTCUSDT", "2026-08-01", "2026-08-03",
                10000.0, 220L, 100L, 50L, 50L,
                Map.of("minTauMs", 2000L), Map.of()
        );
        BacktestResult largeRun1 = new BacktestResult(
                "1c2bf32f-7ffa-4980-ae14-f3bf408a6f48",
                "Oracle Large 600",
                "OraclePulseLockStrategy",
                "BTCUSDT",
                Instant.parse("2026-08-04T10:00:00Z"),
                250L,
                points600.size(),
                cfg,
                CALC.calculate(trades600, 10000.0, 0.12, 3.0),
                points600,
                trades600,
                false,
                0
        );
        BacktestResult largeRun2 = new BacktestResult(
                "6493f360-d36a-4140-a6d1-2dbcfa02e57c",
                "Oracle Large 350",
                "OraclePulseLockStrategy",
                "BTCUSDT",
                Instant.parse("2026-08-04T11:00:00Z"),
                190L,
                points350.size(),
                cfg,
                CALC.calculate(trades350, 10000.0, 0.12, 2.0),
                points350,
                trades350,
                false,
                0
        );

        try (BacktestHistoryRepository repo = new BacktestHistoryRepository(dbFile, new ObjectMapper(), CALC)) {
            repo.saveRun(largeRun1);
            repo.saveRun(largeRun2);

            // Merge an extra day into the 600-trade run (exercises DELETE + UPDATE + batch re-insert)
            long extraEpoch = 1785801600L; // 2026-08-04T00:00:00Z
            TradeRecord extraTrade = new TradeRecord(
                    "2026-08-04T00:01:00Z", extraEpoch, TradeDirection.UP,
                    0.50, 1.0, 10.0, 20.0, 0.2, 9.8, "Resolution", 0.7, 0.5, 0.2, true, bal1 + 9.8);
            BacktestResult merged = repo.mergeRunDays(
                    largeRun1.runId(),
                    null,
                    cfg,
                    List.of(new MarketEquityPoint(extraEpoch, "2026-08-04", 1, 1, 9.8, 0.2, 0.09, true, bal1 + 9.8)),
                    List.of(extraTrade),
                    java.util.Set.of(),
                    40L,
                    0
            );
            assertThat(merged.trades()).hasSize(601);

            // Deleting the 601-trade run must succeed without DuckDB ART index error
            assertThat(repo.deleteRun(largeRun1.runId())).isTrue();
            assertThat(repo.listRuns(null, null)).hasSize(1);
            assertThat(repo.queryTrades(largeRun2.runId(), null, null, "ALL", "ALL", null, 10, 0))
                    .isPresent()
                    .get()
                    .extracting(TradesPageResponse::totalCount)
                    .isEqualTo(350);

            // Bulk deleteAllRuns clears remaining history cleanly
            assertThat(repo.deleteAllRuns()).isEqualTo(1);
            assertThat(repo.listRuns(null, null)).isEmpty();
        }

        // Verify secondary ART indexes and legacy latency_factor column remain absent on disk
        try (Connection rawConn = DriverManager.getConnection("jdbc:duckdb:" + dbFile);
             Statement stmt = rawConn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name IN ('idx_bt_points_run', 'idx_bt_trades_run')")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isZero();
            }
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'backtest_runs' AND column_name = 'latency_factor'")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isZero();
            }
        }
    }
}

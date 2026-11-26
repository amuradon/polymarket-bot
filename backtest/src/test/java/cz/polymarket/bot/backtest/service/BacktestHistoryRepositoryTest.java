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
import java.time.Instant;
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
                10000.0, 1.0, 100L, 50L, 50L,
                Map.of("minTauMs", 2000L, "maxSpread", 0.026),
                Map.of()
        );
        BacktestResult res1 = new BacktestResult(
                "run-1", "Oracle Default", "OraclePulseLockStrategy", "BTCUSDT",
                Instant.parse("2026-08-04T10:00:00Z"), 150L, 4, cfg1,
                CALC.calculate(List.of(r1t1, r1t2, r1t3), 10000.0, 0.185, 3.0),
                r1Points, List.of(r1t1, r1t2, r1t3)
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
                10000.0, 1.5, 100L, 50L, 50L,
                Map.of("minTauMs", 5000L, "maxSpread", 0.026),
                Map.of("minTauMs", 5000L)
        );
        BacktestResult res2 = new BacktestResult(
                "run-2", "Oracle Tau 5s", "OraclePulseLockStrategy", "BTCUSDT",
                Instant.parse("2026-08-04T11:00:00Z"), 120L, 4, cfg2,
                CALC.calculate(List.of(r2t1), 10000.0, 0.21, 3.0),
                r2Points, List.of(r2t1)
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
            assertThat(detail.config().strategyOverrides()).containsEntry("minTauMs", 5000);

            // MARKET granularity across full range includes 0-trade markets (4 points)
            ComparisonResponse fullMarket = reopened.compareRuns(
                    List.of("run-1", "run-2"), EquityGranularity.MARKET, null, null);
            assertThat(fullMarket.series()).hasSize(2);
            assertThat(fullMarket.series().get(0).points()).hasSize(4);
            assertThat(fullMarket.series().get(0).points().get(1).tradesCount()).isZero();
            assertThat(fullMarket.series().get(0).points().get(1).cumulativePnlUsd()).isCloseTo(98.0, within(1e-6));

            // Parameter diff highlights latencyFactor and minTauMs, while maxSpread is identical
            assertThat(fullMarket.parameterDiffs())
                    .anySatisfy(row -> {
                        assertThat(row.parameter()).isEqualTo("latencyFactor");
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
}

package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
class BacktestApplicationTest {

    @Inject
    BacktestApplication application;

    @Test
    @DisplayName("BacktestApplication CDI bean should be injected successfully and implement QuarkusApplication")
    void shouldInjectBacktestApplication() {
        assertThat(application).isNotNull();
        assertThat(application).isInstanceOf(QuarkusApplication.class);
    }

    @Test
    @DisplayName("onStart method should handle StartupEvent without throwing exceptions")
    void shouldHandleStartupEvent() {
        assertThatCode(() -> application.onStart(new StartupEvent()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("BacktestApplication should have a public static main method")
    void shouldHaveMainMethod() throws NoSuchMethodException {
        Method mainMethod = BacktestApplication.class.getMethod("main", String[].class);
        assertThat(mainMethod).isNotNull();
        assertThat(java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers())).isTrue();
        assertThat(java.lang.reflect.Modifier.isPublic(mainMethod.getModifiers())).isTrue();
    }

    @Test
    @DisplayName("Batch mode should execute backtest on startup with configured defaults and exit with code 0")
    void shouldRunBacktestAndExitZeroInBatchModeWithDefaults() throws Exception {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        String todayUtc = LocalDate.now(ZoneOffset.UTC).toString();
        BacktestResult expectedResult = new BacktestResult(
                "UtterParameciumStrategy",
                "BTCUSDT",
                Instant.now(),
                42,
                PerformanceMetrics.empty(10000.0),
                List.of(),
                "D:/Crypto/data/Polymarket/backtesting/backtest_UtterParameciumStrategy_20261111_180000.json"
        );

        when(mockEngine.runBacktest(
                eq("UtterParameciumStrategy"),
                eq("BTCUSDT"),
                eq("2026-10-12"),
                eq(todayUtc),
                eq(10000.0),
                eq("D:/Crypto/data/Polymarket/backtesting"),
                eq(1.0)
        )).thenReturn(expectedResult);

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "UtterParameciumStrategy",
                "BTCUSDT",
                "2026-10-12",
                Optional.empty(),
                10000.0,
                "D:/Crypto/data/Polymarket/backtesting",
                1.0
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isZero();
        verify(mockEngine).runBacktest(
                "UtterParameciumStrategy",
                "BTCUSDT",
                "2026-10-12",
                todayUtc,
                10000.0,
                "D:/Crypto/data/Polymarket/backtesting",
                1.0
        );
    }

    @Test
    @DisplayName("Batch mode should pass custom strategy, capital, from, to, and dir overrides to BacktestEngine")
    void shouldRunBacktestWithCustomParametersInBatchMode() throws Exception {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        BacktestResult expectedResult = new BacktestResult(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                Instant.now(),
                10,
                PerformanceMetrics.empty(25000.0),
                List.of(),
                "D:/Custom/Out/backtest_TWAPArbitrageStrategy_20261111_180000.json"
        );

        when(mockEngine.runBacktest(
                eq("TWAPArbitrageStrategy"),
                eq("BTCUSDT"),
                eq("2026-10-15"),
                eq("2026-10-20"),
                eq(25000.0),
                eq("D:/Custom/Out"),
                eq(1.25)
        )).thenReturn(expectedResult);

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-10-15",
                Optional.of("2026-10-20"),
                25000.0,
                "D:/Custom/Out",
                1.25
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isZero();
        verify(mockEngine).runBacktest(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-10-15",
                "2026-10-20",
                25000.0,
                "D:/Custom/Out",
                1.25
        );
    }

    @Test
    @DisplayName("Batch mode should return exit code 1 when BacktestEngine fails")
    void shouldReturnExitCodeOneWhenBacktestEngineFailsInBatchMode() throws Exception {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        when(mockEngine.runBacktest(any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new IllegalArgumentException("Unknown strategy: InvalidStrategy"));

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "InvalidStrategy",
                "BTCUSDT",
                "2026-10-12",
                Optional.of("2026-10-15"),
                10000.0,
                "D:/Crypto/data/Polymarket/backtesting",
                1.0
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isEqualTo(1);
    }
}

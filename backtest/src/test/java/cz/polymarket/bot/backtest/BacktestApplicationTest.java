package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
class BacktestApplicationTest {

    @Inject
    BacktestApplication application;

    @ConfigProperty(name = "backtest.binance.delay-ms")
    long configuredBinanceDelayMs;

    @ConfigProperty(name = "backtest.kraken.delay-ms")
    long configuredKrakenDelayMs;

    @ConfigProperty(name = "backtest.coinbase.delay-ms")
    long configuredCoinbaseDelayMs;

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
    void shouldRunBacktestAndExitZeroInBatchModeWithDefaults() {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        String todayUtc = LocalDate.now(ZoneOffset.UTC).toString();
        BacktestResult expectedResult = new BacktestResult(
                "run-default",
                "UtterParameciumStrategy (2026-10-12 .. " + todayUtc + ")",
                "UtterParameciumStrategy",
                "BTCUSDT",
                Instant.now(),
                100L,
                42,
                new BacktestRunConfig("UtterParameciumStrategy", "BTCUSDT", "2026-10-12", todayUtc,
                        10000.0, 220L, 100L, 50L, 50L, Map.of(), Map.of()),
                PerformanceMetrics.empty(10000.0),
                List.of(),
                List.of(),
                false,
                0
        );

        when(mockEngine.runBacktest(isNull(), any(BacktestRunRequest.class), any())).thenReturn(expectedResult);

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "UtterParameciumStrategy",
                "BTCUSDT",
                "2026-10-12",
                Optional.empty(),
                10000.0,
                220L
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isZero();
        ArgumentCaptor<BacktestRunRequest> captor = ArgumentCaptor.forClass(BacktestRunRequest.class);
        verify(mockEngine).runBacktest(isNull(), captor.capture(), any());
        BacktestRunRequest req = captor.getValue();
        assertThat(req.strategyName()).isEqualTo("UtterParameciumStrategy");
        assertThat(req.symbol()).isEqualTo("BTCUSDT");
        assertThat(req.startDate()).isEqualTo("2026-10-12");
        assertThat(req.endDate()).isEqualTo(todayUtc);
        assertThat(req.initialCapital()).isEqualTo(10000.0);
        assertThat(req.orderGatewayDelayMs()).isEqualTo(220L);
    }

    @Test
    @DisplayName("Batch mode should pass custom strategy, capital, from, to, and gateway delay overrides to BacktestEngine")
    void shouldRunBacktestWithCustomParametersInBatchMode() {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        BacktestResult expectedResult = new BacktestResult(
                "run-custom",
                "UtterParameciumStrategy (2026-10-15 .. 2026-10-20)",
                "UtterParameciumStrategy",
                "BTCUSDT",
                Instant.now(),
                80L,
                10,
                new BacktestRunConfig("UtterParameciumStrategy", "BTCUSDT", "2026-10-15", "2026-10-20",
                        25000.0, 250L, 100L, 50L, 50L, Map.of(), Map.of()),
                PerformanceMetrics.empty(25000.0),
                List.of(),
                List.of(),
                false,
                0
        );

        when(mockEngine.runBacktest(isNull(), any(BacktestRunRequest.class), any())).thenReturn(expectedResult);

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "UtterParameciumStrategy",
                "BTCUSDT",
                "2026-10-15",
                Optional.of("2026-10-20"),
                25000.0,
                250L
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isZero();
        ArgumentCaptor<BacktestRunRequest> captor = ArgumentCaptor.forClass(BacktestRunRequest.class);
        verify(mockEngine).runBacktest(isNull(), captor.capture(), any());
        BacktestRunRequest req = captor.getValue();
        assertThat(req.startDate()).isEqualTo("2026-10-15");
        assertThat(req.endDate()).isEqualTo("2026-10-20");
        assertThat(req.initialCapital()).isEqualTo(25000.0);
        assertThat(req.orderGatewayDelayMs()).isEqualTo(250L);
    }

    @Test
    @DisplayName("Batch mode should return exit code 1 when BacktestEngine fails")
    void shouldReturnExitCodeOneWhenBacktestEngineFailsInBatchMode() {
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        when(mockEngine.runBacktest(any(), any(), any()))
                .thenThrow(new IllegalArgumentException("Unknown strategy: InvalidStrategy"));

        BacktestApplication batchApp = new BacktestApplication(
                mockEngine,
                true,
                "InvalidStrategy",
                "BTCUSDT",
                "2026-10-12",
                Optional.of("2026-10-15"),
                10000.0,
                220L
        );

        int exitCode = batchApp.run();

        assertThat(exitCode).isEqualTo(1);
    }

    @Test
    @DisplayName("Backtest application.properties should configure Enable-Native-Access manifest attribute and exchange feed delays")
    void shouldConfigureNativeAccessManifestAttributeForBacktestJar() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources/application.properties"), StandardCharsets.UTF_8);

        assertThat(lines)
                .contains("quarkus.package.jar.manifest.attributes.\"Enable-Native-Access\"=ALL-UNNAMED");
        assertThat(configuredBinanceDelayMs).isEqualTo(100L);
        assertThat(configuredKrakenDelayMs).isEqualTo(50L);
        assertThat(configuredCoinbaseDelayMs).isEqualTo(50L);
    }
}

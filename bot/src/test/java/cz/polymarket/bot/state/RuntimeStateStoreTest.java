package cz.polymarket.bot.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.engine.Position;
import cz.polymarket.bot.engine.StrategyMode;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RuntimeStateStore} verifying atomic write-and-replace persistence and deserialization
 * of {@link RuntimeStateSnapshot}.
 */
class RuntimeStateStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldAtomicallySaveAndLoadRuntimeStateSnapshotWithTargetPriceModeAndOpenPositions() {
        Path stateFile = tempDir.resolve("nested/runtime-state.json");
        RuntimeStateStore store = new RuntimeStateStore(stateFile.toString(), new ObjectMapper());
        assertThat(store.load()).isEmpty();

        Position openPos = new Position(
                "client-1",
                "0xclob-1",
                TradeDirection.UP,
                0.56,
                100.0,
                56.0,
                1.72,
                1_795_000_005_000L,
                1_795_000_000L,
                "BTC-UP",
                "50942702967128588059751532145364262040082665861951814492817689719364041142318",
                0.70,
                0.56,
                0.14,
                0.58,
                true
        );

        RuntimeStateSnapshot snapshot = new RuntimeStateSnapshot(
                1_795_000_010_000L,
                "BTCUSDT",
                "15m",
                StrategyMode.PAPER,
                1_795_000_000L,
                1_795_000_900L,
                91_234.56,
                "btc-updown-15m-1795000000",
                "0xcond-abc",
                "50942702967128588059751532145364262040082665861951814492817689719364041142318",
                "50942702967128588059751532145364262040082665861951814492817689719364041142319",
                1_795_000_009_000L,
                1_795_000_009_500L,
                91_240.00,
                91_250.00,
                91_220.00,
                10_000.0,
                9_944.0,
                List.of(openPos)
        );

        store.save(snapshot);

        assertThat(Files.exists(stateFile)).isTrue();
        assertThat(Files.exists(stateFile.resolveSibling("runtime-state.json.tmp"))).isFalse();

        Optional<RuntimeStateSnapshot> loadedOpt = store.load();
        assertThat(loadedOpt).isPresent();
        RuntimeStateSnapshot loaded = loadedOpt.get();
        assertThat(loaded.strategyMode()).isEqualTo(StrategyMode.PAPER);
        assertThat(loaded.twapOpenPrice()).isEqualTo(91_234.56);
        assertThat(loaded.activeMarketSlug()).isEqualTo("btc-updown-15m-1795000000");
        assertThat(loaded.openPositions()).hasSize(1);
        assertThat(loaded.openPositions().getFirst().clientOrderId()).isEqualTo("client-1");
        assertThat(loaded.openPositions().getFirst().trailingStopArmed()).isTrue();
    }
}

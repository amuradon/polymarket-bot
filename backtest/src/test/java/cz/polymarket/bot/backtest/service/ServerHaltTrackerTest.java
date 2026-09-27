package cz.polymarket.bot.backtest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServerHaltTrackerTest {

    private ServerHaltTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new ServerHaltTracker();
    }

    @Test
    void shouldTrackHaltedServerByUrlAndHost() {
        String url = "https://api.cryptohftdata.com/v1/download";
        String host = "api.cryptohftdata.com";

        assertThat(tracker.isHalted(url)).isFalse();
        assertThat(tracker.isHalted(host)).isFalse();

        tracker.haltServer(url, "HTTP 429 Too Many Requests");

        assertThat(tracker.isHalted(url)).isTrue();
        assertThat(tracker.isHalted(host)).isTrue();
        assertThat(tracker.getHaltReason(host)).contains("HTTP 429 Too Many Requests");
        assertThat(tracker.getHaltReason(url)).contains("HTTP 429 Too Many Requests");
    }

    @Test
    void shouldIsolateDifferentServers() {
        tracker.haltServer("https://api.cryptohftdata.com/v1", "HTTP 429");

        assertThat(tracker.isHalted("https://api.cryptohftdata.com/v1")).isTrue();
        assertThat(tracker.isHalted("https://data.binance.vision/data")).isFalse();
        assertThat(tracker.isHalted("data.binance.vision")).isFalse();
    }

    @Test
    void shouldResetHaltedServers() {
        tracker.haltServer("api.cryptohftdata.com", "HTTP 429");
        assertThat(tracker.isHalted("api.cryptohftdata.com")).isTrue();

        tracker.reset();
        assertThat(tracker.isHalted("api.cryptohftdata.com")).isFalse();
        assertThat(tracker.getHaltReason("api.cryptohftdata.com")).isEmpty();
    }
}

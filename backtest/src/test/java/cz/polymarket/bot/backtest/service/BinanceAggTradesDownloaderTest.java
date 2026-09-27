package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.DownloadResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BinanceAggTradesDownloaderTest {

    @TempDir
    Path tempDir;

    private HttpClient mockHttpClient;
    private ServerHaltTracker serverHaltTracker;
    private BinanceAggTradesDownloader downloader;

    @BeforeEach
    void setUp() {
        mockHttpClient = Mockito.mock(HttpClient.class);
        serverHaltTracker = new ServerHaltTracker();
        downloader = new BinanceAggTradesDownloader(
                tempDir.toString(),
                "https://data.binance.vision/data/spot/daily/aggTrades",
                "https://data.binance.vision/data/futures/um/daily/aggTrades",
                mockHttpClient,
                serverHaltTracker
        );
    }

    @Test
    void shouldDownloadAndExtractSpotTrades() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        String csvContent = "1,10000.0,1.5,100,200,1600000000,true,true";
        byte[] zipBytes = createZipPayload("BTCUSDT-aggTrades-2026-08-01.csv", csvContent);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(zipBytes);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadSpotTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(0);

        Path expectedCsv = tempDir.resolve("spot").resolve(symbol).resolve("aggTrades")
                .resolve("BTCUSDT-aggTrades-2026-08-01.csv");
        assertThat(expectedCsv).exists();
        assertThat(Files.readString(expectedCsv)).isEqualTo(csvContent);
    }

    @Test
    void shouldSkipDownloadIfFileAlreadyExists() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        Path targetDir = tempDir.resolve("futures").resolve(symbol).resolve("aggTrades");
        Files.createDirectories(targetDir);
        Path existingFile = targetDir.resolve("BTCUSDT-aggTrades-2026-08-01.csv");
        Files.writeString(existingFile, "already-present");

        DownloadResult result = downloader.downloadFuturesTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(0);

        // HttpClient should not be called when file already exists
        verify(mockHttpClient, never()).send(any(), any());
        assertThat(Files.readString(existingFile)).isEqualTo("already-present");
    }

    @Test
    void shouldHandleHttp404Gracefully() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(404);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadSpotTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0)).contains("404");
    }

    @Test
    void shouldHaltServerAndSkipRemainingOnHttp429() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate start = LocalDate.of(2026, 8, 1);
        LocalDate end = LocalDate.of(2026, 8, 2);
        byte[] body = "{\"code\":-1003,\"msg\":\"Way too many requests\"}".getBytes(StandardCharsets.UTF_8);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(429);
        when(mockResponse.body()).thenReturn(body);
        HttpHeaders headers = HttpHeaders.of(Map.of("Retry-After", List.of("120")), (k, v) -> true);
        when(mockResponse.headers()).thenReturn(headers);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        // Download 2 days: Day 1 should hit 429, Day 2 must be skipped without HTTP request
        DownloadResult result = downloader.downloadSpotTrades(symbol, start, end);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("HTTP 429 Too Many Requests")
                .contains("Retry-After: 120")
                .contains("Way too many requests");

        verify(mockHttpClient, times(1)).send(any(), any());
        assertThat(serverHaltTracker.isHalted("data.binance.vision")).isTrue();
    }

    @Test
    void shouldLogAndRecordTimeout() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new HttpTimeoutException("request timed out"));

        DownloadResult result = downloader.downloadSpotTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("Timeout")
                .contains("30s");
    }

    @Test
    void shouldLogAndRecordHttpErrorWithResponseBody() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        byte[] body = "Service Temporarily Unavailable".getBytes(StandardCharsets.UTF_8);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(503);
        when(mockResponse.body()).thenReturn(body);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadSpotTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("HTTP 503")
                .contains("Service Temporarily Unavailable");
    }

    private byte[] createZipPayload(String entryName, String content) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry(entryName));
            zos.write(content.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        return baos.toByteArray();
    }
}

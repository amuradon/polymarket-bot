package cz.polymarket.bot.backtest.service;

import com.github.luben.zstd.ZstdOutputStream;
import cz.polymarket.bot.backtest.data.DownloadResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BinanceOrderBookDownloaderTest {

    @TempDir
    Path tempDir;

    private HttpClient mockHttpClient;
    private ServerHaltTracker serverHaltTracker;
    private BinanceOrderBookDownloader downloader;

    @BeforeEach
    void setUp() {
        mockHttpClient = Mockito.mock(HttpClient.class);
        serverHaltTracker = new ServerHaltTracker();
        downloader = new BinanceOrderBookDownloader(
                tempDir.toString(),
                "https://api.cryptohftdata.com/v1",
                Optional.of("test-api-key"),
                mockHttpClient,
                serverHaltTracker
        );
    }

    @Test
    void shouldDownloadAndDecompressZstdOrderBook() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        byte[] rawParquet = "PAR1-dummy-parquet-data-bytes-PAR1".getBytes(StandardCharsets.UTF_8);
        byte[] zstdBytes = compressZstd(rawParquet);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(zstdBytes);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        // Download for 1 hour to test decompression logic
        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 0);

        assertThat(result.downloaded()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(0);

        Path expectedParquet = tempDir.resolve("futures").resolve(symbol).resolve("orderBook")
                .resolve("BTCUSDT-orderbook-2026-08-01-00.parquet");
        assertThat(expectedParquet).exists();
        assertThat(Files.readAllBytes(expectedParquet)).isEqualTo(rawParquet);
    }

    @Test
    void shouldDownloadBareParquetWithoutZstd() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        byte[] rawParquet = "PAR1-uncompressed-parquet-bytes-PAR1".getBytes(StandardCharsets.UTF_8);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(rawParquet);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 5);

        assertThat(result.downloaded()).isEqualTo(1);
        Path expectedParquet = tempDir.resolve("futures").resolve(symbol).resolve("orderBook")
                .resolve("BTCUSDT-orderbook-2026-08-01-05.parquet");
        assertThat(expectedParquet).exists();
        assertThat(Files.readAllBytes(expectedParquet)).isEqualTo(rawParquet);
    }

    @Test
    void shouldSkipOrderBookIfFileAlreadyExists() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        Path targetDir = tempDir.resolve("futures").resolve(symbol).resolve("orderBook");
        Files.createDirectories(targetDir);
        Path existing = targetDir.resolve("BTCUSDT-orderbook-2026-08-01-12.parquet");
        Files.writeString(existing, "existing-data");

        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 12);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(0);

        verify(mockHttpClient, never()).send(any(), any());
        assertThat(Files.readString(existing)).isEqualTo("existing-data");
    }

    @Test
    void shouldHandleHttp404Gracefully() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(404);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 3);

        // 404 is normal for hours without trades or missing objects in cryptohftdata
        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(1);
    }

    @Test
    void shouldHaltServerAndSkipRemainingOnHttp429() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        byte[] body = "{\"error\":\"rate_limited\",\"message\":\"Too many requests\"}".getBytes(StandardCharsets.UTF_8);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(429);
        when(mockResponse.body()).thenReturn(body);
        HttpHeaders headers = HttpHeaders.of(Map.of("Retry-After", List.of("60")), (k, v) -> true);
        when(mockResponse.headers()).thenReturn(headers);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        // Download full day (24 hours): hour 0 should hit 429, hours 1..23 must be skipped without HTTP requests
        DownloadResult result = downloader.downloadOrderBook(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(23);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("HTTP 429 Too Many Requests")
                .contains("Retry-After: 60")
                .contains("rate_limited");

        // The HTTP client should only be called once (for hour 0), not for hours 1..23!
        verify(mockHttpClient, times(1)).send(any(), any());
        assertThat(serverHaltTracker.isHalted("https://api.cryptohftdata.com/v1")).isTrue();
        assertThat(serverHaltTracker.isHalted("api.cryptohftdata.com")).isTrue();
    }

    @Test
    void shouldLogAndRecordTimeout() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new HttpTimeoutException("request timed out"));

        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 7);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("Timeout")
                .contains("30s")
                .contains("hour 07");
    }

    @Test
    void shouldLogAndRecordHttp500WithResponseBody() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        byte[] body = "Internal Server Error Occurred".getBytes(StandardCharsets.UTF_8);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(500);
        when(mockResponse.body()).thenReturn(body);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadOrderBookHour(symbol, date, 8);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0))
                .contains("HTTP 500")
                .contains("Internal Server Error Occurred")
                .contains("hour 08");
    }

    private byte[] compressZstd(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zos = new ZstdOutputStream(baos)) {
            zos.write(data);
        }
        return baos.toByteArray();
    }
}

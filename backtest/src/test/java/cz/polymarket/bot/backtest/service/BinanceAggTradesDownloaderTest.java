package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.DownloadResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
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

/**
 * Unit tests for {@link BinanceAggTradesDownloader}.
 * Verifies downloading, Parquet conversion, CSV cleanup, skipping, and error tracking using synthetic test fixtures.
 */
class BinanceAggTradesDownloaderTest {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
        } catch (Exception ignored) {
        }
    }

    @TempDir
    Path tempDir;

    private HttpClient mockHttpClient;
    private ServerHaltTracker serverHaltTracker;
    private BinanceAggTradesParquetConverter parquetConverter;
    private BinanceAggTradesDownloader downloader;

    @BeforeEach
    void setUp() {
        mockHttpClient = Mockito.mock(HttpClient.class);
        serverHaltTracker = new ServerHaltTracker();
        parquetConverter = new BinanceAggTradesParquetConverter();
        downloader = new BinanceAggTradesDownloader(
                tempDir.toString(),
                "https://data.binance.vision/data/spot/daily/aggTrades",
                "https://data.binance.vision/data/futures/um/daily/aggTrades",
                mockHttpClient,
                serverHaltTracker,
                parquetConverter
        );
    }

    @Test
    @DisplayName("Should download Spot trades, convert CSV to Parquet immediately, and delete the CSV file")
    void shouldDownloadAndExtractSpotTrades() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        String csvContent = "4000000001,65000.50,0.015,5000000001,5000000002,1785542400100000,true,true\n";
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

        Path spotDir = tempDir.resolve("spot").resolve(symbol).resolve("aggTrades");
        Path expectedParquet = spotDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        Path deletedCsv = spotDir.resolve("BTCUSDT-aggTrades-2026-08-01.csv");

        assertThat(expectedParquet).exists();
        assertThat(Files.size(expectedParquet)).isPositive();
        assertThat(deletedCsv).doesNotExist();

        // Verify Parquet content
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            String normPath = expectedParquet.toAbsolutePath().toString().replace('\\', '/');
            try (ResultSet rs = stmt.executeQuery("SELECT count(*), price FROM read_parquet('" + normPath + "') GROUP BY price")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(1L);
                assertThat(rs.getDouble(2)).isEqualTo(65000.50);
            }
        }
    }

    @Test
    @DisplayName("Should download Futures trades, convert CSV to Parquet immediately, and delete the CSV file")
    void shouldDownloadAndExtractFuturesTrades() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        String csvContent = """
                agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
                2000000001,65100.2,0.05,3000000001,3000000001,1785542400123,true
                """;
        byte[] zipBytes = createZipPayload("BTCUSDT-aggTrades-2026-08-01.csv", csvContent);

        HttpResponse<byte[]> mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(zipBytes);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        DownloadResult result = downloader.downloadFuturesTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(0);

        Path futDir = tempDir.resolve("futures").resolve(symbol).resolve("aggTrades");
        Path expectedParquet = futDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        Path deletedCsv = futDir.resolve("BTCUSDT-aggTrades-2026-08-01.csv");

        assertThat(expectedParquet).exists();
        assertThat(Files.size(expectedParquet)).isPositive();
        assertThat(deletedCsv).doesNotExist();
    }

    @Test
    @DisplayName("Should skip download if Parquet file already exists")
    void shouldSkipDownloadIfFileAlreadyExists() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        Path targetDir = tempDir.resolve("futures").resolve(symbol).resolve("aggTrades");
        Files.createDirectories(targetDir);
        Path existingParquet = targetDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        Files.writeString(existingParquet, "already-present");

        DownloadResult result = downloader.downloadFuturesTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(0);

        // HttpClient should not be called when parquet file already exists
        verify(mockHttpClient, never()).send(any(), any());
        assertThat(Files.readString(existingParquet)).isEqualTo("already-present");
    }

    @Test
    @DisplayName("Should convert existing CSV to Parquet and delete CSV if Parquet is missing")
    void shouldConvertExistingCsvAndCleanup() throws Exception {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);
        Path targetDir = tempDir.resolve("spot").resolve(symbol).resolve("aggTrades");
        Files.createDirectories(targetDir);
        Path existingCsv = targetDir.resolve("BTCUSDT-aggTrades-2026-08-01.csv");
        Files.writeString(existingCsv, "4000000001,65000.50,0.015,5000000001,5000000002,1785542400100000,true,true\n");

        DownloadResult result = downloader.downloadSpotTrades(symbol, date, date);

        assertThat(result.downloaded()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(0);

        // HttpClient must not be invoked since CSV was converted locally
        verify(mockHttpClient, never()).send(any(), any());

        Path expectedParquet = targetDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        assertThat(expectedParquet).exists();
        assertThat(existingCsv).doesNotExist();
    }

    @Test
    @DisplayName("Should handle HTTP 404 gracefully")
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
    @DisplayName("Should halt server and skip remaining on HTTP 429")
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
    @DisplayName("Should log and record timeout")
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
    @DisplayName("Should log and record HTTP error with response body")
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

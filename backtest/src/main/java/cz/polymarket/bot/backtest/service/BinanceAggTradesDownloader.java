package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.DownloadResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@ApplicationScoped
public class BinanceAggTradesDownloader {

    private static final Logger LOG = Logger.getLogger(BinanceAggTradesDownloader.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path basePath;
    private final String spotTradesUrl;
    private final String futuresTradesUrl;
    private final HttpClient httpClient;
    private final ServerHaltTracker serverHaltTracker;

    @Inject
    public BinanceAggTradesDownloader(
            @ConfigProperty(name = "polymarket.data.base-dir") String baseDir,
            @ConfigProperty(name = "polymarket.data.binance.spot-trades-url") String spotTradesUrl,
            @ConfigProperty(name = "polymarket.data.binance.futures-trades-url") String futuresTradesUrl,
            HttpClient httpClient,
            ServerHaltTracker serverHaltTracker
    ) {
        this.basePath = Path.of(baseDir);
        this.spotTradesUrl = spotTradesUrl;
        this.futuresTradesUrl = futuresTradesUrl;
        this.httpClient = httpClient;
        this.serverHaltTracker = serverHaltTracker;
    }

    public DownloadResult downloadSpotTrades(String symbol, LocalDate startDate, LocalDate endDate) {
        Path targetDir = basePath.resolve("spot").resolve(symbol).resolve("aggTrades");
        return downloadTradesInternal("spot", spotTradesUrl, targetDir, symbol, startDate, endDate);
    }

    public DownloadResult downloadFuturesTrades(String symbol, LocalDate startDate, LocalDate endDate) {
        Path targetDir = basePath.resolve("futures").resolve(symbol).resolve("aggTrades");
        return downloadTradesInternal("futures", futuresTradesUrl, targetDir, symbol, startDate, endDate);
    }

    private DownloadResult downloadTradesInternal(
            String market,
            String baseUrl,
            Path targetDir,
            String symbol,
            LocalDate startDate,
            LocalDate endDate
    ) {
        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            String err = "Failed to create target directory " + targetDir + ": " + e.getMessage();
            LOG.error(err, e);
            return new DownloadResult(0, 0, 1, List.of(err));
        }

        int downloaded = 0;
        int skipped = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            String dateStr = current.format(DATE_FORMATTER);
            String csvFilename = symbol + "-aggTrades-" + dateStr + ".csv";
            Path targetCsv = targetDir.resolve(csvFilename);

            if (serverHaltTracker.isHalted(baseUrl)) {
                String haltReason = serverHaltTracker.getHaltReason(baseUrl).orElse("Server halted");
                LOG.warnf("Skipping %s aggTrades download for %s %s: %s", market, symbol, dateStr, haltReason);
                skipped++;
                current = current.plusDays(1);
                continue;
            }

            if (Files.exists(targetCsv)) {
                LOG.infof("File %s already exists, skipping download.", targetCsv);
                skipped++;
            } else {
                String zipUrl = baseUrl + "/" + symbol + "/" + symbol + "-aggTrades-" + dateStr + ".zip";
                try {
                    FetchResult res = fetchAndExtractZip(market, symbol, dateStr, zipUrl, targetCsv, csvFilename);
                    if (res.success()) {
                        downloaded++;
                    } else {
                        failed++;
                        errors.add(res.error());
                    }
                } catch (Exception e) {
                    failed++;
                    String err = String.format("Error downloading %s aggTrades for %s %s (URL: %s): %s",
                            market, symbol, dateStr, zipUrl, e.getMessage());
                    LOG.error(err, e);
                    errors.add(err);
                }
            }

            current = current.plusDays(1);
        }

        return new DownloadResult(downloaded, skipped, failed, errors);
    }

    private record FetchResult(boolean success, String error) {}

    private FetchResult fetchAndExtractZip(
            String market,
            String symbol,
            String dateStr,
            String zipUrl,
            Path targetCsv,
            String expectedCsvName
    ) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(zipUrl))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        HttpResponse<byte[]> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (HttpTimeoutException e) {
            String err = String.format("Timeout downloading %s aggTrades for %s %s (URL: %s after 30s): %s",
                    market, symbol, dateStr, zipUrl, e.getMessage());
            LOG.error(err);
            return new FetchResult(false, err);
        }

        int statusCode = response.statusCode();

        if (statusCode == 429) {
            String bodyStr = response.body() != null && response.body().length > 0
                    ? new String(response.body(), StandardCharsets.UTF_8)
                    : "<empty body>";
            Optional<String> retryAfter = response.headers().firstValue("Retry-After");
            String retryAfterInfo = retryAfter.map(s -> " (Retry-After: " + s + ")").orElse("");
            String err = String.format("HTTP 429 Too Many Requests downloading %s aggTrades for %s %s (URL: %s)%s: %s",
                    market, symbol, dateStr, zipUrl, retryAfterInfo, bodyStr);
            LOG.error(err);
            serverHaltTracker.haltServer(zipUrl, "HTTP 429 Too Many Requests" + retryAfterInfo);
            return new FetchResult(false, err);
        }

        if (statusCode == 404) {
            String msg = String.format("Archive not found (HTTP 404) for %s %s (%s): %s", symbol, dateStr, market, zipUrl);
            LOG.warn(msg);
            return new FetchResult(false, msg);
        }

        if (statusCode != 200) {
            String bodyStr = response.body() != null && response.body().length > 0
                    ? new String(response.body(), StandardCharsets.UTF_8)
                    : "<empty body>";
            String msg = String.format("HTTP %d error downloading %s aggTrades for %s %s (URL: %s): %s",
                    statusCode, market, symbol, dateStr, zipUrl, bodyStr);
            LOG.error(msg);
            return new FetchResult(false, msg);
        }

        byte[] body = response.body();
        if (body == null || body.length == 0) {
            String msg = String.format("Empty response body (HTTP 200) for %s aggTrades %s %s: %s",
                    market, symbol, dateStr, zipUrl);
            LOG.error(msg);
            return new FetchResult(false, msg);
        }

        Path partFile = targetCsv.resolveSibling(targetCsv.getFileName() + ".part");
        boolean extracted = false;

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(body))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().equals(expectedCsvName) || entry.getName().endsWith(".csv")) {
                    Files.copy(zis, partFile, StandardCopyOption.REPLACE_EXISTING);
                    extracted = true;
                    zis.closeEntry();
                    break;
                }
                zis.closeEntry();
            }
        }

        if (!extracted) {
            Files.deleteIfExists(partFile);
            String msg = String.format("No matching CSV entry found inside archive for %s %s: %s",
                    symbol, dateStr, zipUrl);
            LOG.error(msg);
            return new FetchResult(false, msg);
        }

        try {
            Files.move(partFile, targetCsv, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(partFile, targetCsv, StandardCopyOption.REPLACE_EXISTING);
        }

        LOG.infof("Extracted %s to %s", expectedCsvName, targetCsv);
        return new FetchResult(true, null);
    }
}

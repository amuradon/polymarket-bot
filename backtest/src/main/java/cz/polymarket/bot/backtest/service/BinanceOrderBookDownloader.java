package cz.polymarket.bot.backtest.service;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.backtest.data.DownloadResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

@ApplicationScoped
public class BinanceOrderBookDownloader {

    private static final Logger LOG = Logger.getLogger(BinanceOrderBookDownloader.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final byte[] ZSTD_MAGIC = new byte[]{(byte) 0x28, (byte) 0xb5, (byte) 0x2f, (byte) 0xfd};

    private final Path basePath;
    private final String baseUrl;
    private final Optional<String> apiKey;
    private final HttpClient httpClient;

    @Inject
    public BinanceOrderBookDownloader(
            @ConfigProperty(name = "polymarket.data.base-dir") String baseDir,
            @ConfigProperty(name = "polymarket.data.cryptohftdata.base-url") String baseUrl,
            @ConfigProperty(name = "polymarket.data.cryptohftdata.api-key") Optional<String> apiKey,
            HttpClient httpClient
    ) {
        this.basePath = Path.of(baseDir);
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.apiKey = apiKey;
        this.httpClient = httpClient;
    }

    public DownloadResult downloadOrderBook(String symbol, LocalDate startDate, LocalDate endDate) {
        DownloadResult aggregate = DownloadResult.empty();
        LocalDate current = startDate;

        while (!current.isAfter(endDate)) {
            for (int hour = 0; hour < 24; hour++) {
                DownloadResult hourResult = downloadOrderBookHour(symbol, current, hour);
                aggregate = aggregate.add(hourResult);
            }
            current = current.plusDays(1);
        }

        return aggregate;
    }

    public DownloadResult downloadOrderBookHour(String symbol, LocalDate date, int hour) {
        Path targetDir = basePath.resolve("futures").resolve(symbol).resolve("orderBook");
        try {
            Files.createDirectories(targetDir);
        } catch (IOException e) {
            String err = "Failed to create target directory " + targetDir + ": " + e.getMessage();
            LOG.error(err, e);
            return new DownloadResult(0, 0, 1, List.of(err));
        }

        String dateStr = date.format(DATE_FORMATTER);
        String hourStr = String.format("%02d", hour);
        String filename = symbol + "-orderbook-" + dateStr + "-" + hourStr + ".parquet";
        Path targetFile = targetDir.resolve(filename);

        if (Files.exists(targetFile)) {
            LOG.infof("File %s already exists, skipping download.", targetFile);
            return new DownloadResult(0, 1, 0, List.of());
        }

        String primaryKey = "binance_futures/" + dateStr + "/" + hourStr + "/" + symbol + "_orderbook.parquet";
        try {
            HttpResponse<byte[]> response = fetchKey(primaryKey);
            if (response.statusCode() == 404) {
                // Try fallback candidate before native-ZSTD cutover
                String fallbackKey = "binance_futures/" + dateStr + "/" + hourStr + "/" + symbol + "_orderbook.parquet.zst";
                response = fetchKey(fallbackKey);
            }

            if (response.statusCode() == 404) {
                LOG.debugf("Orderbook not found for %s %s hour %s (skipped)", symbol, dateStr, hourStr);
                return new DownloadResult(0, 1, 0, List.of());
            }

            if (response.statusCode() != 200) {
                String err = "HTTP " + response.statusCode() + " downloading orderbook for " + primaryKey;
                LOG.error(err);
                return new DownloadResult(0, 0, 1, List.of(err));
            }

            byte[] body = response.body();
            if (body == null || body.length == 0) {
                String err = "Empty response body for " + primaryKey;
                LOG.error(err);
                return new DownloadResult(0, 0, 1, List.of(err));
            }

            savePayload(body, targetFile);
            LOG.infof("Successfully downloaded orderbook to %s", targetFile);
            return new DownloadResult(1, 0, 0, List.of());

        } catch (Exception e) {
            String err = "Error downloading orderbook for " + primaryKey + ": " + e.getMessage();
            LOG.error(err, e);
            return new DownloadResult(0, 0, 1, List.of(err));
        }
    }

    private HttpResponse<byte[]> fetchKey(String key) throws IOException, InterruptedException {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        String requestUrl = baseUrl + "/download?file=" + encodedKey;

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(requestUrl))
                .timeout(Duration.ofSeconds(30))
                .GET();

        if (apiKey.isPresent() && !apiKey.get().isBlank()) {
            builder.header("X-API-Key", apiKey.get());
        }

        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private void savePayload(byte[] body, Path targetFile) throws IOException {
        Path partFile = targetFile.resolveSibling(targetFile.getFileName() + ".part");
        boolean isZstd = isZstdCompressed(body);

        if (isZstd) {
            try (ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(body));
                 OutputStream out = Files.newOutputStream(partFile)) {
                zis.transferTo(out);
            }
        } else {
            Files.write(partFile, body);
        }

        try {
            Files.move(partFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(partFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private boolean isZstdCompressed(byte[] data) {
        if (data.length < ZSTD_MAGIC.length) {
            return false;
        }
        for (int i = 0; i < ZSTD_MAGIC.length; i++) {
            if (data[i] != ZSTD_MAGIC[i]) {
                return false;
            }
        }
        return true;
    }
}

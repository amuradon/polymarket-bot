package cz.polymarket.bot.backtest.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Exports backtest execution results and detailed trade records to formatted JSON files.
 * Defaults to saving in D:/Crypto/data/Polymarket/backtesting and maintains a latest.json copy.
 */
@ApplicationScoped
public class BacktestJsonExporter {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC);

    private final ObjectMapper objectMapper;
    private final String defaultOutputDir;

    @Inject
    public BacktestJsonExporter(
            ObjectMapper objectMapper,
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String defaultOutputDir) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        this.objectMapper = objectMapper.copy()
                .registerModule(new JavaTimeModule())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.defaultOutputDir = defaultOutputDir != null && !defaultOutputDir.isBlank()
                ? defaultOutputDir
                : "D:/Crypto/data/Polymarket/backtesting";
    }

    public Path export(BacktestResult result, String outputDirectory) throws IOException {
        if (result == null) {
            throw new IllegalArgumentException("result cannot be null");
        }

        String targetDirStr = (outputDirectory != null && !outputDirectory.isBlank())
                ? outputDirectory
                : defaultOutputDir;

        Path targetDir = Path.of(targetDirStr);
        Files.createDirectories(targetDir);

        String timestamp = TIMESTAMP_FORMATTER.format(Instant.now());
        String filename = String.format("backtest_%s_%s.json", result.strategyName(), timestamp);
        Path targetFile = targetDir.resolve(filename);

        // Serialize result to file
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(targetFile.toFile(), result);

        // Update latest.json
        Path latestFile = targetDir.resolve("latest.json");
        Files.copy(targetFile, latestFile, StandardCopyOption.REPLACE_EXISTING);

        return targetFile;
    }

    public String getDefaultOutputDir() {
        return defaultOutputDir;
    }
}

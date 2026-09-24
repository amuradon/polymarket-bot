package cz.polymarket.bot.backtest.cucumber;

import com.github.luben.zstd.ZstdOutputStream;
import cz.polymarket.bot.backtest.data.DownloadJob;
import cz.polymarket.bot.backtest.service.BinanceAggTradesDownloader;
import cz.polymarket.bot.backtest.service.BinanceOrderBookDownloader;
import cz.polymarket.bot.backtest.service.DataDownloadJobManager;
import cz.polymarket.bot.backtest.web.DataDownloadResource;
import cz.polymarket.bot.backtest.web.DataDownloadResource.DownloadApiRequest;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import jakarta.ws.rs.core.Response;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DataDownloadSteps {

    private Path tempBaseDir;
    private HttpClient mockHttpClient;
    private ExecutorService executorService;
    private DataDownloadJobManager jobManager;
    private DataDownloadResource resource;

    private Response lastResponse;
    private String lastJobId;
    @SuppressWarnings("unchecked")
    private Map<String, Object> lastResponseBody;

    @Given("the data storage directory is initialized")
    public void theDataStorageDirectoryIsInitialized() throws IOException {
        tempBaseDir = Files.createTempDirectory("polymarket-cucumber-data");
        mockHttpClient = Mockito.mock(HttpClient.class);
        executorService = Executors.newVirtualThreadPerTaskExecutor();

        BinanceAggTradesDownloader aggTradesDownloader = new BinanceAggTradesDownloader(
                tempBaseDir.toString(),
                "https://data.binance.vision/data/spot/daily/aggTrades",
                "https://data.binance.vision/data/futures/um/daily/aggTrades",
                mockHttpClient
        );

        BinanceOrderBookDownloader orderBookDownloader = new BinanceOrderBookDownloader(
                tempBaseDir.toString(),
                "https://api.cryptohftdata.com/v1",
                Optional.of("test-key"),
                mockHttpClient
        );

        jobManager = new DataDownloadJobManager(aggTradesDownloader, orderBookDownloader, executorService);
        resource = new DataDownloadResource(jobManager);
    }

    @When("a user requests download for symbol {string} from {string} to {string}")
    public void aUserRequestsDownloadForSymbolFromTo(String symbol, String start, String end) throws Exception {
        // Setup default mock responses for downloads if HTTP client is queried
        byte[] dummyZip = createZipPayload(symbol + "-aggTrades-" + start + ".csv", "id,price,qty,first,last,time,isBuyer,isBest");
        byte[] dummyZstdParquet = compressZstd("PAR1-dummy-parquet-data".getBytes(StandardCharsets.UTF_8));

        HttpResponse<byte[]> mockZipResponse = Mockito.mock(HttpResponse.class);
        when(mockZipResponse.statusCode()).thenReturn(200);
        when(mockZipResponse.body()).thenReturn(dummyZip);

        HttpResponse<byte[]> mockParquetResponse = Mockito.mock(HttpResponse.class);
        when(mockParquetResponse.statusCode()).thenReturn(200);
        when(mockParquetResponse.body()).thenReturn(dummyZstdParquet);

        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenAnswer(invocation -> {
                    HttpRequest req = invocation.getArgument(0);
                    if (req.uri().toString().endsWith(".zip")) {
                        return mockZipResponse;
                    }
                    return mockParquetResponse;
                });

        DownloadApiRequest request = new DownloadApiRequest(symbol, start, end, null);
        lastResponse = resource.triggerDownload(request);
        if (lastResponse.getEntity() instanceof Map<?, ?> map) {
            lastResponseBody = (Map<String, Object>) map;
            lastJobId = (String) map.get("jobId");
        }
    }

    @Then("the system accepts the request with status {int}")
    public void theSystemAcceptsTheRequestWithStatus(int statusCode) {
        assertThat(lastResponse.getStatus()).isEqualTo(statusCode);
    }

    @Then("a valid job ID is returned")
    public void aValidJobIdIsReturned() {
        assertThat(lastJobId).isNotNull().isNotBlank();
    }

    @Then("the job downloads spot aggTrades into {string}")
    public void theJobDownloadsSpotAggTradesInto(String subPath) {
        Path dir = tempBaseDir.resolve(subPath);
        await().untilAsserted(() -> {
            assertThat(Files.exists(dir)).isTrue();
            assertThat(Files.list(dir).count()).isGreaterThan(0);
        });
    }

    @Then("the job downloads futures aggTrades into {string}")
    public void theJobDownloadsFuturesAggTradesInto(String subPath) {
        Path dir = tempBaseDir.resolve(subPath);
        await().untilAsserted(() -> {
            assertThat(Files.exists(dir)).isTrue();
            assertThat(Files.list(dir).count()).isGreaterThan(0);
        });
    }

    @Then("the job downloads futures orderbook into {string}")
    public void theJobDownloadsFuturesOrderbookInto(String subPath) {
        Path dir = tempBaseDir.resolve(subPath);
        await().untilAsserted(() -> {
            assertThat(Files.exists(dir)).isTrue();
            assertThat(Files.list(dir).count()).isGreaterThan(0);
        });
    }

    @Given("spot aggTrades file {string} already exists in {string}")
    public void spotAggTradesFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "existing-spot");
    }

    @Given("futures aggTrades file {string} already exists in {string}")
    public void futuresAggTradesFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "existing-futures");
    }

    @Given("futures orderbook file {string} already exists in {string}")
    public void futuresOrderbookFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "existing-orderbook");
    }

    @Then("the job skips the existing files and logs INFO messages")
    public void theJobSkipsTheExistingFilesAndLogsInfoMessages() {
        await().untilAsserted(() -> {
            DownloadJob job = jobManager.getJob(lastJobId).orElseThrow();
            assertThat(job.skippedFiles()).isGreaterThan(0);
        });
    }

    @Then("the system rejects the request with status {int}")
    public void theSystemRejectsTheRequestWithStatus(int statusCode) {
        assertThat(lastResponse.getStatus()).isEqualTo(statusCode);
    }

    @Then("the error message indicates {string}")
    public void theErrorMessageIndicates(String expectedMsg) {
        assertThat(lastResponseBody).isNotNull();
        Object error = lastResponseBody.get("error");
        assertThat(error).isNotNull();
        assertThat(error.toString()).contains(expectedMsg);
    }

    @When("the user queries the status for the returned job ID")
    public void theUserQueriesTheStatusForTheReturnedJobId() {
        lastResponse = resource.getJobStatus(lastJobId);
        if (lastResponse.getEntity() instanceof Map<?, ?> map) {
            lastResponseBody = (Map<String, Object>) map;
        }
    }

    @Then("the job status is returned with progress statistics")
    public void theJobStatusIsReturnedWithProgressStatistics() {
        assertThat(lastResponse.getStatus()).isEqualTo(200);
        assertThat(lastResponseBody).containsKeys("jobId", "status", "symbol", "downloadedFiles", "skippedFiles", "failedFiles");
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

    private byte[] compressZstd(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zos = new ZstdOutputStream(baos)) {
            zos.write(data);
        }
        return baos.toByteArray();
    }
}

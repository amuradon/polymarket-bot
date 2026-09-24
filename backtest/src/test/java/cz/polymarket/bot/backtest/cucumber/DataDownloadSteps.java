package cz.polymarket.bot.backtest.cucumber;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;

public class DataDownloadSteps {

    private Path tempBaseDir;
    private int lastStatusCode;
    private String lastJobId;
    private String lastErrorMessage;

    @Given("the data storage directory is initialized")
    public void theDataStorageDirectoryIsInitialized() throws IOException {
        tempBaseDir = Files.createTempDirectory("polymarket-test-data");
    }

    @When("a user requests download for symbol {string} from {string} to {string}")
    public void aUserRequestsDownloadForSymbolFromTo(String symbol, String start, String end) {
        // Will be wired to the REST resource or coordinator service in Phase 3
    }

    @Then("the system accepts the request with status {int}")
    public void theSystemAcceptsTheRequestWithStatus(int statusCode) {
        // Assertion hook
    }

    @Then("a valid job ID is returned")
    public void aValidJobIdIsReturned() {
        // Assertion hook
    }

    @Then("the job downloads spot aggTrades into {string}")
    public void theJobDownloadsSpotAggTradesInto(String subPath) {
        // Assertion hook
    }

    @Then("the job downloads futures aggTrades into {string}")
    public void theJobDownloadsFuturesAggTradesInto(String subPath) {
        // Assertion hook
    }

    @Then("the job downloads futures orderbook into {string}")
    public void theJobDownloadsFuturesOrderbookInto(String subPath) {
        // Assertion hook
    }

    @Given("spot aggTrades file {string} already exists in {string}")
    public void spotAggTradesFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "dummy-spot-trades");
    }

    @Given("futures aggTrades file {string} already exists in {string}")
    public void futuresAggTradesFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "dummy-futures-trades");
    }

    @Given("futures orderbook file {string} already exists in {string}")
    public void futuresOrderbookFileAlreadyExistsIn(String filename, String subPath) throws IOException {
        Path dir = tempBaseDir.resolve(subPath);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(filename), "dummy-orderbook-parquet");
    }

    @Then("the job skips the existing files and logs INFO messages")
    public void theJobSkipsTheExistingFilesAndLogsInfoMessages() {
        // Assertion hook
    }

    @Then("the system rejects the request with status {int}")
    public void theSystemRejectsTheRequestWithStatus(int statusCode) {
        // Assertion hook
    }

    @Then("the error message indicates {string}")
    public void theErrorMessageIndicates(String expectedMsg) {
        // Assertion hook
    }

    @When("the user queries the status for the returned job ID")
    public void theUserQueriesTheStatusForTheReturnedJobId() {
        // Assertion hook
    }

    @Then("the job status is returned with progress statistics")
    public void theJobStatusIsReturnedWithProgressStatistics() {
        // Assertion hook
    }
}

package cz.polymarket.bot.backtest.data;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DownloadJob {

    private final String jobId;
    private final DownloadRequest request;
    private final AtomicReference<DownloadJobStatus> status;
    private final AtomicInteger downloadedFiles;
    private final AtomicInteger skippedFiles;
    private final AtomicInteger failedFiles;
    private final List<String> errors;
    private final Instant createdAt;
    private volatile Instant completedAt;

    public DownloadJob(DownloadRequest request) {
        this(UUID.randomUUID().toString(), request);
    }

    public DownloadJob(String jobId, DownloadRequest request) {
        this.jobId = jobId;
        this.request = request;
        this.status = new AtomicReference<>(DownloadJobStatus.QUEUED);
        this.downloadedFiles = new AtomicInteger(0);
        this.skippedFiles = new AtomicInteger(0);
        this.failedFiles = new AtomicInteger(0);
        this.errors = new CopyOnWriteArrayList<>();
        this.createdAt = Instant.now();
    }

    public String jobId() {
        return jobId;
    }

    public DownloadRequest request() {
        return request;
    }

    public DownloadJobStatus status() {
        return status.get();
    }

    public void setStatus(DownloadJobStatus newStatus) {
        this.status.set(newStatus);
        if (newStatus == DownloadJobStatus.COMPLETED || newStatus == DownloadJobStatus.FAILED) {
            this.completedAt = Instant.now();
        }
    }

    public int downloadedFiles() {
        return downloadedFiles.get();
    }

    public int incrementDownloaded() {
        return downloadedFiles.incrementAndGet();
    }

    public int skippedFiles() {
        return skippedFiles.get();
    }

    public int incrementSkipped() {
        return skippedFiles.incrementAndGet();
    }

    public int failedFiles() {
        return failedFiles.get();
    }

    public int incrementFailed() {
        return failedFiles.incrementAndGet();
    }

    public void addError(String error) {
        errors.add(error);
    }

    public List<String> errors() {
        return Collections.unmodifiableList(errors);
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant completedAt() {
        return completedAt;
    }

    public void applyResult(DownloadResult result) {
        downloadedFiles.addAndGet(result.downloaded());
        skippedFiles.addAndGet(result.skipped());
        failedFiles.addAndGet(result.failed());
        errors.addAll(result.errors());
    }
}

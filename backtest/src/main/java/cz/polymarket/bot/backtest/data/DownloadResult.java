package cz.polymarket.bot.backtest.data;

import java.util.List;

public record DownloadResult(
        int downloaded,
        int skipped,
        int failed,
        List<String> errors
) {
    public static DownloadResult empty() {
        return new DownloadResult(0, 0, 0, List.of());
    }

    public DownloadResult add(DownloadResult other) {
        var mergedErrors = new java.util.ArrayList<>(this.errors);
        mergedErrors.addAll(other.errors);
        return new DownloadResult(
                this.downloaded + other.downloaded,
                this.skipped + other.skipped,
                this.failed + other.failed,
                List.copyOf(mergedErrors)
        );
    }
}

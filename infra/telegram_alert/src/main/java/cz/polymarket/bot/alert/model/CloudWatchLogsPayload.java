package cz.polymarket.bot.alert.model;

import java.util.List;

/**
 * Structured CloudWatch Logs subscription filter payload decompressed from base64 and GZIP.
 *
 * @param messageType         type of CloudWatch event (e.g. {@code DATA_MESSAGE} or {@code CONTROL_MESSAGE})
 * @param owner               AWS Account ID that owns the log group
 * @param logGroup            name of the CloudWatch log group
 * @param logStream           name of the CloudWatch log stream
 * @param subscriptionFilters list of subscription filter names that matched
 * @param logEvents           list of log events delivered in this payload batch
 */
public record CloudWatchLogsPayload(
        String messageType,
        String owner,
        String logGroup,
        String logStream,
        List<String> subscriptionFilters,
        List<LogEvent> logEvents) {

    public CloudWatchLogsPayload {
        if (messageType == null) {
            messageType = "";
        }
        if (owner == null) {
            owner = "";
        }
        if (logGroup == null) {
            logGroup = "";
        }
        if (logStream == null) {
            logStream = "";
        }
        if (subscriptionFilters == null) {
            subscriptionFilters = List.of();
        } else {
            subscriptionFilters = List.copyOf(subscriptionFilters);
        }
        if (logEvents == null) {
            logEvents = List.of();
        } else {
            logEvents = List.copyOf(logEvents);
        }
    }
}

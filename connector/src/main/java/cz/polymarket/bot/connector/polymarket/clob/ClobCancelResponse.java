package cz.polymarket.bot.connector.polymarket.clob;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Response payload returned by Polymarket CLOB {@code DELETE /order}.
 *
 * @param canceled    list of order IDs successfully canceled
 * @param notCanceled map of order IDs that could not be canceled to reason strings
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ClobCancelResponse(
        List<String> canceled,
        @JsonProperty("not_canceled")
        Map<String, String> notCanceled
) {

    /**
     * Checks whether the specified {@code orderId} was confirmed canceled.
     *
     * @param orderId order ID to check
     * @return true if present in {@code canceled} or absent from {@code notCanceled}
     */
    public boolean isCanceled(String orderId) {
        if (canceled != null && canceled.contains(orderId)) {
            return true;
        }
        return notCanceled == null || !notCanceled.containsKey(orderId);
    }
}

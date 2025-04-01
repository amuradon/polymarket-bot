package cz.amuradon.tralon.newlisting.json;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import cz.amuradon.tralon.connector.mexc.Side;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TradeDetail(
		@JsonProperty("p") BigDecimal price,
		@JsonProperty("v") BigDecimal quantity,
		@JsonProperty("S") Side side,
		@JsonProperty("t") long timestamp
		) {

}

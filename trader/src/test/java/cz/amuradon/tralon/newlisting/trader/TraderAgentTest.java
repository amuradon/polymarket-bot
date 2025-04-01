package cz.amuradon.tralon.newlisting.trader;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import cz.amuradon.tralon.connector.mexc.MexcClient;
import cz.amuradon.tralon.connector.mexc.MexcWsClient;
import cz.amuradon.tralon.connector.mexc.OrderResponse;
import cz.amuradon.tralon.newlisting.trader.RequestBuilder.NewOrderRequestBuilder;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.StatusType;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TraderAgentTest {

	private static final String HMAC_SHA256 = "HmacSHA256";
	
	@Mock
	private MexcClient mexcClientMock;
	
	@Mock
	private MexcWsClient mexcWsClientMock;
	
	@Mock
	private ComputeInitialPrice computeInitialPriceMock;
	
	@Mock
	private Path dataPathMock;
	
	@Mock
	private DataHolder dataHolderMock;
	
	@Mock
	private RequestBuilder requestBuilderMock;
	
	@Mock(answer = Answers.RETURNS_SELF)
	private NewOrderRequestBuilder newOrderRequestBuilderMock;
	
	private TraderAgent agent;
	
	@BeforeEach
	public void prepare() {
		when(requestBuilderMock.newOrder()).thenReturn(newOrderRequestBuilderMock);
		when(newOrderRequestBuilderMock.send()).thenReturn(new OrderResponse("orderId"));
		
		when(dataHolderMock.getInitialBuyPrice()).thenReturn(BigDecimal.ONE);
		
		agent = new TraderAgent(mexcClientMock, mexcWsClientMock, computeInitialPriceMock,
				"secretKey", "10", "RXUSDT", "11:00", dataPathMock, 20, 2, dataHolderMock, requestBuilderMock);
	}

	@Test
	public void testSuccessfulSend() throws Exception {
		agent.placeNewBuyOrder();
		verify(newOrderRequestBuilderMock).send();
	}

	@ParameterizedTest
	@MethodSource("errorResponsePriceData")
	public void errorResponsePrice(String errorMessage, String expectedMaxPrice) throws Exception {
		WebApplicationException webApplicationExceptionMock = mock(WebApplicationException.class);
		Response responseMock = mock(Response.class);
		StatusType statusTypeMock = mock(StatusType.class);
		
		when(webApplicationExceptionMock.getResponse()).thenReturn(responseMock);
		when(responseMock.getStatus()).thenReturn(400);
		when(responseMock.readEntity(ErrorResponse.class))
				.thenReturn(new ErrorResponse("30010", errorMessage));
		when(responseMock.getStatusInfo()).thenReturn(statusTypeMock);
		when(statusTypeMock.getReasonPhrase()).thenReturn("Bad Request");
		
		doThrow(webApplicationExceptionMock).when(newOrderRequestBuilderMock).send();
		agent.placeNewBuyOrder();
		
		// XXX zatim nemam, ze druhy pokus je success...
		verify(newOrderRequestBuilderMock, times(2)).price(new BigDecimal(expectedMaxPrice));
	}
	
	static Stream<Arguments> errorResponsePriceData() {
		return Stream.of(
				Arguments.of("Order price cannot exceed 5USDT", "5"),
				Arguments.of("Order price cannot exceed 0.05USDT", "0.05")
		);
	}
	
}

# Polymarket Mechanics, Order Book Rules & End-to-End Link Latencies

## 1. Binary Market & CLOB Execution Rules

- **15m Crypto Up/Down Resolution**: Resolves strictly on Chainlink Data Streams 60-second TWAP (`twapClose >= twapOpen ? UP : DOWN`). Never approximate settlement using CEX spot prices.
- **Unified CTF Complementarity (`P_up + P_down = 1.0`)**:
  - `UP` and `DOWN` conditional tokens share complementary liquidity on a 1-bps grid (`0..10_000` ticks): a resting `DOWN` bid at `1 - p` is executable as an `UP` ask at `p` (and symmetrically for all four sides).
  - `PolymarketOrderBook` derives effective BBO, `depth1c`, and multi-level VWAP sweeps (`estimateBuyFillableShares`, `estimateBuyFillVwap`, `executeFill`) across both explicit and complementary opposite-token levels without double-counting.
- **Order Constraints & Fees**:
  - Marketable `BUY` orders require integer `wholeShares >= 5` and notional `>= $1.00` (`1,000` millicent-shares), rounded to millicent tick precision (`0.001`).
  - Parabolic crypto taker fee: `feeUsd = shares * rate * p * (1 - p)` (`rate = 0.07`, peaking at `1.75%` around `p = 0.50` and decaying toward `0` near `0.01` / `0.99`).
- **`FAK` (Fill-And-Kill) Matching**:
  - Sweeps available resting liquidity up to `limitPrice` at the exact instant the order reaches the matching engine after the speed bump, cancelling any unfilled remainder immediately.
  - If makers pull or reprice quotes above `limitPrice` before matching, CLOB returns `HTTP 400: {"error":"no orders found to match with FAK order"}` — an expected microstructure non-fill (`REJECTED`), never an application fault.

---

## 2. Polymarket WebSocket Stream Quirks (`/ws/market`, `/ws/user`, PolyBolt)

- **Payload Shapes & Key Order (`/ws/market`)**:
  - Frames arrive both as standalone JSON objects `{...}` and JSON array batches `[{...}]` (`book`, `price_change`, `best_bid_ask`, `last_trade_price`) with non-deterministic JSON key ordering. Both live `PolymarketStreamParser` and Parquet converters (`convert-to-parquet.sh` / `.ps1`) must unwrap arrays per element `event_type` and parse fields regardless of key order.
- **Per-Side Monotonic `server_ts` Filtering**:
  - `UP` and `DOWN` assets maintain independent server timestamps. Always enforce `tryAcceptServerTimestamp(isUp, serverTsMs)` **per token side** (never a single market-wide watermark), especially across redundant `PRIMARY` + `BACKUP` WebSocket legs.
- **Non-Fabricating BBO Hints (`event_code = 4`)**:
  - `best_bid` / `best_ask` fields on `price_change` and `best_bid_ask` omit share quantity. Apply them via `applyBboHint` **exclusively to prune stale crossed levels** (`bid > bestBid` or `ask < bestAsk`) — NEVER insert synthetic `1.0`-share depth.
- **User Stream Reconciliation (`/ws/user`)**:
  - Trade events transition `MATCHED` -> `MINED` -> `CONFIRMED` (or `RETRYING` / `FAILED`). Reconcile `/ws/user` fills by `orderId` against synchronous `POST /order` REST fills so local fills are never double-counted, while still applying external fills or `FAILED` reverts.
- **PolyBolt Oracle Precision (`ws-live-data.polymarket.com`)**:
  - Always parse `full_accuracy_value` (`full_accuracy_price` / `full_accuracy_twap_price`) rather than rounded float `value`, and deduplicate strictly by monotonic `oracleTimestampMs`.

---

## 3. End-to-End Link Latency Map (Measured from AWS `eu-west-1` Dublin `c8gn.xlarge`)

| Link / Feed | Route & Endpoint | Measured Latency (`eu-west-1`) | Engine / Backtest Modeling |
| :--- | :--- | :--- | :--- |
| **Coinbase Spot** (`matches`) | US-East -> Dublin WS | **`~45–55 ms`** one-way (`t_recv - trade_ts`) | `backtest.coinbase.delay-ms = 50` (`side` in `matches` is *maker* side: `'sell'` => taker buy) |
| **Kraken Spot** (`BTC/USD`, `USDT/USD`) | London / Cloudflare -> Dublin WS | **`~35–55 ms`** one-way (`t_recv - trade_ts`) | `backtest.kraken.delay-ms = 50` (seed prior `USDT/USD` peg before warm-up window) |
| **Binance Spot** (`aggTrade`) | Tokyo (`ap-northeast-1`) -> Dublin WS (`~217 ms` RTT) | **`~100–115 ms`** one-way (`t_recv - event_time`) | `backtest.binance.delay-ms = 100` (`COALESCE(event_time, trade_time)`) |
| **PolyBolt Chainlink** (`price.crypto`, `twap`) | Chainlink DON -> PolyBolt -> Dublin WS | **`~1,200–1,800 ms`** effective lag behind direct CEX USD spot | Direct CEX USD spot leads oracle ticks, while 15m resolution anchors strictly on Chainlink 60s TWAP |
| **Polymarket CLOB `/ws/market`** | London (`eu-west-2`) -> CloudFront Dublin (`0.67 ms` TCP `minrtt`) -> EC2 | **`~10–25 ms`** one-way (`~15 ms` baseline `t_recv - server_ts`) | `maxQuoteAgeMs = 250 ms`; observed excess WS lag (`max(0, wsMs - 15)`) adds to congestion in simulation |
| **Local EIP-712 Signing** | `CPU #3` (`order-execution-io-worker`) | **`< 1 ms`** | Included in outbound phase |
| **Polymarket CLOB REST Network RTT** | Dublin <-> Cloudflare Dublin <-> London (`GET /time` on warm HTTP/2) | **`~23–30 ms`** RTT (`~12–15 ms` one-way) | Warm HTTP/2 pool eliminates TCP/TLS handshake; cannot be reduced further from `eu-west-1` |
| **Polymarket CLOB `POST /order` (Taker)** | Outbound + EIP-1271 check + **Server-Side Taker Speed Bump (`~180 ms`)** + Matching + Return | **`~208–226 ms`** total RTT (`220 ms` default) | `SimulatedClobExecutionEngine`: **`89%` (`~196 ms`)** outbound + speed bump before atomic L2 sweep, **`11%` (`~24 ms`)** HTTP/2 response return |

### Key Implications of the `~220 ms` `POST /order` Pipeline
1. **Physical vs. Protocol Limit**: Network transit + EIP-712 signing account for only `~25–30 ms` of the `~220 ms` RTT; the remaining **`~180–190 ms`** is Polymarket's intentional server-side **Taker Speed Bump** + EIP-1271 `DepositWallet` validation. Pre-computation or extra sockets cannot bypass it.
2. **Adverse Selection & Maker Cancellations**: During the `~196 ms` window between `submitOrder` and CLOB matching, makers reacting to CEX impulses can pull stale quotes. Both `PAPER` (`polymarket.paper.order-latency-ms=220`) and `BACKTEST` (`polymarket.backtest.order-gateway-delay-ms=220`) MUST evaluate the L2 sweep at `tSubmit + outboundMatchDelayMs`, never at `tSubmit`.

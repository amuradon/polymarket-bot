# TCP, Socket & WebSocket Disconnect Forensics

## 1. Decoding `ss -tinp` on Market WebSocket Streams

When inspecting `ss -tinp | grep -A 1 "java"` on `c8gn.xlarge` (`eu-west-1`), interpret each field accurately:

| `ss -tinp` Field | Typical Value | Meaning & Forensic Interpretation |
| :--- | :--- | :--- |
| `Recv-Q` / `Send-Q` | `0` / `0` | Unread inbound bytes in kernel TCP buffer / unsent outbound bytes. Persistent `0` proves the JVM `market-ws-io-reader` drains sockets immediately. |
| `bbr` vs `cubic` | `bbr wscale:8,10` | Active TCP congestion control and window scale factors (`snd,rcv`). Under `cubic`, a single Tail Loss Probe on a 32B TLS `PING` collapses `cwnd:7 ssthresh:7`; **`bbr` + `fq` is mandatory**. |
| `app_limited` | Present on all WS | **Benign.** Linux `tcp_rate.c` sets `app_limited` when the **local sender** is not filling `cwnd` (our bot only sends periodic `PING` frames). Never interpret as inbound consumer slowness. |
| `mtu:9001 mss:1428 advmss:8949` | AWS VPC + CDN | `ens34` uses VPC Jumbo Frames (`mtu 9001` -> `advmss 8949`), while internet PMTUD/MSS clamping negotiates `mss:1428` (CloudFront) or `1388` (Cloudflare Anycast encapsulation). |
| `rcv_space` vs `rcv_wnd` | `rcv_space:89490` vs `rcv_wnd:10485760` | `rcv_space` starts at `10 * advmss` (`89,490 B`). **`rcv_wnd` is the actual TCP receive window advertised on the wire** to CloudFront/exchanges and is the decisive flow-control metric (`>= 8–10 MiB` healthy; `< 300 KB` throttles bursts). |
| `rtt` / `minrtt` vs `rcv_rtt` | `rtt:0.75/0.12 minrtt:0.67` vs `rcv_rtt:176` | `minrtt` (`0.67–1.24 ms` to Dublin CloudFront/Cloudflare POPs; `~217 ms` to Tokyo Binance) is true network RTT. `rcv_rtt` is a passive receiver estimate skewed upward on unidirectional inbound streams. |
| `rcv_ooopack` / `bytes_retrans` | `rcv_ooopack:76` | Out-of-order packets received on this socket (common on `eu-west-1 <-> ap-northeast-1` Tokyo Binance route). |

---

## 2. The Sub-Millisecond RTT + Jumbo MTU `rcv_wnd` Trap (`code=1013`)

### Mechanism
1. Polymarket CLOB (`ws-subscriptions-clob.polymarket.com`) terminates at the **Dublin CloudFront POP (`minrtt = 0.67 ms`)** and pushes **~220k–275k frames (~176 MB) per 15m market** with sub-millisecond price-move bursts of **300–600 KB** (`~700–1,000 msg/s`).
2. If `/etc/sysctl.d/99-polymarket-network.conf` sets `net.core.rmem_default = 262144` and `net.ipv4.tcp_rmem = 4096 262144 33554432` (`256 KiB` default), Linux receiver auto-tuning (`tcp_moderate_rcvbuf = 1`) calculates a tiny Bandwidth-Delay Product (`BDP` at `0.67 ms` RTT is only a few kilobytes) and **never grows `sk_rcvbuf` above `256 KiB`**.
3. Combined with VPC `mtu = 9001` (`advmss = 8949`) receiving internet `mss = 1428` / small TLS records (`rcvmss ~ 626`), Linux penalizes the advertised window by the `mss / truesize` ratio, shrinking `rcv_wnd` to **`121–285 KB`**.
4. During a 400 KB CLOB burst, CloudFront briefly hits the `rcv_wnd` ceiling, back-pressuring Polymarket's origin Go WebSocket hub, whose non-blocking per-client Go channel overflows and closes the socket with **`code=1013, reason=slow consumer: send buffer full`** — even though Java `queueDepth = 0` and `maxQueueLagMs <= 4 ms`.

### Production `sysctl` Baseline (`/etc/sysctl.d/99-polymarket-network.conf`)
```ini
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.core.rmem_default = 16777216
net.core.rmem_max = 33554432
net.ipv4.tcp_rmem = 4096 16777216 33554432
net.core.wmem_default = 262144
net.core.wmem_max = 16777216
net.ipv4.tcp_wmem = 4096 262144 16777216
net.ipv4.tcp_moderate_rcvbuf = 1
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_reordering = 32
net.ipv4.tcp_early_retrans = 0
net.ipv4.tcp_autocorking = 0
net.ipv4.tcp_keepalive_time = 30
net.ipv4.tcp_keepalive_intvl = 5
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_retries2 = 8
```
- **Memory note**: Linux only allocates physical pages for **unread** bytes sitting in `Recv-Q` (`~0–1.5 MB` across the entire OS), so `rmem_default = 16 MiB` is completely safe on an `8 GiB` host with `-Xmx4g`.
- **Socket lifecycle note**: `sysctl --system` updates the running kernel immediately without rebooting EC2, but `rmem_default` / `tcp_rmem[1]` are latched into `sk_rcvbuf` **when a new socket is created**. 15m CLOB sockets adopt `rcv_wnd ~ 10.0–10.2 MiB` on the next 15m rollover; long-lived sockets require reconnect or `systemctl restart polymarket-bot`.

---

## 3. WebSocket Disconnect Taxonomy & Upstream Correlation

Always compare Polymarket disconnect counts against **non-Polymarket crypto streams** (`Kraken`, `Coinbase`, `Binance`) over the same uptime window. If exchange streams have `0–1` disconnects over 15+ hours, local EC2/ENA networking is proven clean.

| Stream & Close Code / Error | Root Cause | Expected System Behavior |
| :--- | :--- | :--- |
| `POLYMARKET-CLOB-*` -> `code=1000` (`all subscribed assets resolved`) | Normal post-expiry market resolution (~60–90s after 15m window end) | Expected lifecycle close (`shouldReconnect = false`); `.jsonl.zst` flushes `"market_resolved"` FIFO before closing file, and next lookahead subscribes immediately. |
| `POLYMARKET-CLOB-*` -> `code=1013` (`slow consumer: send buffer full`) | Upstream Polymarket Go hub per-client channel overflow during minute-boundary / volatility broadcast bursts | Active-Active `PRIMARY` + `BACKUP` on **distinct CloudFront IPv4s** absorbs drop with `0 ms` gap (`BOTH STREAMS DOWN = 0`), reconnecting dropped leg in `~50–150 ms`. |
| `POLYBOLT-*` / `CLOB-USER` -> `code=4003` (`draining`), `code=1001` (`server shutting down`), `error(-104)` (`Connection reset by peer`) | Upstream Polymarket rolling deploys or CloudFront edge proxy resets (often simultaneous across `POLYBOLT-TWAP`, `POLYBOLT-CRYPTO-PRICE`, and `CLOB-USER` within 2s) | Fast reconnect (`50–100 ms`) + `auth -> subscribe`. Brief 100–300 ms oracle gap on single-leg PolyBolt is accepted by design. |
| Silent blackhole (no frames, no TCP `RST`) | Intermediate NAT/CDN blackhole without `FIN`/`RST` | Caught within seconds by application **Read-Idle / Pong Watchdog** (`now - lastMessageTimestampMs > idleTimeoutMs`) + kernel `tcp_keepalive_time = 30` / `tcp_retries2 = 8` instead of stalling 15 minutes. |

---

## 4. Multi-Edge IPv4 Diversity & Vert.x Pitfall

- **Why IP diversity matters**: When `PRIMARY` and `BACKUP` land on the **same** CloudFront edge IP (e.g. `18.65.194.62:443`), an edge drain or upstream hitch drops `PRIMARY` (`1013`) and drops `BACKUP` seconds later (`Connection was closed`).
- **Vert.x 4.5.x pitfall**: `HttpClientBase.webSocket(WebSocketConnectOptions)` constructs `EndpointKey` purely from `connectOptions.getHost()` and **silently ignores `connectOptions.setServer(...)`**, while `ChannelProvider.handleConnect` resolves via `context.owner().nettyAddressResolverGroup()`.
- **Production contract (`MarketWebSocketRuntime` + `ReconnectingWebSocketStream`)**:
  - Uses `WebSocketClient` backed by a per-stream `SlotAwareAddressResolverGroup` (wrapping `VertxInternal` / `ContextInternal` / `PromiseInternal`) so TCP connects directly to the slot's chosen IPv4 while preserving logical hostname for **TLS SNI, X.509 SAN hostname verification, and HTTP `Host` header**.
  - Sibling-aware selection excludes both `excludedPeerIp` (active or in-flight `CONNECTING` IP of the sibling leg) and `lastFailedIp` on startup and reconnect.
  - **Verification**: Run `ss -tanp | grep java` and verify no two `PRIMARY`/`BACKUP` legs share the same peer `IP:port`.

---

## 5. Transcontinental Binance (`eu-west-1` <-> Tokyo `217 ms` RTT) & TLS 1.3

- All public Binance WebSocket endpoints (`stream.binance.com`, `data-stream.binance.vision`, `fstream.binance.com`) resolve from `eu-west-1` to AWS **`ap-northeast-1` (Tokyo)** (`~217 ms` RTT, `~105–110 ms` one-way delay, frequent `TCPOFOQueue` / `rcv_ooopack` packet reordering).
- **TLS 1.3 constraint**: Within a single TCP+TLS 1.3 connection, missing TCP segments **cannot** be skipped — TLS 1.3 AEAD uses strict sequential per-record nonces, so out-of-order decryption fails immediately with `BAD_RECORD_MAC`.
- **Two-part mitigation**:
  1. **Kernel**: `bbr` + `tcp_reordering = 32` + `tcp_early_retrans = 0` prevents spurious fast-retransmits and `cwnd` collapse when packets arrive out of order across intercontinental hops.
  2. **Connector**: Dual Active-Active TCP+TLS streams (`PRIMARY` + `BACKUP`) pinned to **two distinct Tokyo edge IPv4s** combined with lock-free monotonic `aggTradeId` (`"a"`) deduplication — whichever path delivers trade `N` without a TCP stall wins immediately, and the delayed copy on the stalled leg is discarded in nanoseconds.

---

## 6. `/proc/net/tcp` Little-Endian Hex & Loopback Contamination

In `/proc/net/tcp` (IPv4 on little-endian x86_64/ARM64 Linux), IPv4 octets `A.B.C.D` are printed reversed as `DDCCBBAA`:
- **`127.0.0.1`** -> `0100007F` (ends with **`7F`**, i.e. `(addr & 0xFF) == 0x7F` for `127.0.0.0/8`).
- **`54.230.114.127`** (a real public CloudFront IP ending in `.127`!) -> `7F72E636` (**starts** with `7F`, ends with `36`). Never check `startsWith("7F")`!
- **SSM/SSH artifact**: Active `aws ssm start-session` / SSH port-forwarding sessions (`sshd-session <-> ssm-session-worker`, `uid=0`, `127.0.0.1:22`) routinely hold `1.5–2.0 MB` in `tx_queue` and trigger `TcpExtTCPToZeroWindowAdv` (`tr=04` probe timer). Always filter `/proc/net/tcp*` to established external sockets owned by the `polymarket-bot` UID (`SystemNetworkTelemetryMonitor.parseProcNetTcpLines`).

# CPU Affinity, Context-Switch, Memory & Capacity Model

## 1. AWS Nitro Hardware Baseline (`c8gn.xlarge` vs Burstable `t4g.*`)

Why `t4g.small` / burstable instances fail market-data peaks while `c8gn.xlarge` runs with >85% headroom:

| Resource | `t4g.small` (Fails Micro-Bursts) | `c8gn.xlarge` (Production Standard) | Operational Implication |
| :--- | :--- | :--- | :--- |
| **vCPUs** | 2x Graviton2 (burstable CPU credits) | **4x physical Graviton4 cores** (Neoverse-V2, `steal = 0 %`) | Deterministic sub-microsecond L1/L2 cache latency 24/7 |
| **ENA Network** | Up to 5 Gbps (~80 Mbps baseline, strict Nitro PPS shaper) | **30 Gbps ENA Nitro v5** (12.5 Gbps baseline, high PPS allowance) | Extreme 6,500 msg/s market burst (~48 Mbps) = **0.16 %** of NIC capacity |
| **EBS Bus** | Shared low baseline (~42 Mbps) | **Dedicated 10 Gbps (1,250 MB/s)** NVMe controller, **physically separate from ENA** | `.jsonl.zst` disk flushes **never** contend with WebSocket NIC bandwidth |
| **System RAM** | 2 GiB (~1.7 GiB usable) | **8 GiB** (`7,761 MiB` usable by OS, `Swap = 0`) | With `-Xms4g -Xmx4g`, **`~2.9–3.0 GiB` remains free** for OS, TCP & Zstd |

---

## 2. Strict 4-Core Thread Pinning Architecture (`CPU #0..#3`)

Every hot thread must be pinned via `LinuxCpuAffinity` (`sched_setaffinity`) and verified in `/proc/$PID/task/*/status` (`Cpus_allowed_list`):

| Physical Core | Assigned Threads & Subsystems | Avg CPU | Peak Burst CPU | Strict Isolation Rule |
| :--- | :--- | :--- | :--- | :--- |
| **`CPU #0`** | `systemd` default (`CPUAffinity=0`), ENA IRQs (`ens34-Tx-Rx-*`), ZGC workers, `clob-collector-worker` (Zstd `.jsonl.zst`), `clob-user-worker`, Paper JSON I/O, DNS worker, Quarkus HTTP `:443` | `~2–4 %` | `~15–20 %` | Absorbs **all** syscalls, VFS/XFS journal commits (`1–5 ms`), interrupts, GC, and REST telemetry |
| **`CPU #1`** | **`market-ws-io-reader`** (1x `EpollEventLoop` + `BoringSSL` reading all market WebSockets, decoding UTF-8 once into `TimestampedFrame`, zero-copy MPSC fan-out) | `~5–15 %` | `~35–45 %` | **Strictly 1 thread.** Never run blocking DNS, JSON parsing, or disk writes here |
| **`CPU #2`** | **`market-strategy-worker`** (Parse-Once `PolymarketLocalBook` + `PolyBolt` + `CalculatedSpotTwapEngine` + 1..5 lock-free `TradingStrategy` slots in L1 cache) | `~1–3 %` | `~10–15 %` | **Strictly 1 thread.** Zero locks, zero heap boxing, independent L1 `PayloadDeduplicator` |
| **`CPU #3`** | **`order-execution-io-worker`** (EIP-712 order signing + dedicated CLOB `POST /order` HTTP client) | `< 0.5 %` | `~15 %` | **Strictly 1 thread.** Decoupled from `CLOB-USER` WS so `200–500 ms` REST order validation never lags user fills |

### Known Thread-Leak & Affinity Traps to Check
1. **Quarkus ArC `_ClientProxy` Constructor Trap**: `@ApplicationScoped` beans like `MarketWebSocketRuntime` receive a generated subclass proxy calling `super()`. Always keep a package-private no-arg constructor that allocates **zero `Vertx` instances and zero threads**, putting runtime creation inside `@Inject`.
2. **Child Thread Affinity Inheritance**: Any thread lazily spawned from `CPU #1` (e.g. Vert.x `executeBlocking` DNS resolver) inherits `Cpus_allowed_list: 1` from its caller! Eagerly initialize helper workers at startup and explicitly pin them to `CPU #0`, while capping `VertxOptions.setWorkerPoolSize(1)` and `setInternalBlockingPoolSize(1)`.
3. **Unified CLOB Topology (`<= 3` Market Streams)**: Active market uses `2` shared legs (`PRIMARY` + `BACKUP`) feeding both `CPU #2` Strategy and `CPU #0` `clob-collector-worker` (each with its own lock-free `PayloadDeduplicator` so `.jsonl.zst` has **zero duplicates**), plus `1` single-leg lookahead stream (`lookahead-count = 1`). In `OFF` mode, `BACKUP` closes immediately (`2` single-leg streams total).

---

## 3. Diagnosing High `vmstat` Context Switches (`cs`) & Interrupts (`in`)

When `vmstat 1` shows elevated `cs` (e.g. `7,000–9,600/s`) despite `97–100 %` idle CPU:
1. Measure **5-second per-thread deltas** of `voluntary_ctxt_switches` (`+vcs`) and `nonvoluntary_ctxt_switches` (`+nvcs`) across `/proc/$PID/task/*/status`.
2. **Interpretation**:
   - **`+nvcs > 0` on `CPU #1..#3`**: Preemptive time-slice contention — more than one runnable thread or IRQ is competing on an isolated hot core. Fix affinity immediately.
   - **High `+vcs` (`> 1,000/s` per thread) with `us + sy < 3 %`**: Over-short idle park intervals (`LockSupport.parkNanos`) or too-short spin-before-park causing repeated Linux `futex` sleep/wake syscalls on every market tick (`~500–1,500 msg/s`). Use a calibrated micro-spin window (e.g. `~300 us` on `market-strategy-worker`) on dedicated cores before parking, and consolidate collector draining onto a single `NORM_PRIORITY` `clob-collector-worker` on `CPU #0` instead of per-market `MAX_PRIORITY` spinning drain threads.

---

## 4. ZGC Memory Accounting & Linux OOM Safety (`Swap = 0`)

- With `-XX:+UseZGC -XX:+AlwaysPreTouch` and `-Xms == -Xmx`, ZGC maps heap backing memory via `memfd` (`RssShmem` in `/proc/$PID/status`), locking the **entire `-Xmx` upfront** even when live Java heap (`usedHeapMb`) is only `150–590 MiB`.
- **Sizing Formula on `8 GiB` EC2 (`7,761 MiB` OS total, `Swap = 0`)**:
  $$\text{Available OS RAM} = 7761\text{ MiB} - \text{Xmx} - \text{JVM Metaspace/CodeCache/Direct/BoringSSL }(\approx 650\text{ MiB}) - \text{OS/Agents }(\approx 250\text{ MiB})$$
  - `-Xmx6g` (`6,144 MiB`) -> leaves **`< 1 GiB` (`~700–990 MiB`) `available`** -> dangerous OOM Killer risk during TLS/TCP/Zstd bursts.
  - **`-Xms4g -Xmx4g` (`4,096 MiB`)** -> leaves **`~2.8–3.0 GiB` `available`** (>6x headroom over live heap + full safety margin).
- Always pass **`-Djava.net.preferIPv4Stack=true`** on IPv4-only VPC subnets to eliminate dual-stack `[::ffff:...]` sockets and kernel `Ip6OutNoRoutes` errors.

---

## 5. Zstandard (`.jsonl.zst`) & EBS Throughput Capacity Model

Use these measured Graviton4 benchmarks when evaluating new recorded streams or multi-strategy expansion:

- **Parse-Once Strategy Scaling (`CPU #2`)**:
  - `PolymarketLocalBook` + `PolyBolt` + `CalculatedSpotTwapEngine` = **`~0.41 %` of 1 core**.
  - 1 `TradingStrategy` evaluation = `~200–500 ns` (`~0.05 %` of 1 core) -> **5 concurrent strategies = `~0.25 %` of 1 core (`~1–2.5 us/tick`)**.
- **Native `zstd-jni` (Level 3) on `CPU #0`**:
  - Compression speed on 1 Graviton4 core: **`~500 MB/s`** (`~7.3 : 1` JSON ratio overall; `~9 : 1` on CLOB, `~6 : 1` on `bookTicker`).
  - Raw stream rates -> compressed disk rates:
    - Polymarket CLOB (active + lookahead): `~200 KB/s` avg (`2.5 MB/s` peak) -> **`~22 KB/s` avg (`280 KB/s` peak)**, `~1.8–2.2 GB/day`.
    - Polymarket PolyBolt (`twap` + `crypto`): `~1 KB/s` avg -> **`< 0.2 KB/s`**, `~15 MB/day`.
    - Optional Binance Spot + Futures `bookTicker`: `~180 KB/s` avg (`1.0 MB/s` peak) -> **`~30 KB/s` avg (`170 KB/s` peak)**, `~2.4–2.8 GB/day`.
  - Total Zstd CPU cost on `CPU #0`: **`~0.08 %` avg, `< 1.5 %` peak**.
  - Native RAM per open `ZstdOutputStream`: `~1.5–2.5 MiB` `ZSTD_CCtx` + `64 KiB` buffer (`~20–25 MiB` total for ~10 open files).
  - Baseline `100 GiB` `gp3` EBS (`3,000 IOPS`, `125 MB/s`): actual load with `64 KiB` buffer + `5s` flush is **`3–6 IOPS/s` (`~20 IOPS` on flush) and `0.05–0.45 MB/s` (`< 0.5 %` of disk bandwidth)**, storing ~20–45 days depending on enabled streams.

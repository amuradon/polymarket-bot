---
name: network-and-performance-analysis
description: >-
  End-to-end forensic runbook for Linux OS, AWS ENA NIC, kernel TCP/IP stack
  (ss -tinp, /proc/net/tcp, nstat, softnet_stat, sysctl BBR/rmem), WebSocket
  disconnect root-cause analysis, 4-core Graviton CPU affinity and thread
  context-switch profiling, ZGC/OS memory headroom, and EBS/Zstd capacity
  evaluation on polymarket-bot EC2 instances in eu-west-1. Use when diagnosing
  connection drops, slow-consumer (1013) closes, high latency, packet reordering,
  context-switching overhead, telemetry anomalies, or sizing EC2 capacity.
---

# Network & Performance Forensic Analysis Guide

## 1. Scope & Guardrails

- **AWS Region & Access**: Production runs in **`eu-west-1` (Dublin)** (`c8gn.xlarge`, Amazon Linux 2023 ARM64, `polymarket-bot.service`). Public SSH (`:22`) is closed; collect all host diagnostics non-interactively via **AWS MCP Server** (`aws___run_script` calling `ssm:SendCommand` with `AWS-RunShellScript`).
- **Secret Safety**: NEVER run `secretsmanager get-secret-value`, dump `/proc/$PID/environ`, or print private keys/API credentials during diagnostics.
- **Evidence Before Hypotheses**: Never assume WebSocket disconnects or scary `ss -tinp` flags mean host overload. Execute the **7-layer bottom-up sweep** first, separate **upstream CDN behavior & measurement artifacts** from **local OS/JVM/code bottlenecks**, and quantify every claim with counters and 5-second deltas.

---

## 2. Step-by-Step Diagnostic Workflow (5 Steps)

### Step 1 — Collect Coherent Host & Application Snapshot (via SSM)
Run [scripts/collect-host-diagnostics.sh](scripts/collect-host-diagnostics.sh) on the target EC2 instance via `ssm:SendCommand` (`eu-west-1`) alongside `ec2:DescribeInstances` / `DescribeInstanceStatus`. Always sample **two points 5 seconds apart** (`t0` and `t0 + 5s`) so cumulative boot-time counters are never confused with active degradation.

### Step 2 — Evaluate the 7-Layer Health & Telemetry Matrix

| Layer | Primary Data Sources | Healthy Production Baseline (`c8gn.xlarge`) | Actionable Failure Signature |
| :--- | :--- | :--- | :--- |
| **1. Nitro & ENA NIC** | `ethtool -S ens34` (5s delta), `ethtool -c ens34`, `ip -s -s link`, `/proc/interrupts` | `dropped = 0`, `missed = 0`, `overrun = 0`; `+0/5s` delta on `bw_in/out_allowance_exceeded`, `pps_allowance_exceeded`, `conntrack_allowance_exceeded`; ENA IRQs pinned to `CPU #0` (`irqbalance` inactive, `rx-usecs=25`, `tx-usecs=64`) | Positive 5s delta on any `*_allowance_exceeded` (undersized instance like `t4g.small`) or ENA IRQs firing on `CPU #1..#3` |
| **2. Kernel SoftIRQ & TCP Stack** | `/proc/net/softnet_stat`, `nstat -az` (5s delta), `sysctl` (`/etc/sysctl.d/99-polymarket-network.conf`) | `softnet_stat` col 2 (`dropped`) = `0` & col 3 (`time_squeeze`) = `0` on all 4 cores; `TCPRcvCollapsed = 0`, `TCPBacklogDrop = 0`; `fq` + `bbr` active; `rmem_default = 16777216`, `tcp_rmem = 4096 16777216 33554432`, `tcp_wmem = 4096 262144 16777216`, `tcp_reordering = 32`, `tcp_early_retrans = 0`; `Ip6OutNoRoutes = 0` | `cubic` collapsing `cwnd:7` after ping TLP; `rmem_default = 256 KiB` shrinking `rcv_wnd` to `121–285 KB`; dual-stack IPv6 sockets (`[::ffff:...]`) incrementing `Ip6OutNoRoutes` |
| **3. Per-Socket TCP & Edge Diversity** | `ss -tinp`, `ss -tanp`, `/proc/net/tcp`, `getent ahostsv4 <host>` | All `java` sockets IPv4 (`Recv-Q = 0`, `Send-Q = 0`), `bbr` active, `rcv_wnd >= 8 MiB` (`wscale:*,10`); `PRIMARY` and `BACKUP` legs connected to **distinct remote IPv4s**; max `3` CLOB market sockets (`2` active + `1` lookahead) + `1` user socket | `PRIMARY` & `BACKUP` sharing the same CloudFront/Binance edge IP; `rcv_wnd < 512 KB` on high-burst CLOB sockets; >3 CLOB market streams open |
| **4. OS RAM, ZGC & NVMe EBS** | `free -m`, `/proc/$PID/status` (`RssShmem`, `VmRSS`), `df -h`, `vmstat 1 5` (`wa`) | `-Xms4g -Xmx4g` ZGC on 8 GiB host (`Swap = 0`) leaves **`available >= 2.5 GiB`** for kernel TCP buffers, Netty direct memory, BoringSSL & Zstd (`~20–25 MiB`); EBS `gp3` `iowait = 0.0 %` | `-Xms6g -Xmx6g` (`AlwaysPreTouch` locking 6 GiB in `RssShmem`) leaving `< 1 GiB` `available` -> acute Linux OOM Killer risk |
| **5. CPU Affinity & Context Switches** | `vmstat 1 5` (`in`, `cs`, `us`, `sy`, `id`), `/proc/$PID/task/*/status` (`Cpus_allowed_list`, 5s delta of `voluntary_ctxt_switches` & `nonvoluntary_ctxt_switches`) | `CPU #0`: OS/IRQs/ZGC/`clob-collector-worker`/`clob-user-worker`/REST; `CPU #1`: **strictly 1 thread** (`market-ws-io-reader`); `CPU #2`: **strictly 1 thread** (`market-strategy-worker`); `CPU #3`: **strictly 1 thread** (`order-execution-io-worker`); `+nvcs ~ 0/5s` on `CPU #1..#3` | Unpinned worker pools (`Cpus_allowed = 0-3`), multiple threads competing on `CPU #1..#3` (`+nvcs > 0`), or short park/unpark `futex` storms (`cs > 5,000/s`) |
| **6. App Runtime Telemetry** | `GET http://127.0.0.1:443/api/health`, `/api/telemetry`, `/api/collector/status` | `healthy: true`, `nativeTransportEnabled: true` (`Epoll`), `openSslAvailable: true` (`BoringSSL`), market `queueDepth = 0`, `maxQueueLagMs <= 5 ms`, `staleQuotesSkipped = 0`, `maxQuoteAgeMsObserved <= 10 ms` | `maxQueueLagMs > 50 ms` (e.g. blocking REST `POST /order` sharing the `CLOB-USER` loop -> `1,300+ ms` lag), or non-zero `currentTcpTxQueueBytes` caused by un-filtered `127.0.0.1:22` sockets |
| **7. Disconnect & Log Forensics** | `/var/log/polymarket-bot/bot.log` (`WARN`/`ERROR`, close codes, timestamps) | **`BOTH STREAMS DOWN = 0`**; crypto exchange baselines (`Kraken`, `Coinbase`, `Binance`) show `~0` unexpected drops (proving local AWS network is clean); `FAK` non-fills logged at `INFO`/`WARN` (never `ERROR`) | Correlated drops across both `PRIMARY` & `BACKUP` on the same edge IP; silent blackholes due to missing read-idle/pong watchdog; `BOTH STREAMS DOWN > 0` |

### Step 3 — Separate False Positives from True Root Causes
Before proposing fixes, validate every suspect metric against known Linux/CDN pitfalls (detailed in [references/tcp-and-disconnect-forensics.md](references/tcp-and-disconnect-forensics.md)):

1. **`app_limited` in `ss -tinp` is NOT consumer slowness**: In Linux `tcp_rate.c`, `app_limited` measures the **outbound transmit path** (bot only sends tiny `PING` frames every 10s, so `Send-Q = 0`). Check `Recv-Q`, `maxTcpRxQueueBytes`, and `maxQueueLagMs` for inbound health.
2. **Low `rcv_space` (`~89 KB`) & inflated `rcv_rtt` (`90–530 ms` vs `0.67 ms` `minrtt`)**: VPC Jumbo Frames (`mtu 9001` -> `advmss 8949`) skew initial `rcv_space`, while unidirectional inbound streams skew passive `rcv_rtt`. Always inspect **`rcv_wnd`** (must be `>= 8 MiB` after `16 MiB` `tcp_rmem` tuning) and **`minrtt`**.
3. **Why `rmem_default = 256 KiB` causes `code=1013` on Dublin CloudFront**: With `minrtt = 0.67 ms` to Dublin CloudFront and VPC `mtu 9001` vs internet `mss 1428` (`mss/truesize` penalty), Linux `tcp_moderate_rcvbuf` computes a tiny BDP and never grows `sk_rcvbuf`, advertising `rcv_wnd = 121–285 KB`. Sub-ms 300–600 KB Polymarket bursts stall at CloudFront and trigger Polymarket Go hub's `1013 (slow consumer: send buffer full)` even when Java `queueDepth = 0`.
4. **Loopback SSH-over-SSM (`127.0.0.1:22`, `uid=0`) polluting `/proc/net/tcp` & `ZeroWindow`**: Always filter `/proc/net/tcp*` by socket state (`01`), non-loopback address (note little-endian hex: `127.0.0.1` ends in `7F` -> `0100007F`, whereas `54.230.114.127` starts with `7F` -> `7F72E636`), and the `polymarket-bot` effective `uid`.
5. **Static Boot-Time Counters**: ENA `pps_allowance_exceeded` (e.g. `101`) or `TCPOFOQueue` accumulate since boot; only a non-zero delta during live load indicates an active issue.

### Step 4 — Apply Live OS vs. Code Remediation Safely
- **Kernel `sysctl` changes (`/etc/sysctl.d/99-polymarket-network.conf` -> `sysctl --system`)**:
  - **No EC2 reboot required.** Congestion control, reordering, and keepalive parameters apply immediately; socket buffer defaults (`rmem_default`, `tcp_rmem`, `tcp_wmem`) apply **at `socket()` creation time**.
  - Rotating 15m Polymarket CLOB sockets pick up the new `rcv_wnd` automatically on the next `:00/:15/:30/:45` rollover; long-lived sockets (Binance, Kraken, Coinbase, PolyBolt) retain their old buffer until reconnect or `systemctl restart polymarket-bot`.
  - Always persist matching changes in `infra/polymarket-bot-ec2.yaml` and validate via `aws cloudformation validate-template`.
- **Hot-Path & Thread Isolation Changes**: Follow [references/cpu-memory-and-capacity-model.md](references/cpu-memory-and-capacity-model.md) so disk/Zstd I/O, DNS workers, and Quarkus ArC proxy constructors never touch `CPU #1..#3`.

### Step 5 — Post-Change Verification
1. Confirm `ss -tinp | grep -A 1 java` shows `bbr`, `rcv_wnd >= 8 MiB` on newly opened sockets, `Recv-Q = 0`, and **distinct remote IPv4s** on every `PRIMARY`/`BACKUP` pair (`ss -tanp | grep java`).
2. Confirm `/proc/$PID/task/*/status` shows strictly `1` thread on `CPU #1`, `1` on `CPU #2`, `1` on `CPU #3`, and `GET http://127.0.0.1:443/api/health` returns `.healthy == true`.

---

## 3. Progressive Deep-Dive References

Load **only** when needed for your current investigation:

- **[references/tcp-and-disconnect-forensics.md](references/tcp-and-disconnect-forensics.md)**: `ss -tinp` & `/proc/net/tcp` little-endian hex reference, VPC Jumbo MTU + low-RTT `rcv_wnd` math, WebSocket close-code taxonomy (`1000`, `1001`, `1013`, `4003`, `-104`), Vert.x multi-edge IPv4 routing, and Tokyo Binance (`217 ms` RTT) TLS 1.3 reordering mitigation.
- **[references/cpu-memory-and-capacity-model.md](references/cpu-memory-and-capacity-model.md)**: `c8gn.xlarge` vs burstable Nitro architecture, 4-core pinning & `futex` context-switch diagnostics, Quarkus ArC / Vert.x worker affinity pitfalls, ZGC `memfd` RAM sizing, and Zstandard `.jsonl.zst` + EBS capacity formulas.
- **[scripts/collect-host-diagnostics.sh](scripts/collect-host-diagnostics.sh)**: Read-only 7-layer bash collector with 5-second delta sampling for AWS SSM `AWS-RunShellScript`.
- **[../project-architecture/references/polymarket-mechanics-and-latencies.md](../project-architecture/references/polymarket-mechanics-and-latencies.md)**: Empirically measured `eu-west-1` baseline latencies across CEX WebSockets, PolyBolt, CLOB `/ws/market`, HTTP/2 `GET /time` (`23–30 ms`), and `POST /order` (`208–226 ms` including Polymarket's `~180 ms` server-side Taker Speed Bump).

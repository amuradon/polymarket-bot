#!/usr/bin/env bash
# Read-only 7-layer OS, ENA, TCP, CPU/thread-affinity, ZGC, and app telemetry snapshot
# Execute on the target EC2 instance in eu-west-1 via AWS SSM (AWS-RunShellScript).
set -uo pipefail

echo "=== 1. HOST, UPTIME, MEMORY & DISK ==="
uname -a
uptime
free -m
swapon --show || true
df -h / /mnt/ebs/polymarket-bot 2>/dev/null || df -h
vmstat 1 5

echo "=== 2. KERNEL SYSCTL & CONGESTION CONTROL ==="
cat /etc/sysctl.d/99-polymarket-network.conf 2>/dev/null || true
sysctl net.core.default_qdisc net.ipv4.tcp_congestion_control net.ipv4.tcp_available_congestion_control \
  net.core.rmem_default net.core.rmem_max net.ipv4.tcp_rmem \
  net.core.wmem_default net.core.wmem_max net.ipv4.tcp_wmem \
  net.ipv4.tcp_moderate_rcvbuf net.ipv4.tcp_reordering net.ipv4.tcp_early_retrans net.ipv4.tcp_autocorking \
  net.ipv4.tcp_keepalive_time net.ipv4.tcp_keepalive_intvl net.ipv4.tcp_keepalive_probes net.ipv4.tcp_retries2 2>/dev/null || true

echo "=== 3. ENA NIC, IRQ AFFINITY & SOFTNET (T0) ==="
NIC=$(ip -o -4 route show to default | awk '{print $5}' | head -n1)
echo "NIC=${NIC:-unknown}"
if [ -n "${NIC:-}" ]; then
  ip -s -s link show "$NIC"
  ethtool -c "$NIC" 2>/dev/null || true
  ethtool -S "$NIC" | grep -E "allowance|drop|miss|err|overrun" > /tmp/diag_ena_t0.txt || true
  echo "irqbalance: $(systemctl is-active irqbalance 2>/dev/null || echo inactive)"
  for irq in $(awk -v nic="$NIC" '$0 ~ nic {gsub(":", "", $1); print $1}' /proc/interrupts); do
    aff=$(cat "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || echo "?")
    echo "IRQ $irq affinity=$aff :: $(awk -v q="$irq:" '$1 == q {print $0}' /proc/interrupts)"
  done
fi
cat /proc/net/softnet_stat
nstat -az | grep -E "ZeroWindow|OFO|Collapse|Drop|Abort|Retrans|Timeouts|Ip6OutNoRoutes" > /tmp/diag_nstat_t0.txt || true

echo "=== 4. JVM PROCESS & THREAD SNAPSHOT (T0 -> T0+5s) ==="
systemctl status polymarket-bot.service --no-pager -l | head -n 25 || true
PID=$(pgrep -f "quarkus-run.jar|bot-.*\.jar" | head -n1 || true)
echo "JAVA_PID=${PID:-none}"

snapshot_threads() {
  local pid="$1" out="$2"
  : > "$out"
  for t in "/proc/$pid/task"/*; do
    [ -d "$t" ] || continue
    tid=$(basename "$t")
    comm=$(tr -d '\n' < "$t/comm" 2>/dev/null || echo "?")
    aff=$(awk '/Cpus_allowed_list:/ {print $2}' "$t/status" 2>/dev/null || echo "?")
    vcs=$(awk '/^voluntary_ctxt_switches:/ {print $2}' "$t/status" 2>/dev/null || echo 0)
    nvcs=$(awk '/^nonvoluntary_ctxt_switches:/ {print $2}' "$t/status" 2>/dev/null || echo 0)
    echo "$tid|$comm|$aff|$vcs|$nvcs" >> "$out"
  done
}

if [ -n "${PID:-}" ]; then
  ps -o pid,ppid,uid,lstart,etime,pcpu,pmem,rss,vsz,nlwp,args -p "$PID"
  grep -E "Uid|State|Threads|Vm|Rss|Cpus_allowed|CapEff|voluntary" "/proc/$PID/status"
  snapshot_threads "$PID" /tmp/diag_threads_t0.txt
fi

sleep 5

echo "=== 5. 5-SECOND DELTAS (ENA, TCP NSTAT & THREAD CONTEXT SWITCHES) ==="
if [ -n "${NIC:-}" ]; then
  ethtool -S "$NIC" | grep -E "allowance|drop|miss|err|overrun" > /tmp/diag_ena_t1.txt || true
  echo "--- ENA T0 vs T1 ---"
  paste /tmp/diag_ena_t0.txt /tmp/diag_ena_t1.txt
fi
nstat -az | grep -E "ZeroWindow|OFO|Collapse|Drop|Abort|Retrans|Timeouts|Ip6OutNoRoutes" > /tmp/diag_nstat_t1.txt || true
echo "--- NSTAT T0 vs T1 ---"
paste /tmp/diag_nstat_t0.txt /tmp/diag_nstat_t1.txt

if [ -n "${PID:-}" ]; then
  snapshot_threads "$PID" /tmp/diag_threads_t1.txt
  echo "Total JVM threads: $(wc -l < /tmp/diag_threads_t1.txt)"
  echo "--- Thread CPU Affinity Distribution ---"
  awk -F'|' '{print $3}' /tmp/diag_threads_t1.txt | sort | uniq -c
  echo "--- Non-CPU0 Pinned Threads (Must be 1 on CPU 1, 1 on CPU 2, 1 on CPU 3) ---"
  awk -F'|' '$3 != "0" {printf "%6s | %-30s | cpus=%s\n", $1, $2, $3}' /tmp/diag_threads_t1.txt
  echo "--- Top 20 Context-Switching Threads over 5s ---"
  join -t'|' -1 1 -2 1 <(sort /tmp/diag_threads_t0.txt) <(sort /tmp/diag_threads_t1.txt) | awk -F'|' '{
    dv = $8 - $4; dnv = $9 - $5;
    printf "%6d | %-28s | cpus=%-5s | +vcs=%6d (+%6.1f/s) | +nvcs=%4d\n", $1, $6, $7, dv, dv/5.0, dnv
  }' | sort -t'=' -k3 -nr | head -n 20
fi

echo "=== 6. PER-SOCKET TCP FORENSICS & EDGE IP DIVERSITY ==="
ss -s
echo "--- Java Remote IP:Port Mapping (Check PRIMARY vs BACKUP Diversity) ---"
ss -tanp | grep "java" || true
echo "--- Java Detailed TCP Info (ss -tinp) ---"
ss -tinp | grep -A 1 "java" | head -n 120 || true
echo "--- /proc/net/tcp Established Sockets (Col 4 == 01, Check UID Col 8 & Loopback) ---"
awk 'NR==1 || $4=="01" {print $1, $2, $3, $4, $5, $8, $10}' /proc/net/tcp

echo "=== 7. APPLICATION TELEMETRY & LOG DISCONNECT SUMMARY ==="
curl -fsS http://127.0.0.1:443/api/health 2>/dev/null; echo ""
curl -fsS http://127.0.0.1:443/api/collector/status 2>/dev/null; echo ""
curl -fsS http://127.0.0.1:443/api/telemetry 2>/dev/null | head -c 8000; echo ""

if [ -f /var/log/polymarket-bot/bot.log ]; then
  echo "BOTH STREAMS DOWN count: $(grep -c 'BOTH STREAMS DOWN' /var/log/polymarket-bot/bot.log || true)"
  echo "--- Disconnect Counts by Stream & Close Code ---"
  grep "\[WARN\]" /var/log/polymarket-bot/bot.log | grep "WebSocket " | \
    sed -E 's/.*\[([^]]+)\] WebSocket ([^ ]+ [^ ]+) \(remote=([^,]+), code=([^,]+), reason=([^,]+).*/\2 | code=\4 | reason=\5/' | \
    sort | uniq -c | sort -rn | head -n 25 || true
  echo "--- Last 30 WARN/ERROR Lines ---"
  grep -E "\[WARN\]|\[ERROR\]" /var/log/polymarket-bot/bot.log | tail -n 30 || true
fi

echo "=== 8. DMESG KERNEL ALERTS ==="
dmesg -T | grep -iE "oom|kill|drop|ena|nvme|xfs|conntrack|error" | tail -n 25 || true
rm -f /tmp/diag_ena_t0.txt /tmp/diag_ena_t1.txt /tmp/diag_nstat_t0.txt /tmp/diag_nstat_t1.txt /tmp/diag_threads_t0.txt /tmp/diag_threads_t1.txt

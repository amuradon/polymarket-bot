#!/usr/bin/env python3
"""DNS resolution patch pro obcházení DNS blokování a hijackingu domén Polymarket.

Tento modul řeší problém, kdy někteří poskytovatelé internetu (např. v ČR ISP UVT Net
prostřednictvím Whalebone DNS filtrování) blokují přístup k doménám *.polymarket.com.
Při této blokaci DNS resolver vrátí adresu 159.100.251.128 (redirect.whalebone.io)
se self-signed certifikátem (sad.certificate.that.cannot.be.valid.com), což v aplikaci
vyvolá chybu:
    [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate

Modul instaluje transparentní hook do `socket.getaddrinfo`, který pro všechny domény
`*.polymarket.com` překládá IP adresy přímo přes veřejné necenzurované DNS servery
(Cloudflare 1.1.1.1, Google 8.8.8.8) s in-memory mezipamětí a fallbackem na statické
Anycast IP adresy.
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
from typing import Any

logger = logging.getLogger("polymarket-dns")

# Bezpečné veřejné DNS servery
PUBLIC_DNS_SERVERS = ["1.1.1.1", "8.8.8.8", "1.0.0.1", "8.8.4.4"]

# Statický fallback v případě, že selže UDP i dnspython (např. v přísném firewall prostředí)
FALLBACK_IPS: dict[str, list[str]] = {
    "clob.polymarket.com": ["104.18.34.205", "172.64.153.51"],
    "data-api.polymarket.com": ["104.18.34.205", "172.64.153.51"],
    "gamma-api.polymarket.com": ["18.66.26.34", "18.66.26.73", "18.66.26.29", "18.66.26.77"],
    "relayer-v2.polymarket.com": ["104.18.34.205", "172.64.153.51"],
    "ws-subscriptions-clob.polymarket.com": ["99.84.91.109", "99.84.91.80", "99.84.91.104", "99.84.91.122"],
    "polymarket.com": ["104.18.34.205", "172.64.153.51"],
}
DEFAULT_FALLBACK_IPS = ["104.18.34.205", "172.64.153.51"]

# Cache a zámek
_cache: dict[str, list[str]] = {}
_cache_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _query_dns_udp(domain: str, dns_ip: str, timeout: float = 2.0) -> list[str]:
    """Získá A záznamy pro doménu přes UDP socket na zadaný DNS server bez externích závislostí."""
    tid = 0x504F  # 'PO'
    flags = 0x0100  # standard query, recursion desired
    header = struct.pack("!HHHHHH", tid, flags, 1, 0, 0, 0)
    qname = b"".join(bytes([len(p)]) + p.encode("ascii") for p in domain.split(".")) + b"\x00"
    question = qname + struct.pack("!HH", 1, 1)  # QTYPE=A (1), QCLASS=IN (1)
    packet = header + question

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(packet, (dns_ip, 53))
        data, _ = sock.recvfrom(1024)
    finally:
        sock.close()

    # Kontrola délky a ID transakce
    if len(data) < 12 or struct.unpack("!H", data[:2])[0] != tid:
        return []

    # Přeskočení hlavičky a otázky
    idx = 12
    while idx < len(data) and data[idx] != 0:
        if (data[idx] & 0xC0) == 0xC0:
            idx += 2
            break
        idx += 1 + data[idx]
    else:
        idx += 1
    idx += 4  # QTYPE + QCLASS

    # Parsování odpovědí
    ips: list[str] = []
    ancount = struct.unpack("!H", data[6:8])[0]
    for _ in range(ancount):
        if idx >= len(data):
            break
        if (data[idx] & 0xC0) == 0xC0:
            idx += 2
        else:
            while idx < len(data) and data[idx] != 0:
                idx += 1 + data[idx]
            idx += 1
        if idx + 10 > len(data):
            break
        rtype, _, _, rdlen = struct.unpack("!HHIH", data[idx : idx + 10])
        idx += 10
        if rtype == 1 and rdlen == 4 and idx + 4 <= len(data):  # A record
            ip = socket.inet_ntoa(data[idx : idx + 4])
            # Ignorujeme známé redirect / block IP adresy
            if ip not in ("159.100.251.128", "127.0.0.1", "0.0.0.0"):
                ips.append(ip)
        idx += rdlen
    return ips


def resolve_domain(domain: str) -> list[str]:
    """Přeloží doménu pomocí veřejných DNS serverů s fallbacky a mezipamětí."""
    domain = domain.lower().strip()
    with _cache_lock:
        if domain in _cache and _cache[domain]:
            return _cache[domain]

    ips: list[str] = []

    # 1. Zkusíme dnspython, pokud je k dispozici
    try:
        import dns.resolver

        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = PUBLIC_DNS_SERVERS
        resolver.timeout = 2.0
        resolver.lifetime = 3.0
        answers = resolver.resolve(domain, "A")
        ips = [
            rdata.address
            for rdata in answers
            if rdata.address not in ("159.100.251.128", "127.0.0.1", "0.0.0.0")
        ]
    except Exception:
        pass

    # 2. Pokud dnspython selhal nebo není nainstalován, použijeme čistý UDP socket
    if not ips:
        for dns_ip in PUBLIC_DNS_SERVERS:
            try:
                found = _query_dns_udp(domain, dns_ip, timeout=1.5)
                if found:
                    ips = found
                    break
            except Exception:
                continue

    # 3. Statický fallback pro kritické Polymarket domény
    if not ips:
        ips = FALLBACK_IPS.get(domain, DEFAULT_FALLBACK_IPS)
        logger.warning(
            "DNS dotaz na veřejné servery selhal pro %s, použit statický fallback: %s",
            domain,
            ips,
        )

    with _cache_lock:
        _cache[domain] = ips

    return ips


def patched_getaddrinfo(
    host: Any,
    port: Any,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[Any]:
    """Hook pro socket.getaddrinfo obcházející DNS blokaci pro *.polymarket.com."""
    if isinstance(host, str) and (host.endswith(".polymarket.com") or host == "polymarket.com"):
        ips = resolve_domain(host)
        if ips:
            numeric_port = 443
            if isinstance(port, int):
                numeric_port = port
            elif isinstance(port, str):
                if port.isdigit():
                    numeric_port = int(port)
                elif port in ("https", "wss"):
                    numeric_port = 443
                elif port in ("http", "ws"):
                    numeric_port = 80
                else:
                    try:
                        numeric_port = socket.getservbyname(port)
                    except OSError:
                        numeric_port = 443

            socktype = type if type != 0 else socket.SOCK_STREAM
            sockproto = proto if proto != 0 else 6  # IPPROTO_TCP

            results = []
            for ip in ips:
                sockaddr = (ip, numeric_port)
                results.append((socket.AF_INET, socktype, sockproto, "", sockaddr))
            return results

    return _orig_getaddrinfo(host, port, family, type, proto, flags)


def setup_dns_patch() -> None:
    """Aktivuje DNS patch v modulu socket."""
    if getattr(socket, "_polymarket_dns_patched", False):
        return
    socket.getaddrinfo = patched_getaddrinfo
    socket._polymarket_dns_patched = True


# Automatická aktivace při importu modulu
setup_dns_patch()

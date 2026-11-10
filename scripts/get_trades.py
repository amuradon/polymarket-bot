#!/usr/bin/env python3
"""Polymarket Trades History Retrieval Script.

Tento skript načte a přehledně zobrazí historii obchodů z Polymarketu
za definované časové období (např. 2026-10-17 až 2026-10-19).

Použité proměnné prostředí (.env):
  - POLYMARKET_ADDRESS:     Adresa peněženky (Deposit Wallet / EOA)
  - POLYMARKET_SIGNER_KEY:   Privátní klíč signera (dříve POLYMARKET_PRIVATE_KEY)

Použití:
  python scripts/get_trades.py 2026-10-17 2026-10-19
  python scripts/get_trades.py --start 2026-10-10 --end 2026-10-12
  python scripts/get_trades.py --all
  python scripts/get_trades.py 2026-10-10 2026-10-12 --format csv --output trades.csv
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import datetime
import json
import logging
import os
import sys
from decimal import Decimal
from typing import Any

# Zajistíme transparentní DNS překlad pro Polymarket domény i při ISP / Whalebone blokaci
try:
    import dns_patch  # noqa: F401
except ImportError:
    from scripts import dns_patch  # noqa: F401

from dotenv import load_dotenv

# Polymarket unified SDK
try:
    from polymarket import PRODUCTION, PublicClient
    from polymarket._internal.environment import get_environment_config
    from polymarket._internal.wallet import derive_beacon_deposit_wallet_address
except ImportError as err:
    print(
        f"[FATAL] Chyba importu polymarket-client: {err}\n"
        "Nainstalujte závislosti pomocí: pip install -r scripts/requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# Zajistíme UTF-8 kódování standardního výstupu na Windows konzoli
if hasattr(sys.stdout, "reconfigure"):
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("polymarket-trades")


def parse_date_param(val: str, is_end: bool = False) -> datetime.datetime:
    """Převede textový řetězec data (YYYY-MM-DD nebo ISO formát) na datetime v UTC."""
    val = val.strip()
    dt: datetime.datetime | None = None

    # Zkusíme různé formáty data a času
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]

    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(val.rstrip("Z"), fmt.rstrip("Z"))
            break
        except ValueError:
            continue

    if dt is None:
        raise ValueError(
            f"Neplatný formát data '{val}'. Očekáván formát YYYY-MM-DD (např. 2026-10-17)."
        )

    # Pokud byl zadán pouze den bez času, nastavíme začátek dne (00:00:00) nebo konec dne (23:59:59)
    if len(val) == 10:
        if is_end:
            dt = dt.replace(hour=23, minute=59, second=59)
        else:
            dt = dt.replace(hour=0, minute=0, second=0)

    return dt.replace(tzinfo=datetime.timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Získání historie obchodů z Polymarket za definované období."
    )
    parser.add_argument(
        "dates",
        nargs="*",
        help="Počáteční a koncové datum (např. '2026-10-17 2026-10-19')",
    )
    parser.add_argument(
        "--start",
        "--from-date",
        dest="start_date",
        help="Počáteční datum období ve formátu YYYY-MM-DD (např. 2026-10-17)",
    )
    parser.add_argument(
        "--end",
        "--to-date",
        dest="end_date",
        help="Koncové datum období ve formátu YYYY-MM-DD (např. 2026-10-19)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Načte celou historii obchodů bez časového omezení",
    )
    parser.add_argument(
        "--address",
        help="Ruční přepsání adresy peněženky (jinak se načte z POLYMARKET_ADDRESS v .env)",
    )
    parser.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        help="Formát výstupu: 'table' (výchozí), 'csv' nebo 'json'",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Volitelná cesta k souboru pro uložení výsledků (např. trades.csv)",
    )
    return parser.parse_args()


def resolve_wallet_address(custom_address: str | None) -> str:
    """Získá adresu peněženky z parametru, .env nebo z privátního klíče."""
    if custom_address and custom_address.strip():
        return custom_address.strip()

    env_addr = os.getenv("POLYMARKET_ADDRESS")
    if env_addr and env_addr.strip():
        return env_addr.strip()

    signer_key = os.getenv("POLYMARKET_SIGNER_KEY") or os.getenv("POLYMARKET_PRIVATE_KEY")
    if signer_key and signer_key.strip():
        try:
            from eth_account import Account

            signer_acc = Account.from_key(signer_key.strip())
            cfg = get_environment_config(PRODUCTION)
            deposit_wallet = derive_beacon_deposit_wallet_address(
                signer_acc.address, cfg.wallet_derivation
            )
            logger.info(
                "POLYMARKET_ADDRESS nenalezena v .env; odvozena Deposit Wallet z klíče: %s",
                deposit_wallet,
            )
            return deposit_wallet
        except Exception as err:
            logger.debug("Nepodařilo se odvodit peněženku z klíče: %s", err)

    logger.error(
        "[FATAL] Není zadána žádná adresa peněženky!\n"
        "Nastavte POLYMARKET_ADDRESS v souboru .env nebo použijte parametr --address 0x..."
    )
    sys.exit(1)


def print_table(trades: list[Any], wallet: str, start_dt: Any, end_dt: Any) -> None:
    """Vytiskne přehlednou textovou tabulku s obchody a statistikou."""
    print("\n" + "=" * 120)
    print(" POLYMARKET HISTORIE OBCHODŮ")
    print("=" * 120)
    print(f" Peněženka: {wallet}")
    if start_dt and end_dt:
        print(f" Období:    {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  -->  {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        print(" Období:    Celá historie (--all)")
    print(f" Počet:     {len(trades)} obchodů")
    print("-" * 120)

    if not trades:
        print(" V zadaném období nebyly nalezeny žádné obchody.")
        print("=" * 120 + "\n")
        return

    # Hlavička tabulky
    header = (
        f"{'Čas (UTC)':<19} | {'Typ':<4} | {'Volba':<6} | "
        f"{'Počet shares':>12} | {'Cena':>8} | {'Hodnota USD':>12} | {'Trh'}"
    )
    print(header)
    print("-" * 120)

    total_shares = Decimal("0")
    total_volume_usd = Decimal("0")
    buy_count = 0
    sell_count = 0

    for t in trades:
        size = Decimal(str(t.size))
        price = Decimal(str(t.price))
        val_usd = size * price

        total_shares += size
        total_volume_usd += val_usd
        if (t.side or "").upper() == "BUY":
            buy_count += 1
        else:
            sell_count += 1

        ts_str = t.timestamp.strftime("%Y-%m-%d %H:%M:%S") if hasattr(t, "timestamp") else "-"
        side_str = (t.side or "-").upper()
        outcome_str = t.outcome or "-"
        title_str = (t.title or "Neznámý trh")
        if len(title_str) > 45:
            title_str = title_str[:42] + "..."

        row = (
            f"{ts_str:<19} | {side_str:<4} | {outcome_str:<6} | "
            f"{float(size):>12.4f} | ${float(price):>7.3f} | ${float(val_usd):>11.2f} | {title_str}"
        )
        print(row)

    print("-" * 120)
    print(
        f" Souhrn: Obchodů: {len(trades)} (Nákupy: {buy_count}, Prodeje: {sell_count}) | "
        f"Celkem shares: {float(total_shares):.4f} | Celkový objem: ${float(total_volume_usd):.2f}"
    )
    print("=" * 120 + "\n")


def export_csv(trades: list[Any], file_path: str) -> None:
    """Exportuje obchody do formátu CSV."""
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_utc",
            "side",
            "outcome",
            "size_shares",
            "price_usd",
            "total_usd",
            "market_title",
            "slug",
            "condition_id",
            "asset_id",
            "transaction_hash",
        ])
        for t in trades:
            size = Decimal(str(t.size))
            price = Decimal(str(t.price))
            val_usd = size * price
            writer.writerow([
                t.timestamp.isoformat() if hasattr(t, "timestamp") else "",
                t.side,
                t.outcome,
                str(size),
                str(price),
                str(val_usd),
                t.title,
                t.slug,
                t.condition_id,
                t.asset_id,
                t.transaction_hash,
            ])
    logger.info("Výsledky úspěšně uloženy do CSV: %s", file_path)


def export_json(trades: list[Any], file_path: str | None = None) -> None:
    """Exportuje obchody do JSON struktury."""
    data = []
    for t in trades:
        size = Decimal(str(t.size))
        price = Decimal(str(t.price))
        data.append({
            "timestamp": t.timestamp.isoformat() if hasattr(t, "timestamp") else None,
            "side": t.side,
            "outcome": t.outcome,
            "size": float(size),
            "price": float(price),
            "total_usd": float(size * price),
            "market_title": t.title,
            "slug": t.slug,
            "condition_id": t.condition_id,
            "asset_id": t.asset_id,
            "transaction_hash": t.transaction_hash,
        })

    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    if file_path:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        logger.info("Výsledky úspěšně uloženy do JSON: %s", file_path)
    else:
        print(json_str)


def main() -> None:
    load_dotenv()
    args = parse_args()

    # 1. Zjištění adresy peněženky
    wallet_address = resolve_wallet_address(args.address)

    # 2. Vyhodnocení časového období
    start_dt: datetime.datetime | None = None
    end_dt: datetime.datetime | None = None

    if not args.all:
        raw_start = args.start_date
        raw_end = args.end_date

        # Podpora pozičních argumentů např. python get_trades.py 2026-10-17 2026-10-19
        if not raw_start and len(args.dates) >= 1:
            raw_start = args.dates[0]
        if not raw_end and len(args.dates) >= 2:
            raw_end = args.dates[1]

        if not raw_start or not raw_end:
            logger.error(
                "[CHYBA] Zadejte počáteční a koncové datum (nebo použijte přepínač --all).\n"
                "Příklad použití:\n"
                "  python scripts/get_trades.py 2026-10-17 2026-10-19\n"
                "  python scripts/get_trades.py --start 2026-10-10 --end 2026-10-12\n"
                "  python scripts/get_trades.py --all"
            )
            sys.exit(1)

        try:
            start_dt = parse_date_param(raw_start, is_end=False)
            end_dt = parse_date_param(raw_end, is_end=True)
        except ValueError as err:
            logger.error("%s", err)
            sys.exit(1)

        if start_dt > end_dt:
            logger.error("Počáteční datum (%s) nesmí být novější než koncové datum (%s)!", raw_start, raw_end)
            sys.exit(1)

    # 3. Načtení obchodů přes Polymarket PublicClient
    client = PublicClient()
    logger.info("Dotazuji Polymarket Data API pro peněženku %s...", wallet_address)

    try:
        if args.all:
            paginator = client.list_trades(user=wallet_address, full_history=True)
        else:
            paginator = client.list_trades(user=wallet_address, start=start_dt, end=end_dt)

        trades: list[Any] = list(paginator.iter_items())
    except Exception as exc:
        logger.error("Chyba při stahování obchodů: %s", exc)
        sys.exit(1)

    # Seřadíme obchody chronologicky (nejnovější nahoře nebo nejstarší první)
    trades.sort(key=lambda t: t.timestamp if hasattr(t, "timestamp") else datetime.datetime.min, reverse=True)

    # 4. Formátování a výstup
    if args.format == "csv":
        out_file = args.output or "trades.csv"
        export_csv(trades, out_file)
    elif args.format == "json":
        export_json(trades, args.output)
    else:
        print_table(trades, wallet_address, start_dt, end_dt)
        if args.output:
            if args.output.endswith(".csv"):
                export_csv(trades, args.output)
            elif args.output.endswith(".json"):
                export_json(trades, args.output)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Polymarket Activity History Retrieval Script.

Tento skript načte a přehledně zobrazí historii aktivit z Polymarketu
(obchody, uplatnění zisků/redeem, vklady, výběry, splity, merge, odměny atd.)
za definované časové období nebo za celou historii.

Použité proměnné prostředí (.env):
  - POLYMARKET_ADDRESS:     Adresa peněženky (Deposit Wallet / EOA)
  - POLYMARKET_SIGNER_KEY:   Privátní klíč signera (dříve POLYMARKET_PRIVATE_KEY)

Použití:
  python scripts/get_activity.py 2026-10-17 2026-10-19
  python scripts/get_activity.py --start 2026-10-10 --end 2026-10-12
  python scripts/get_activity.py --all
  python scripts/get_activity.py --all --type REDEEM
  python scripts/get_activity.py --all --type TRADE --side BUY
  python scripts/get_activity.py 2026-10-10 2026-10-12 --format csv --output activity.csv
  python scripts/get_activity.py --all --format json --output activity.json
"""

from __future__ import annotations

import argparse
import collections
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
logger = logging.getLogger("polymarket-activity")


def parse_date_param(val: str, is_end: bool = False) -> datetime.datetime:
    """Převede textový řetězec data (YYYY-MM-DD nebo ISO formát) na datetime v UTC."""
    val = val.strip()
    dt: datetime.datetime | None = None

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

    if len(val) == 10:
        if is_end:
            dt = dt.replace(hour=23, minute=59, second=59)
        else:
            dt = dt.replace(hour=0, minute=0, second=0)

    return dt.replace(tzinfo=datetime.timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Získání historie aktivit z Polymarket (obchody, redeemy, vklady, výběry)."
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
        help="Načte celou historii aktivit bez časového omezení",
    )
    parser.add_argument(
        "--type",
        "--activity-type",
        dest="activity_type",
        help="Filtrovat podle typu aktivity (např. TRADE, REDEEM, DEPOSIT, WITHDRAWAL, SPLIT, MERGE, REWARD, YIELD)",
    )
    parser.add_argument(
        "--side",
        choices=["BUY", "SELL", "buy", "sell"],
        help="Filtrovat obchody podle strany (BUY / SELL)",
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
        help="Volitelná cesta k souboru pro uložení výsledků (např. activity.csv)",
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


def get_activity_type_str(act: Any) -> str:
    """Vrátí textový název typu aktivity (např. TRADE, REDEEM, DEPOSIT)."""
    raw_type = getattr(act, "type", None)
    if hasattr(raw_type, "value"):
        return str(raw_type.value).upper()
    if raw_type is not None:
        return str(raw_type).upper()
    cls_name = type(act).__name__
    if cls_name.endswith("Activity"):
        return cls_name[:-8].upper()
    return cls_name.upper()


def get_activity_title(act: Any) -> str:
    """Vrátí lidsky čitelný název trhu nebo popis aktivity."""
    title = getattr(act, "title", None)
    if title and title.strip():
        return title.strip()

    act_type = get_activity_type_str(act)
    if act_type == "DEPOSIT":
        return "Vklad USDC na peněženku"
    if act_type == "WITHDRAWAL":
        return "Výběr USDC z peněženky"
    if act_type in ("MAKER_REBATE", "TAKER_REBATE"):
        return "Polymarket poplatkový rebate"
    if act_type == "REWARD":
        return "Polymarket odměna"
    if act_type == "YIELD":
        return "Úrokový výnos (Yield)"
    if act_type == "REFERRAL_REWARD":
        return "Doporučitelská odměna"
    if act_type == "TIP":
        return "Spropitné (Tip)"
    return f"Aktivita {act_type}"


def format_tx_hash(tx_hash: str | None) -> str:
    """Zkrátí transakční hash na přehledný formát 0x1234...5678."""
    if not tx_hash:
        return "-"
    tx_str = str(tx_hash).strip()
    if len(tx_str) > 13:
        return f"{tx_str[:6]}...{tx_str[-4:]}"
    return tx_str


def print_table(activities: list[Any], wallet: str, start_dt: Any, end_dt: Any) -> None:
    """Vytiskne přehlednou textovou tabulku s aktivitami a souhrnnou statistikou."""
    table_width = 148
    print("\n" + "=" * table_width)
    print(" POLYMARKET HISTORIE AKTIVIT")
    print("=" * table_width)
    print(f" Peněženka: {wallet}")
    if start_dt and end_dt:
        print(f" Období:    {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  -->  {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        print(" Období:    Celá historie (--all)")
    print(f" Počet:     {len(activities)} aktivit")
    print("-" * table_width)

    if not activities:
        print(" V zadaném období nebyly nalezeny žádné aktivity.")
        print("=" * table_width + "\n")
        return

    # Hlavička tabulky
    header = (
        f"{'Čas (UTC)':<19} | {'Typ aktivity':<12} | {'Strana':<6} | {'Volba':<6} | "
        f"{'Shares':>11} | {'Cena':>8} | {'Částka USD':>12} | {'Trh / Popis':<40} | {'Tx Hash':<14}"
    )
    print(header)
    print("-" * table_width)

    type_counts: dict[str, int] = collections.defaultdict(int)
    total_amount_by_type: dict[str, Decimal] = collections.defaultdict(Decimal)
    buy_count = 0
    sell_count = 0

    for act in activities:
        act_type = get_activity_type_str(act)
        type_counts[act_type] += 1

        raw_side = getattr(act, "side", None)
        side_str = str(raw_side).upper() if raw_side else "-"
        if side_str == "BUY":
            buy_count += 1
        elif side_str == "SELL":
            sell_count += 1

        outcome_str = getattr(act, "outcome", None) or "-"
        raw_shares = getattr(act, "shares", None)
        shares_str = f"{float(raw_shares):>11.4f}" if raw_shares is not None else f"{'-':>11}"

        raw_price = getattr(act, "price", None)
        price_str = f"${float(raw_price):>7.3f}" if raw_price is not None else f"{'-':>8}"

        raw_amount = getattr(act, "amount", None)
        if raw_amount is not None:
            dec_amount = Decimal(str(raw_amount))
            total_amount_by_type[act_type] += dec_amount
            amount_str = f"${float(dec_amount):>11.2f}"
        else:
            amount_str = f"{'-':>12}"

        ts = getattr(act, "timestamp", None)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, (datetime.datetime, datetime.date)) else "-"

        title_str = get_activity_title(act)
        if len(title_str) > 40:
            title_str = title_str[:37] + "..."

        tx_str = format_tx_hash(getattr(act, "transaction_hash", None))

        row = (
            f"{ts_str:<19} | {act_type:<12} | {side_str:<6} | {outcome_str:<6} | "
            f"{shares_str} | {price_str} | {amount_str} | {title_str:<40} | {tx_str:<14}"
        )
        print(row)

    print("-" * table_width)

    # Přehledný souhrn statistik
    summary_parts = [f"Celkem aktivit: {len(activities)}"]
    if "TRADE" in type_counts:
        summary_parts.append(
            f"Obchody: {type_counts['TRADE']} (Nákupy: {buy_count}, Prodeje: {sell_count}, Objem: ${float(total_amount_by_type['TRADE']):.2f})"
        )
    if "REDEEM" in type_counts:
        summary_parts.append(
            f"Uplatněno (Redeem): {type_counts['REDEEM']} (${float(total_amount_by_type['REDEEM']):.2f})"
        )
    if "DEPOSIT" in type_counts:
        summary_parts.append(
            f"Vklady: {type_counts['DEPOSIT']} (${float(total_amount_by_type['DEPOSIT']):.2f})"
        )
    if "WITHDRAWAL" in type_counts:
        summary_parts.append(
            f"Výběry: {type_counts['WITHDRAWAL']} (${float(total_amount_by_type['WITHDRAWAL']):.2f})"
        )

    other_types = [
        f"{t}: {cnt}" for t, cnt in type_counts.items()
        if t not in ("TRADE", "REDEEM", "DEPOSIT", "WITHDRAWAL")
    ]
    if other_types:
        summary_parts.append(f"Ostatní: {', '.join(other_types)}")

    print(" Souhrn: " + " | ".join(summary_parts))
    print("=" * table_width + "\n")


def export_csv(activities: list[Any], file_path: str) -> None:
    """Exportuje aktivity do formátu CSV."""
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_utc",
            "activity_type",
            "side",
            "outcome",
            "shares",
            "price_usd",
            "amount_usd",
            "market_title",
            "slug",
            "condition_id",
            "asset_id",
            "transaction_hash",
            "wallet",
        ])
        for act in activities:
            ts = getattr(act, "timestamp", None)
            ts_str = ts.isoformat() if hasattr(ts, "isoformat") else ""
            raw_shares = getattr(act, "shares", None)
            raw_price = getattr(act, "price", None)
            raw_amount = getattr(act, "amount", None)

            writer.writerow([
                ts_str,
                get_activity_type_str(act),
                getattr(act, "side", "") or "",
                getattr(act, "outcome", "") or "",
                str(raw_shares) if raw_shares is not None else "",
                str(raw_price) if raw_price is not None else "",
                str(raw_amount) if raw_amount is not None else "",
                get_activity_title(act),
                getattr(act, "slug", "") or "",
                getattr(act, "condition_id", "") or "",
                getattr(act, "asset_id", "") or "",
                getattr(act, "transaction_hash", "") or "",
                getattr(act, "wallet", "") or "",
            ])
    logger.info("Výsledky úspěšně uloženy do CSV: %s", file_path)


def export_json(activities: list[Any], file_path: str | None = None) -> None:
    """Exportuje aktivity do JSON struktury."""
    data = []
    for act in activities:
        ts = getattr(act, "timestamp", None)
        raw_shares = getattr(act, "shares", None)
        raw_price = getattr(act, "price", None)
        raw_amount = getattr(act, "amount", None)

        data.append({
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else None,
            "activity_type": get_activity_type_str(act),
            "side": getattr(act, "side", None),
            "outcome": getattr(act, "outcome", None),
            "shares": float(raw_shares) if raw_shares is not None else None,
            "price": float(raw_price) if raw_price is not None else None,
            "amount_usd": float(raw_amount) if raw_amount is not None else None,
            "market_title": get_activity_title(act),
            "slug": getattr(act, "slug", None),
            "condition_id": getattr(act, "condition_id", None),
            "asset_id": getattr(act, "asset_id", None),
            "transaction_hash": getattr(act, "transaction_hash", None),
            "wallet": getattr(act, "wallet", None),
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

        # Podpora pozičních argumentů např. python get_activity.py 2026-10-17 2026-10-19
        if not raw_start and len(args.dates) >= 1:
            raw_start = args.dates[0]
        if not raw_end and len(args.dates) >= 2:
            raw_end = args.dates[1]

        if not raw_start or not raw_end:
            logger.error(
                "[CHYBA] Zadejte počáteční a koncové datum (nebo použijte přepínač --all).\n"
                "Příklad použití:\n"
                "  python scripts/get_activity.py 2026-10-17 2026-10-19\n"
                "  python scripts/get_activity.py --start 2026-10-10 --end 2026-10-12\n"
                "  python scripts/get_activity.py --all\n"
                "  python scripts/get_activity.py --all --type REDEEM\n"
                "  python scripts/get_activity.py --all --type TRADE --side BUY"
            )
            sys.exit(1)

        try:
            start_dt = parse_date_param(raw_start, is_end=False)
            end_dt = parse_date_param(raw_end, is_end=True)
        except ValueError as err:
            logger.error("%s", err)
            sys.exit(1)

        if start_dt > end_dt:
            logger.error(
                "Počáteční datum (%s) nesmí být novější než koncové datum (%s)!",
                raw_start,
                raw_end,
            )
            sys.exit(1)

    # 3. Příprava filtrů
    activity_types = [args.activity_type.strip().upper()] if args.activity_type else None
    side_filter = args.side.strip().upper() if args.side else None

    # 4. Načtení aktivit přes Polymarket PublicClient
    client = PublicClient()
    logger.info("Dotazuji Polymarket Data API pro aktivity peněženky %s...", wallet_address)

    try:
        kwargs: dict[str, Any] = {
            "user": wallet_address,
            "activity_types": activity_types,
            "side": side_filter,
        }
        if args.all:
            kwargs["full_history"] = True
        else:
            kwargs["start"] = start_dt
            kwargs["end"] = end_dt

        paginator = client.list_activity(**kwargs)
        activities: list[Any] = list(paginator.iter_items())
    except Exception as exc:
        logger.error("Chyba při stahování aktivit: %s", exc)
        sys.exit(1)

    # Seřadíme chronologicky (nejnovější nahoře)
    activities.sort(
        key=lambda a: getattr(a, "timestamp", None)
        or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        reverse=True,
    )

    # 5. Formátování a výstup
    if args.format == "csv":
        out_file = args.output or "activity.csv"
        export_csv(activities, out_file)
    elif args.format == "json":
        export_json(activities, args.output)
    else:
        print_table(activities, wallet_address, start_dt, end_dt)
        if args.output:
            if args.output.endswith(".csv"):
                export_csv(activities, args.output)
            elif args.output.endswith(".json"):
                export_json(activities, args.output)


if __name__ == "__main__":
    main()

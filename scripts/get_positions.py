#!/usr/bin/env python3
"""Polymarket Positions Retrieval Script.

Tento skript načte a přehledně zobrazí portfolio pozic z Polymarketu
(otevřené pozice, pozice k uplatnění/redeemable a uzavřené pozice)
včetně detailních statistik PnL, nákupních a aktuálních cen.

Použité proměnné prostředí (.env):
  - POLYMARKET_ADDRESS:     Adresa peněženky (Deposit Wallet / EOA)
  - POLYMARKET_SIGNER_KEY:   Privátní klíč signera (dříve POLYMARKET_PRIVATE_KEY)

Použití:
  python scripts/get_positions.py                     # Zobrazí aktuální otevřené/redeemable pozice
  python scripts/get_positions.py --status closed     # Zobrazí uzavřené pozice
  python scripts/get_positions.py --status all        # Zobrazí otevřené i uzavřené pozice
  python scripts/get_positions.py --all               # Zobrazí celou historii pozic
  py get_positions.py 2026-11-10 2026-11-10 --status all
  python scripts/get_positions.py --sort-by pnl
  python scripts/get_positions.py --format csv --output positions.csv
  python scripts/get_positions.py --status all --format json --output positions.json
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
logger = logging.getLogger("polymarket-positions")


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
        description="Získání přehledu pozic a portfolia z Polymarket."
    )
    parser.add_argument(
        "dates",
        nargs="*",
        help="Volitelné počáteční a koncové datum pro filtrování (např. '2026-08-20 2026-09-10')",
    )
    parser.add_argument(
        "--start",
        "--from-date",
        dest="start_date",
        help="Počáteční datum období ve formátu YYYY-MM-DD (např. 2026-08-20)",
    )
    parser.add_argument(
        "--end",
        "--to-date",
        dest="end_date",
        help="Koncové datum období ve formátu YYYY-MM-DD (např. 2026-09-10)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Načte celou historii pozic bez časového omezení",
    )
    parser.add_argument(
        "--status",
        choices=["open", "closed", "redeemable", "all"],
        default="open",
        help="Stav pozic: 'open' (výchozí: otevřené i redeemable), 'closed' (uzavřené), 'redeemable', 'all' (otevřené i uzavřené)",
    )
    parser.add_argument(
        "--sort-by",
        choices=["value", "pnl", "size", "price", "date"],
        default="value",
        help="Kritérium řazení: 'value' (hodnota, výchozí), 'pnl' (zisk/ztráta), 'size' (velikost shares), 'price' (cena), 'date' (datum)",
    )
    parser.add_argument(
        "--asc",
        action="store_true",
        help="Vzestupné řazení (výchozí je sestupné)",
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
        help="Volitelná cesta k souboru pro uložení výsledků (např. positions.csv)",
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


def get_position_status_str(pos: Any) -> str:
    """Vrátí textový název stavu pozice."""
    raw_status = getattr(pos, "status", None)
    if hasattr(raw_status, "value"):
        return str(raw_status.value).upper()
    if raw_status is not None:
        return str(raw_status).upper()
    return "UNKNOWN"


def get_position_effective_size(pos: Any) -> Decimal:
    """Vrátí relevantní velikost pozice (current_size pro otevřené, total_size pro uzavřené)."""
    curr = getattr(pos, "current_size", None)
    if curr is not None and Decimal(str(curr)) > 0:
        return Decimal(str(curr))
    tot = getattr(pos, "total_size", None)
    if tot is not None:
        return Decimal(str(tot))
    return Decimal("0")


def get_position_percent_pnl(pos: Any) -> Decimal:
    """Vrátí procentuální PnL z pozice."""
    p_pnl = getattr(pos, "percent_pnl", None)
    if p_pnl is not None and Decimal(str(p_pnl)) != 0:
        return Decimal(str(p_pnl))
    r_pnl = getattr(pos, "percent_realized_pnl", None)
    if r_pnl is not None:
        return Decimal(str(r_pnl))
    return Decimal("0")


def sort_positions(positions: list[Any], sort_by: str, ascending: bool) -> list[Any]:
    """Seřadí pozice podle zadaného kritéria."""
    def sort_key(p: Any) -> Any:
        if sort_by == "pnl":
            val = getattr(p, "total_pnl", None)
            return Decimal(str(val)) if val is not None else Decimal("-999999999")
        if sort_by == "size":
            return get_position_effective_size(p)
        if sort_by == "price":
            val = getattr(p, "avg_price", None)
            return Decimal(str(val)) if val is not None else Decimal("0")
        if sort_by == "date":
            dt = (
                getattr(p, "last_event_at", None)
                or getattr(p, "first_entry_at", None)
                or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
            )
            return dt
        # Výchozí je "value"
        val = getattr(p, "current_value", None)
        return Decimal(str(val)) if val is not None else Decimal("0")

    return sorted(positions, key=sort_key, reverse=not ascending)


def print_table(
    positions: list[Any],
    wallet: str,
    status_filter: str,
    start_dt: Any,
    end_dt: Any,
) -> None:
    """Vytiskne přehlednou textovou tabulku s pozicemi a souhrnnou statistikou."""
    table_width = 139
    print("\n" + "=" * table_width)
    print(" POLYMARKET PŘEHLED POZIC")
    print("=" * table_width)
    print(f" Peněženka: {wallet}")
    print(f" Filtr:     Stav: {status_filter.upper()}")
    if start_dt and end_dt:
        print(f" Období:    {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  -->  {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    else:
        print(" Období:    Všechna data / bez časového omezení")
    print(f" Počet:     {len(positions)} pozic")
    print("-" * table_width)

    if not positions:
        print(" V zadaném filtru nebyly nalezeny žádné pozice.")
        print("=" * table_width + "\n")
        return

    # Hlavička tabulky
    header = (
        f"{'Trh':<38} | {'Volba':<6} | {'Status':<10} | {'Shares':>12} | "
        f"{'Nákup':>8} | {'Aktuální':>8} | {'Hodnota USD':>12} | {'Celkový PnL ($)':>15} | {'PnL (%)':>10}"
    )
    print(header)
    print("-" * table_width)

    status_counts: dict[str, int] = collections.defaultdict(int)
    total_cur_value = Decimal("0")
    total_cost = Decimal("0")
    total_unrealized_pnl = Decimal("0")
    total_realized_pnl = Decimal("0")
    total_pnl = Decimal("0")

    for p in positions:
        st_str = get_position_status_str(p)
        status_counts[st_str] += 1

        title_str = getattr(p, "title", None) or "Neznámý trh"
        if len(title_str) > 38:
            title_str = title_str[:35] + "..."

        outcome_str = getattr(p, "outcome", None) or "-"
        effective_shares = get_position_effective_size(p)

        avg_price = getattr(p, "avg_price", None)
        avg_str = f"${float(avg_price):>7.3f}" if avg_price is not None else f"{'-':>8}"

        cur_price = getattr(p, "current_price", None)
        cur_str = f"${float(cur_price):>7.3f}" if cur_price is not None else f"{'-':>8}"

        cur_val = getattr(p, "current_value", None)
        if cur_val is not None:
            dec_cur_val = Decimal(str(cur_val))
            total_cur_value += dec_cur_val
            val_str = f"${float(dec_cur_val):>11.2f}"
        else:
            val_str = f"{'-':>12}"

        t_cost = getattr(p, "total_cost_usdc", None) or getattr(p, "entry_cost_usdc", None)
        if t_cost is not None:
            total_cost += Decimal(str(t_cost))

        u_pnl = getattr(p, "unrealized_pnl", None)
        if u_pnl is not None:
            total_unrealized_pnl += Decimal(str(u_pnl))

        r_pnl = getattr(p, "realized_pnl", None)
        if r_pnl is not None:
            total_realized_pnl += Decimal(str(r_pnl))

        tot_p = getattr(p, "total_pnl", None)
        if tot_p is not None:
            dec_tot_p = Decimal(str(tot_p))
            total_pnl += dec_tot_p
            pnl_float = float(dec_tot_p)
            pnl_str = f"{'+$' if pnl_float >= 0 else '-$'}{abs(pnl_float):>13.2f}"
        else:
            pnl_str = f"{'-':>15}"

        pct_val = get_position_percent_pnl(p)
        pct_float = float(pct_val)
        pct_str = f"{'+' if pct_float > 0 else ''}{pct_float:>8.1f}%"

        row = (
            f"{title_str:<38} | {outcome_str:<6} | {st_str:<10} | {float(effective_shares):>12.4f} | "
            f"{avg_str} | {cur_str} | {val_str} | {pnl_str} | {pct_str:>10}"
        )
        print(row)

    print("-" * table_width)

    # Souhrn
    status_summary_parts = [f"{st}: {cnt}" for st, cnt in sorted(status_counts.items())]
    pnl_total_float = float(total_pnl)
    pnl_total_str = f"{'+$' if pnl_total_float >= 0 else '-$'}{abs(pnl_total_float):.2f}"

    print(
        f" Souhrn: Celkem pozic: {len(positions)} ({', '.join(status_summary_parts)}) | "
        f"Hodnota portfolia: ${float(total_cur_value):.2f} | Náklad: ${float(total_cost):.2f}"
    )
    print(
        f" PnL:    Nerealizovaný: ${float(total_unrealized_pnl):.2f} | "
        f"Realizovaný: ${float(total_realized_pnl):.2f} | Celkový PnL: {pnl_total_str}"
    )
    print("=" * table_width + "\n")


def export_csv(positions: list[Any], file_path: str) -> None:
    """Exportuje pozice do formátu CSV."""
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "wallet",
            "condition_id",
            "asset_id",
            "market_title",
            "slug",
            "outcome",
            "status",
            "current_size",
            "total_size",
            "avg_price",
            "current_price",
            "current_value_usd",
            "entry_cost_usdc",
            "total_cost_usdc",
            "realized_pnl_usd",
            "unrealized_pnl_usd",
            "total_pnl_usd",
            "percent_pnl",
            "redeemable",
            "first_entry_at",
            "last_event_at",
            "end_date",
        ])
        for p in positions:
            first_entry = getattr(p, "first_entry_at", None)
            last_event = getattr(p, "last_event_at", None)
            end_d = getattr(p, "end_date", None)

            writer.writerow([
                getattr(p, "wallet", "") or "",
                getattr(p, "condition_id", "") or "",
                getattr(p, "asset_id", "") or "",
                getattr(p, "title", "") or "",
                getattr(p, "slug", "") or "",
                getattr(p, "outcome", "") or "",
                get_position_status_str(p),
                str(getattr(p, "current_size", "") or ""),
                str(getattr(p, "total_size", "") or ""),
                str(getattr(p, "avg_price", "") or ""),
                str(getattr(p, "current_price", "") or ""),
                str(getattr(p, "current_value", "") or ""),
                str(getattr(p, "entry_cost_usdc", "") or ""),
                str(getattr(p, "total_cost_usdc", "") or ""),
                str(getattr(p, "realized_pnl", "") or ""),
                str(getattr(p, "unrealized_pnl", "") or ""),
                str(getattr(p, "total_pnl", "") or ""),
                str(get_position_percent_pnl(p)),
                str(bool(getattr(p, "redeemable", False))),
                first_entry.isoformat() if hasattr(first_entry, "isoformat") else "",
                last_event.isoformat() if hasattr(last_event, "isoformat") else "",
                end_d.isoformat() if hasattr(end_d, "isoformat") else "",
            ])
    logger.info("Výsledky úspěšně uloženy do CSV: %s", file_path)


def export_json(positions: list[Any], file_path: str | None = None) -> None:
    """Exportuje pozice do JSON struktury."""
    data = []
    for p in positions:
        first_entry = getattr(p, "first_entry_at", None)
        last_event = getattr(p, "last_event_at", None)
        end_d = getattr(p, "end_date", None)

        cur_sz = getattr(p, "current_size", None)
        tot_sz = getattr(p, "total_size", None)
        avg_pr = getattr(p, "avg_price", None)
        cur_pr = getattr(p, "current_price", None)
        cur_val = getattr(p, "current_value", None)
        ent_cost = getattr(p, "entry_cost_usdc", None)
        tot_cost = getattr(p, "total_cost_usdc", None)
        r_pnl = getattr(p, "realized_pnl", None)
        u_pnl = getattr(p, "unrealized_pnl", None)
        tot_pnl = getattr(p, "total_pnl", None)

        data.append({
            "wallet": getattr(p, "wallet", None),
            "condition_id": getattr(p, "condition_id", None),
            "asset_id": getattr(p, "asset_id", None),
            "market_title": getattr(p, "title", None),
            "slug": getattr(p, "slug", None),
            "outcome": getattr(p, "outcome", None),
            "status": get_position_status_str(p),
            "current_size": float(cur_sz) if cur_sz is not None else None,
            "total_size": float(tot_sz) if tot_sz is not None else None,
            "avg_price": float(avg_pr) if avg_pr is not None else None,
            "current_price": float(cur_pr) if cur_pr is not None else None,
            "current_value_usd": float(cur_val) if cur_val is not None else None,
            "entry_cost_usdc": float(ent_cost) if ent_cost is not None else None,
            "total_cost_usdc": float(tot_cost) if tot_cost is not None else None,
            "realized_pnl_usd": float(r_pnl) if r_pnl is not None else None,
            "unrealized_pnl_usd": float(u_pnl) if u_pnl is not None else None,
            "total_pnl_usd": float(tot_pnl) if tot_pnl is not None else None,
            "percent_pnl": float(get_position_percent_pnl(p)),
            "redeemable": bool(getattr(p, "redeemable", False)),
            "first_entry_at": first_entry.isoformat() if hasattr(first_entry, "isoformat") else None,
            "last_event_at": last_event.isoformat() if hasattr(last_event, "isoformat") else None,
            "end_date": end_d.isoformat() if hasattr(end_d, "isoformat") else None,
        })

    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    if file_path:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        logger.info("Výsledky úspěšně uloženy do JSON: %s", file_path)
    else:
        print(json_str)


def fetch_positions_by_status(
    client: PublicClient,
    wallet: str,
    status_filter: str,
    start_dt: datetime.datetime | None,
    end_dt: datetime.datetime | None,
    full_history: bool,
) -> list[Any]:
    """Načte pozice podle zvoleného stavu (open, closed, redeemable, all)."""
    base_kwargs: dict[str, Any] = {"user": wallet}
    if start_dt and end_dt:
        base_kwargs["start"] = start_dt
        base_kwargs["end"] = end_dt
    elif full_history:
        base_kwargs["full_history"] = True

    if status_filter == "all":
        # Pro "all" načteme otevřené i uzavřené pozice a sloučíme
        open_items = list(client.list_positions(**base_kwargs, status="OPEN").iter_items())
        closed_items = list(client.list_positions(**base_kwargs, status="CLOSED").iter_items())

        combined_dict: dict[str, Any] = {}
        for p in open_items + closed_items:
            key = getattr(p, "asset_id", None) or f"{getattr(p, 'condition_id', '')}_{getattr(p, 'outcome', '')}"
            combined_dict[key] = p
        return list(combined_dict.values())

    if status_filter == "closed":
        return list(client.list_positions(**base_kwargs, status="CLOSED").iter_items())

    if status_filter == "redeemable":
        return list(client.list_positions(**base_kwargs, status="REDEEMABLE").iter_items())

    # Výchozí je "open"
    return list(client.list_positions(**base_kwargs, status="OPEN").iter_items())


def main() -> None:
    load_dotenv()
    args = parse_args()

    # 1. Zjištění adresy peněženky
    wallet_address = resolve_wallet_address(args.address)

    # 2. Vyhodnocení časového období (volitelné)
    start_dt: datetime.datetime | None = None
    end_dt: datetime.datetime | None = None

    raw_start = args.start_date
    raw_end = args.end_date

    if not raw_start and len(args.dates) >= 1:
        raw_start = args.dates[0]
    if not raw_end and len(args.dates) >= 2:
        raw_end = args.dates[1]

    if raw_start or raw_end:
        if not raw_start or not raw_end:
            logger.error(
                "[CHYBA] Pokud zadáváte časové období, zadejte jak počáteční, tak koncové datum.\n"
                "Příklad použití:\n"
                "  python scripts/get_positions.py 2026-08-20 2026-09-10\n"
                "  python scripts/get_positions.py --start 2026-08-20 --end 2026-09-10"
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

    # 3. Načtení pozic přes Polymarket PublicClient
    client = PublicClient()
    logger.info("Dotazuji Polymarket Data API pro pozice peněženky %s...", wallet_address)

    try:
        positions = fetch_positions_by_status(
            client=client,
            wallet=wallet_address,
            status_filter=args.status,
            start_dt=start_dt,
            end_dt=end_dt,
            full_history=args.all,
        )
    except Exception as exc:
        logger.error("Chyba při stahování pozic: %s", exc)
        sys.exit(1)

    # 4. Řazení pozic
    positions = sort_positions(positions, sort_by=args.sort_by, ascending=args.asc)

    # 5. Formátování a výstup
    if args.format == "csv":
        out_file = args.output or "positions.csv"
        export_csv(positions, out_file)
    elif args.format == "json":
        export_json(positions, args.output)
    else:
        print_table(positions, wallet_address, args.status, start_dt, end_dt)
        if args.output:
            if args.output.endswith(".csv"):
                export_csv(positions, args.output)
            elif args.output.endswith(".json"):
                export_json(positions, args.output)


if __name__ == "__main__":
    main()

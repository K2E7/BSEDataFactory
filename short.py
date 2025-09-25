#!/usr/bin/env python3
"""
Download FINRA OTC biweekly CSVs.

Pattern:
  https://cdn.finra.org/equity/otcmarket/biweekly/shrt<YYYYMMDD>.csv

Rules:
- Target days per month: 15th and the last day of month.
- If target day is Sat/Sun, roll back to previous Friday.
- Dates formatted as YYYYMMDD, with filename 'shrtYYYYMMDD.csv'.
- Files saved into ./downloads/
"""

from __future__ import annotations

import argparse
import calendar
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Iterable, Tuple, List, Dict

import requests

BASE_URL = "https://cdn.finra.org/equity/otcmarket/biweekly/"  # fixed base
PREFIX = "shrt"                                                # file prefix
EXT = ".csv"                                                   # file extension


# --------------------------- Date helpers ---------------------------

def parse_year_month(ym: str) -> date:
    """Parse 'YYYY-MM' to a date at the first of that month."""
    return datetime.strptime(ym, "%Y-%m").date().replace(day=1)


def iter_year_months(start: date, end: date) -> Iterable[Tuple[int, int]]:
    """Yield (year, month) inclusive from start..end (both at day=1)."""
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def adjust_to_prev_business_day(d: date) -> date:
    """If d is Sat(5)/Sun(6), step back to previous Friday."""
    wd = d.weekday()
    if wd == 5:
        return d - timedelta(days=1)
    if wd == 6:
        return d - timedelta(days=2)
    return d


def target_dates_for_month(year: int, month: int) -> Tuple[date, date]:
    """Return (mid, eom) with weekend rollback applied."""
    mid = adjust_to_prev_business_day(date(year, month, 15))
    last_day = calendar.monthrange(year, month)[1]
    eom = adjust_to_prev_business_day(date(year, month, last_day))
    return mid, eom


def ymd_compact(d: date) -> str:
    return f"{d.year}{d.month:02d}{d.day:02d}"


def build_file_dates(start: date, end: date) -> List[date]:
    """Collect all target dates from start..end months."""
    dates: List[date] = []
    for y, m in iter_year_months(start, end):
        mid, eom = target_dates_for_month(y, m)
        dates += [mid, eom]
    # De-dup while preserving order
    seen, out = set(), []
    for d in dates:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


# --------------------------- I/O helpers ---------------------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def configure_logging(log_dir: Path, verbose: bool) -> None:
    ensure_dir(log_dir)
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(log_dir / "fetch_finra_biweekly.log",
                                maxBytes=1_000_000, backupCount=3, encoding="utf-8"),
        ],
    )


def parse_headers(header_kv: List[str] | None) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for item in header_kv or []:
        if "=" not in item:
            raise ValueError(f"Invalid header (expected KEY=VALUE): {item}")
        k, v = item.split("=", 1)
        headers[k.strip()] = v.strip()
    return headers


def download_file(url: str, dest: Path, headers: Dict[str, str],
                  overwrite: bool, timeout: int = 30) -> bool:
    """
    Stream a file to disk.
    Returns True if fetched (or skipped because exists and not overwriting),
    False if 404. Raises for other HTTP errors.
    """
    if dest.exists() and not overwrite:
        logging.info(f"Skip (exists): {dest.name}")
        return True

    try:
        with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
            if r.status_code == 404:
                logging.warning(f"Not found (404): {url}")
                return False
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + ".part")
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            tmp.replace(dest)
            logging.info(f"Downloaded: {dest.name}")
            return True
    except requests.RequestException as e:
        logging.error(f"HTTP error for {url}: {e}")
        raise


# --------------------------- Main ---------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Download FINRA OTC biweekly CSVs (weekend rollback to Friday).")
    time_grp = ap.add_mutually_exclusive_group()
    time_grp.add_argument("--start", help="Start month YYYY-MM (inclusive).")
    ap.add_argument("--end", help="End month YYYY-MM (inclusive). Required if --start is set.")
    time_grp.add_argument("--months-back", type=int,
                          help="Pull past N full months up to current month (default 12).")

    ap.add_argument("--out-dir", default="downloads", help="Output directory (default: ./downloads)")
    ap.add_argument("--header", action="append", help="Extra HTTP header KEY=VALUE (repeatable)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files if present")
    ap.add_argument("--dry-run", action="store_true", help="Print planned URLs without downloading")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs")

    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    configure_logging(root / "logs", verbose=args.verbose)

    # Determine month window
    today_first = date.today().replace(day=1)
    if args.start:
        if not args.end:
            ap.error("--end is required when --start is provided.")
        start = parse_year_month(args.start)
        end = parse_year_month(args.end)
    else:
        months_back = args.months_back if (args.months_back and args.months_back > 0) else 12
        # Start = (months_back-1) months before current month
        y, m = today_first.year, today_first.month
        shift = months_back - 1
        y -= shift // 12
        m -= shift % 12
        if m <= 0:
            y -= 1
            m += 12
        start = date(y, m, 1)
        end = today_first

    if end < start:
        ap.error("End month must be >= start month.")

    headers = parse_headers(args.header)
    out_dir = (root / args.out_dir).resolve()
    ensure_dir(out_dir)

    logging.info("=== FINRA biweekly fetch ===")
    logging.info(f"Base URL : {BASE_URL}")
    logging.info(f"Window   : {start:%Y-%m} .. {end:%Y-%m}")
    logging.info(f"Out dir  : {out_dir}")
    logging.info(f"Overwrite: {args.overwrite}")
    logging.info(f"Dry run  : {args.dry_run}")
    if headers:
        logging.debug(f"Headers  : {headers}")

    # Build list of dates & URLs
    dates = build_file_dates(start, end)
    items = [(f"{PREFIX}{ymd_compact(d)}{EXT}", d) for d in dates]

    if args.dry_run:
        for fname, d in items:
            url = f"{BASE_URL}{fname}"
            logging.info(f"[DRY] {url} -> {out_dir / fname}  (weekday={d.weekday()})")
        logging.info("Dry-run complete.")
        return

    # Download
    ok, missing = 0, 0
    for fname, d in items:
        url = f"{BASE_URL}{fname}"
        dest = out_dir / fname
        logging.info(f"Fetching {url} -> {dest.name}")
        try:
            found = download_file(url, dest, headers=headers, overwrite=args.overwrite)
            if found:
                ok += 1
            else:
                missing += 1
        except Exception:
            # already logged
            continue

    logging.info(f"Done. Downloaded/kept: {ok}, Missing (404): {missing}, Total considered: {len(items)}")


if __name__ == "__main__":
    main()

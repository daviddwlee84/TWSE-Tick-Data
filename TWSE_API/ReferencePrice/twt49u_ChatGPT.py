from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


# ----------------------------
# Utilities
# ----------------------------


def parse_yyyymmdd(s: str) -> date:
    """Parse YYYYMMDD string to date."""
    return datetime.strptime(s, "%Y%m%d").date()


def to_iso(d: date) -> str:
    """Convert date to ISO string 'YYYY-MM-DD' for SQLite."""
    return d.isoformat()


def parse_float(x: Any) -> Optional[float]:
    """Parse numeric fields from TWSE JSON; return None for N/A/empty."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.upper() == "N/A":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_symbol(x: Any) -> str:
    """Normalize symbol string."""
    return str(x).strip()


# ----------------------------
# Data model
# ----------------------------


@dataclass(frozen=True)
class ExRightRecord:
    ex_date: str  # ISO 'YYYY-MM-DD'
    symbol: str  # e.g. '2330', '0050'
    name: str

    prev_close: Optional[float]
    ref_price: Optional[float]  # Reference price (除權息參考價)
    rights_div_value: Optional[float]  # rights+dividend value (權值+息值)
    event_type: Optional[str]  # '權' / '息' / '權息'

    limit_up: Optional[float]
    limit_down: Optional[float]
    open_auction_base: Optional[float]  # Opening auction base price (開盤競價基準)
    ex_div_ref_price: Optional[
        float
    ]  # Price after subtracting dividend (減除股利參考價)

    details_key: Optional[str]  # e.g. '3189,20260126'
    mops_latest: Optional[str]  # latest report period / link
    nav_per_share: Optional[float]
    eps_per_share: Optional[float]

    source_start: str  # YYYYMMDD from API
    source_end: str  # YYYYMMDD from API
    fetched_at_utc: str  # ISO datetime


# ----------------------------
# SQLite schema + IO
# ----------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS exright_reference_prices (
    ex_date TEXT NOT NULL,               -- 'YYYY-MM-DD'
    symbol TEXT NOT NULL,
    name TEXT,

    prev_close REAL,
    ref_price REAL,
    rights_div_value REAL,
    event_type TEXT,

    limit_up REAL,
    limit_down REAL,
    open_auction_base REAL,
    ex_div_ref_price REAL,

    details_key TEXT,
    mops_latest TEXT,
    nav_per_share REAL,
    eps_per_share REAL,

    source_start TEXT NOT NULL,           -- 'YYYYMMDD'
    source_end TEXT NOT NULL,             -- 'YYYYMMDD'
    fetched_at_utc TEXT NOT NULL,         -- ISO datetime

    PRIMARY KEY (ex_date, symbol)
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_exright_symbol_date ON exright_reference_prices(symbol, ex_date);",
    "CREATE INDEX IF NOT EXISTS idx_exright_date ON exright_reference_prices(ex_date);",
]

UPSERT_SQL = """
INSERT INTO exright_reference_prices (
    ex_date, symbol, name,
    prev_close, ref_price, rights_div_value, event_type,
    limit_up, limit_down, open_auction_base, ex_div_ref_price,
    details_key, mops_latest, nav_per_share, eps_per_share,
    source_start, source_end, fetched_at_utc
) VALUES (
    :ex_date, :symbol, :name,
    :prev_close, :ref_price, :rights_div_value, :event_type,
    :limit_up, :limit_down, :open_auction_base, :ex_div_ref_price,
    :details_key, :mops_latest, :nav_per_share, :eps_per_share,
    :source_start, :source_end, :fetched_at_utc
)
ON CONFLICT(ex_date, symbol) DO UPDATE SET
    name = excluded.name,
    prev_close = excluded.prev_close,
    ref_price = excluded.ref_price,
    rights_div_value = excluded.rights_div_value,
    event_type = excluded.event_type,
    limit_up = excluded.limit_up,
    limit_down = excluded.limit_down,
    open_auction_base = excluded.open_auction_base,
    ex_div_ref_price = excluded.ex_div_ref_price,
    details_key = excluded.details_key,
    mops_latest = excluded.mops_latest,
    nav_per_share = excluded.nav_per_share,
    eps_per_share = excluded.eps_per_share,
    source_start = excluded.source_start,
    source_end = excluded.source_end,
    fetched_at_utc = excluded.fetched_at_utc
;
"""


def init_sqlite(db_path: str) -> None:
    """Initialize SQLite database schema and indexes."""
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")  # Better concurrency
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute(CREATE_TABLE_SQL)
        for sql in CREATE_INDEXES_SQL:
            con.execute(sql)
        con.commit()
    finally:
        con.close()


def upsert_records(db_path: str, records: Sequence[ExRightRecord]) -> int:
    """Upsert records into SQLite. Returns number of rows affected (approx)."""
    if not records:
        return 0

    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")

        payloads: List[Dict[str, Any]] = [r.__dict__ for r in records]
        con.executemany(UPSERT_SQL, payloads)
        con.commit()
        return len(records)
    finally:
        con.close()


# ----------------------------
# Fetch TWSE TWT49U (public JSON)
# ----------------------------


def fetch_twt49u_json(
    start_yyyymmdd: str, end_yyyymmdd: str, timeout_sec: float = 20.0
) -> Dict[str, Any]:
    """
    Fetch TWSE public JSON for TWT49U.
    Note: '_' query param is cache-buster; not required.
    """
    url = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
    params = {
        "startDate": start_yyyymmdd,
        "endDate": end_yyyymmdd,
        "response": "json",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def parse_twt49u_payload(
    payload: Dict[str, Any], start_yyyymmdd: str, end_yyyymmdd: str
) -> List[ExRightRecord]:
    """Parse TWT49U payload into ExRightRecord list."""
    stat = payload.get("stat")
    if stat != "OK":
        # Sometimes returns "很抱歉..." or "查無資料"
        return []

    fields: List[str] = payload.get("fields", [])
    data_rows: List[List[Any]] = payload.get("data", [])

    # Build column index map
    col_idx: Dict[str, int] = {name: i for i, name in enumerate(fields)}

    def get(row: List[Any], key: str) -> Any:
        i = col_idx.get(key)
        if i is None or i >= len(row):
            return None
        return row[i]

    fetched_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    records: List[ExRightRecord] = []
    for row in data_rows:
        # "資料日期" is ROC year formatted in Chinese; we rely on API params for ISO date mapping:
        # If you want exact conversion from "115年01月26日" -> "2026-01-26", do it here.
        # For robustness, we convert using startDate when fetching daily; for ranges, we can parse from row.
        raw_date = str(get(row, "資料日期") or "").strip()

        # Convert ROC date string like "115年01月26日" -> ISO
        # This conversion assumes ROC year = Gregorian year - 1911.
        ex_date_iso = roc_zh_date_to_iso(raw_date)

        symbol = parse_symbol(get(row, "股票代號"))
        name = str(get(row, "股票名稱") or "").strip()

        rec = ExRightRecord(
            ex_date=ex_date_iso,
            symbol=symbol,
            name=name,
            prev_close=parse_float(get(row, "除權息前收盤價")),
            ref_price=parse_float(get(row, "除權息參考價")),
            rights_div_value=parse_float(get(row, "權值+息值")),
            event_type=str(get(row, "權/息") or "").strip() or None,
            limit_up=parse_float(get(row, "漲停價格")),
            limit_down=parse_float(get(row, "跌停價格")),
            open_auction_base=parse_float(get(row, "開盤競價基準")),
            ex_div_ref_price=parse_float(get(row, "減除股利參考價")),
            details_key=str(get(row, "詳細資料") or "").strip() or None,
            mops_latest=str(get(row, "最近一次申報資料 季別/日期") or "").strip()
            or None,
            nav_per_share=parse_float(get(row, "最近一次申報每股 (單位)淨值")),
            eps_per_share=parse_float(get(row, "最近一次申報每股 (單位)盈餘")),
            source_start=start_yyyymmdd,
            source_end=end_yyyymmdd,
            fetched_at_utc=fetched_at_utc,
        )
        records.append(rec)

    return records


def roc_zh_date_to_iso(roc_zh_date: str) -> str:
    """
    Convert ROC date format like '115年01月26日' to ISO '2026-01-26'.
    """
    s = roc_zh_date.strip()
    if not s:
        # Fallback: unknown
        return "0000-00-00"

    try:
        # Extract ROC year, month, day
        year_part, rest = s.split("年", 1)
        month_part, day_part = rest.split("月", 1)
        day_part = day_part.replace("日", "").strip()

        roc_year = int(year_part)
        month = int(month_part)
        day = int(day_part)

        greg_year = roc_year + 1911
        return date(greg_year, month, day).isoformat()
    except Exception:
        # If parsing fails, return raw string; better than crashing
        return s


def ingest_twt49u_to_sqlite(
    db_path: str, start_yyyymmdd: str, end_yyyymmdd: str
) -> int:
    """Fetch TWT49U and upsert into SQLite. Returns number of records upserted."""
    payload = fetch_twt49u_json(start_yyyymmdd, end_yyyymmdd)
    records = parse_twt49u_payload(payload, start_yyyymmdd, end_yyyymmdd)
    return upsert_records(db_path, records)


# ----------------------------
# Fast query methods
# ----------------------------


def get_reference_price(
    db_path: str,
    ex_date: date,
    symbol: str,
    prefer_auction_base: bool = True,
) -> Optional[float]:
    """
    Query reference price for a given date and symbol.
    If prefer_auction_base=True, return Opening Auction Base Price if available,
    otherwise return Reference Price.
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            """
            SELECT ex_date, symbol, ref_price, open_auction_base
            FROM exright_reference_prices
            WHERE ex_date = ? AND symbol = ?
            """,
            (to_iso(ex_date), symbol),
        ).fetchone()

        if row is None:
            return None

        if prefer_auction_base and row["open_auction_base"] is not None:
            return float(row["open_auction_base"])
        if row["ref_price"] is not None:
            return float(row["ref_price"])
        return None
    finally:
        con.close()


def get_reference_prices_bulk(
    db_path: str,
    ex_date: date,
    symbols: Sequence[str],
    prefer_auction_base: bool = True,
) -> Dict[str, Optional[float]]:
    """
    Bulk query reference price for many symbols on the same date.
    Returns dict: {symbol: price or None}.
    """
    if not symbols:
        return {}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        placeholders = ",".join(["?"] * len(symbols))
        rows = con.execute(
            f"""
            SELECT symbol, ref_price, open_auction_base
            FROM exright_reference_prices
            WHERE ex_date = ? AND symbol IN ({placeholders})
            """,
            (to_iso(ex_date), *symbols),
        ).fetchall()

        out: Dict[str, Optional[float]] = {s: None for s in symbols}
        for r in rows:
            sym = str(r["symbol"])
            if prefer_auction_base and r["open_auction_base"] is not None:
                out[sym] = float(r["open_auction_base"])
            elif r["ref_price"] is not None:
                out[sym] = float(r["ref_price"])
            else:
                out[sym] = None
        return out
    finally:
        con.close()


# ----------------------------
# Example usage
# ----------------------------

if __name__ == "__main__":
    db = "twse_exright.sqlite"
    init_sqlite(db)

    # Ingest one day (your example)
    n = ingest_twt49u_to_sqlite(db, "20260101", "20260126")
    print(f"Upserted {n} records")

    # Query single symbol
    px = get_reference_price(db, date(2026, 1, 26), "3189", prefer_auction_base=True)
    print("3189 ref/auction base:", px)

    # Query many symbols
    pxs = get_reference_prices_bulk(db, date(2026, 1, 26), ["3189", "2330", "0050"])
    print(pxs)

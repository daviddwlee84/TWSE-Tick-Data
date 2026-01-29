from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import yfinance as yf


# ----------------------------
# SQLite schema
# ----------------------------

DAILY_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices (
  date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  adj_close REAL,
  volume REAL,
  source TEXT NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  PRIMARY KEY (date, symbol)
);
"""

DAILY_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_daily_symbol_date ON daily_prices(symbol, date);",
    "CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_prices(date);",
]

DAILY_UPSERT_SQL = """
INSERT INTO daily_prices (
  date, symbol, open, high, low, close, adj_close, volume, source, fetched_at_utc
) VALUES (
  :date, :symbol, :open, :high, :low, :close, :adj_close, :volume, :source, :fetched_at_utc
)
ON CONFLICT(date, symbol) DO UPDATE SET
  open=excluded.open,
  high=excluded.high,
  low=excluded.low,
  close=excluded.close,
  adj_close=excluded.adj_close,
  volume=excluded.volume,
  source=excluded.source,
  fetched_at_utc=excluded.fetched_at_utc
;
"""


def init_daily_prices(db_path: str) -> None:
    """Initialize daily_prices table and indexes."""
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute(DAILY_CREATE_SQL)
        for sql in DAILY_INDEX_SQL:
            con.execute(sql)
        con.commit()
    finally:
        con.close()


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten yfinance output columns.
    - For single ticker, columns are usually simple: Open/High/Low/Close/Adj Close/Volume
    - For multiple tickers, columns are often MultiIndex: ('Open', '2330.TW')
    """
    if isinstance(df.columns, pd.MultiIndex):
        # Convert to columns like 'Open__2330.TW'
        df.columns = [f"{c0}__{c1}" for c0, c1 in df.columns.to_list()]
    return df


def fetch_yfinance_daily(
    symbols: Sequence[str],
    start: str,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch daily OHLCV from yfinance.

    Key points:
    - auto_adjust=False keeps raw OHLC (unadjusted).
    - actions=True includes dividends/splits metadata (not returned as columns in this DataFrame, but useful in some modes).
    """
    df = yf.download(
        tickers=list(symbols),
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,  # IMPORTANT: keep unadjusted prices
        actions=True,
        group_by="column",
        threads=True,
        progress=False,
    )
    df = _flatten_yf_columns(df)
    return df


def _to_payload_rows(
    df: pd.DataFrame,
    symbols: Sequence[str],
    source: str = "yfinance",
) -> List[Dict[str, Any]]:
    """Convert yfinance DataFrame into row dicts for SQLite upsert."""
    fetched_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    out: List[Dict[str, Any]] = []

    # If single symbol, columns are 'Open', 'High'...; if multi, 'Open__2330.TW'...
    single = (len(symbols) == 1) and all(
        col in df.columns
        for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    )

    if single:
        sym = symbols[0]
        for idx, row in df.iterrows():
            out.append(
                {
                    "date": idx.date().isoformat(),
                    "symbol": sym,
                    "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
                    "high": float(row["High"]) if pd.notna(row["High"]) else None,
                    "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
                    "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
                    "adj_close": float(row["Adj Close"])
                    if pd.notna(row["Adj Close"])
                    else None,
                    "volume": float(row["Volume"]) if pd.notna(row["Volume"]) else None,
                    "source": source,
                    "fetched_at_utc": fetched_at_utc,
                }
            )
        return out

    # Multi-symbol path
    for sym in symbols:
        open_c = f"Open__{sym}"
        high_c = f"High__{sym}"
        low_c = f"Low__{sym}"
        close_c = f"Close__{sym}"
        adj_c = f"Adj Close__{sym}"
        vol_c = f"Volume__{sym}"

        if close_c not in df.columns:
            # Skip symbols not returned by Yahoo
            continue

        sub = df[
            [
                c
                for c in [open_c, high_c, low_c, close_c, adj_c, vol_c]
                if c in df.columns
            ]
        ].copy()
        for idx, row in sub.iterrows():
            out.append(
                {
                    "date": idx.date().isoformat(),
                    "symbol": sym,
                    "open": float(row.get(open_c))
                    if pd.notna(row.get(open_c))
                    else None,
                    "high": float(row.get(high_c))
                    if pd.notna(row.get(high_c))
                    else None,
                    "low": float(row.get(low_c)) if pd.notna(row.get(low_c)) else None,
                    "close": float(row.get(close_c))
                    if pd.notna(row.get(close_c))
                    else None,
                    "adj_close": float(row.get(adj_c))
                    if (adj_c in sub.columns and pd.notna(row.get(adj_c)))
                    else None,
                    "volume": float(row.get(vol_c))
                    if (vol_c in sub.columns and pd.notna(row.get(vol_c)))
                    else None,
                    "source": source,
                    "fetched_at_utc": fetched_at_utc,
                }
            )
    return out


def upsert_daily_prices(db_path: str, rows: Sequence[Dict[str, Any]]) -> int:
    """Upsert many daily price rows into SQLite."""
    if not rows:
        return 0
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.executemany(DAILY_UPSERT_SQL, list(rows))
        con.commit()
        return len(rows)
    finally:
        con.close()


# ----------------------------
# Fast queries
# ----------------------------


def get_close_and_prev_close(
    db_path: str, date_iso: str, symbol: str
) -> Tuple[Optional[float], Optional[float]]:
    """
    Query close and prev_close (previous trading day's close) using SQL window logic.

    This avoids storing prev_close physically, while still being fast with indexes.
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            """
            WITH t AS (
              SELECT
                date, symbol, close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
              FROM daily_prices
              WHERE symbol = ?
            )
            SELECT close, prev_close
            FROM t
            WHERE date = ? AND symbol = ?;
            """,
            (symbol, date_iso, symbol),
        ).fetchone()
        if row is None:
            return None, None
        close = float(row["close"]) if row["close"] is not None else None
        prev_close = float(row["prev_close"]) if row["prev_close"] is not None else None
        return close, prev_close
    finally:
        con.close()


# ----------------------------
# Example usage
# ----------------------------

if __name__ == "__main__":
    db = "twse_exright.sqlite"

    init_daily_prices(db)

    # Taiwan tickers on Yahoo typically use ".TW" (listed) or ".TWO" (OTC).
    symbols = ["2330.TW", "0050.TW", "3189.TWO"]

    df = fetch_yfinance_daily(symbols, start="2026-01-01", end="2026-02-01")
    rows = _to_payload_rows(df, symbols)
    n = upsert_daily_prices(db, rows)
    print("Upserted daily rows:", n)

    close, prev_close = get_close_and_prev_close(db, "2026-01-26", "3189.TWO")
    print("close, prev_close:", close, prev_close)

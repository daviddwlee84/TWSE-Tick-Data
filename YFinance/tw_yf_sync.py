#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
tw_yf_sync.py

用 yfinance 下載 / 同步台股 (TWSE/TPEX) 歷史日資料，
每檔個股一個 Parquet 檔，支援增量更新與簡單 rate-limit backoff。

另外會在每檔資料裡計算並儲存：

    AdjFactor   : 復權累積因子 F_t = AdjClose / Close
    RefPrice    : 近似「平盤價 / 參考價」，RefPrice_t = Close_{t-1} * F_{t-1} / F_t
    PreCloseRaw : 原始昨日收盤價 Close_{t-1}
    PreCloseCorr: 修正後昨日收盤價（已吃公司行為），＝ RefPrice_t

依賴:
    pip install yfinance pandas pyarrow
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf


# ==== Config ====


@dataclass
class Config:
    tickers_file: str
    data_dir: str = "./data/tw_yf"
    start: str = "1990-01-01"
    end: Optional[str] = None  # None = 今天
    interval: str = "1d"

    batch_size: int = 50
    sleep: float = 2.0
    max_retries: int = 5
    backoff_factor: float = 2.0

    suffix_tw: str = ".TW"
    suffix_two: str = ".TWO"

    auto_adjust: bool = False
    actions: bool = True
    progress: bool = False


# ==== Helper: 讀取 tickers ====


def load_tickers(path: str, cfg: Config) -> List[str]:
    tickers: List[str] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            code: str
            board: Optional[str] = None

            if "," in line:
                code, board = [x.strip() for x in line.split(",", 1)]
            else:
                code = line

            if "." in code:
                sym = code
            else:
                suffix = (
                    cfg.suffix_tw
                    if (board is None or board == "TW")
                    else (cfg.suffix_two if board == "TWO" else f".{board}")
                )
                sym = f"{code}{suffix}"

            tickers.append(sym)

    tickers = sorted(set(tickers))
    return tickers


# ==== 檔案路徑 / 讀寫 ====


def symbol_to_filename(symbol: str) -> str:
    return symbol.replace(".", "_") + ".parquet"


def get_symbol_path(symbol: str, cfg: Config) -> Path:
    return Path(cfg.data_dir) / symbol_to_filename(symbol)


def load_existing(symbol: str, cfg: Config) -> Optional[pd.DataFrame]:
    path = get_symbol_path(symbol, cfg)
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df


def compute_adjustments(df: pd.DataFrame) -> pd.DataFrame:
    """
    在 yfinance 回來的 DataFrame 上加上：

        AdjFactor   = Adj Close / Close
        RefPrice    = Close.shift(1) * (AdjFactor.shift(1) / AdjFactor)
        PreCloseRaw = Close.shift(1)
        PreCloseCorr= RefPrice

    這裡是純數學推回「理論平盤價」，不代表官方 TWSE/TPEX 平盤價。
    """
    df = df.copy()

    # 有些資料可能沒有 Adj Close（例如某些 ETF 或奇怪 ticker）
    if "Adj Close" in df.columns and "Close" in df.columns:
        with pd.option_context("mode.use_inf_as_na", True):
            # 避免除以 0 或 inf
            adj_factor = df["Adj Close"] / df["Close"]
            df["AdjFactor"] = adj_factor

            # RefPrice_t = Close_{t-1} * F_{t-1} / F_t
            f_prev = adj_factor.shift(1)
            f_curr = adj_factor
            ref = df["Close"].shift(1) * (f_prev / f_curr)

            df["RefPrice"] = ref
    else:
        df["AdjFactor"] = pd.NA
        df["RefPrice"] = pd.NA

    # 原始昨日收盤
    if "Close" in df.columns:
        df["PreCloseRaw"] = df["Close"].shift(1)
    else:
        df["PreCloseRaw"] = pd.NA

    # 修正後昨日收盤
    df["PreCloseCorr"] = df["RefPrice"]

    return df


def save_symbol(symbol: str, df: pd.DataFrame, cfg: Config) -> None:
    """
    儲存前會：
        - 排序 index
        - 去 duplicated dates
        - 加上 AdjFactor / RefPrice / PreCloseRaw / PreCloseCorr
    """
    path = get_symbol_path(symbol, cfg)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    df = compute_adjustments(df)

    df.to_parquet(path, compression="zstd")


# ==== Incremental start date ====


def get_last_date(symbol: str, cfg: Config) -> Optional[pd.Timestamp]:
    df = load_existing(symbol, cfg)
    if df is None or df.empty:
        return None
    return df.index.max().normalize()


def compute_batch_start(
    symbols: List[str],
    cfg: Config,
    end_dt: pd.Timestamp,
) -> Tuple[pd.Timestamp, Dict[str, bool]]:
    start_candidates: List[pd.Timestamp] = []
    fully_covered: Dict[str, bool] = {}

    global_start = pd.to_datetime(cfg.start).normalize()

    for sym in symbols:
        last = get_last_date(sym, cfg)
        if last is None:
            start_candidates.append(global_start)
            fully_covered[sym] = False
        else:
            if last >= end_dt:
                fully_covered[sym] = True
            else:
                start_candidates.append(last + pd.Timedelta(days=1))
                fully_covered[sym] = False

    if not start_candidates:
        return end_dt, fully_covered

    batch_start = min(start_candidates)
    return batch_start, fully_covered


# ==== yfinance download ====


def download_batch(
    symbols: List[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    cfg: Config,
) -> Optional[pd.DataFrame]:
    if not symbols:
        return None

    tickers_str = " ".join(symbols)
    attempt = 0
    delay = 1.0

    while True:
        attempt += 1
        try:
            df = yf.download(
                tickers=tickers_str,
                start=start,
                end=end + pd.Timedelta(days=1),  # end inclusive
                interval=cfg.interval,
                auto_adjust=cfg.auto_adjust,
                actions=cfg.actions,
                group_by="ticker",
                progress=cfg.progress,
                threads=True,
            )
            return df
        except Exception as e:
            msg = str(e)
            is_rate = (
                "Too Many Requests" in msg
                or "rate limit" in msg.lower()
                or "YFRateLimitError" in msg
            )
            if not is_rate or attempt >= cfg.max_retries:
                print(f"[ERROR] download batch failed (attempt {attempt}): {e}")
                raise

            print(f"[WARN] rate-limited, attempt {attempt}, sleep {delay:.1f}s ...")
            time.sleep(delay)
            delay *= cfg.backoff_factor


def normalize_downloaded(
    raw: pd.DataFrame,
    symbols: List[str],
) -> Dict[str, pd.DataFrame]:
    result: Dict[str, pd.DataFrame] = {}
    if raw is None or raw.empty:
        return {sym: pd.DataFrame() for sym in symbols}

    if not isinstance(raw.columns, pd.MultiIndex):
        df = raw.copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        result[symbols[0]] = df
        return result

    for sym in symbols:
        if sym not in raw.columns.get_level_values(0):
            result[sym] = pd.DataFrame()
            continue
        df = raw[sym].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        result[sym] = df

    return result


# ==== 主流程 ====


def sync_all(cfg: Config) -> None:
    tickers = load_tickers(cfg.tickers_file, cfg)
    print(f"[INFO] Loaded {len(tickers)} symbols from {cfg.tickers_file}")

    if cfg.end is None:
        end_dt = pd.Timestamp.today().normalize()
    else:
        end_dt = pd.to_datetime(cfg.end).normalize()

    total = len(tickers)
    for i in range(0, total, cfg.batch_size):
        batch_syms = tickers[i : i + cfg.batch_size]
        print(f"\n[INFO] Batch {i // cfg.batch_size + 1}: {len(batch_syms)} symbols")

        batch_start, fully_covered = compute_batch_start(batch_syms, cfg, end_dt)

        if all(fully_covered.values()):
            print("[INFO] All symbols in this batch already up-to-date, skip.")
            continue

        symbols_to_download = [s for s in batch_syms if not fully_covered[s]]

        print(
            f"[INFO] Downloading {len(symbols_to_download)} symbols "
            f"from {batch_start.date()} to {end_dt.date()}"
        )

        raw = download_batch(symbols_to_download, batch_start, end_dt, cfg)
        per_symbol = normalize_downloaded(raw, symbols_to_download)

        for sym in batch_syms:
            if fully_covered.get(sym, False):
                continue

            new_df = per_symbol.get(sym, pd.DataFrame())
            if new_df is None or new_df.empty:
                print(f"[WARN] No new data for {sym}")
                continue

            existing = load_existing(sym, cfg)
            if existing is None:
                combined = new_df
            else:
                combined = pd.concat([existing, new_df])

            save_symbol(sym, combined, cfg)
            print(
                f"[OK] Saved {sym}: "
                f"{combined.index.min().date()} -> {combined.index.max().date()}"
            )

        if i + cfg.batch_size < total and cfg.sleep > 0:
            print(f"[INFO] Sleeping {cfg.sleep:.1f}s before next batch ...")
            time.sleep(cfg.sleep)


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Sync Taiwan stocks from yfinance.")
    parser.add_argument("--tickers-file", type=str, required=True)
    parser.add_argument("--data-dir", type=str, default="./data/tw_yf")
    parser.add_argument("--start", type=str, default="1990-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--interval", type=str, default="1d")

    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--sleep", type=float, default=2.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-factor", type=float, default=2.0)

    args = parser.parse_args()
    return Config(
        tickers_file=args.tickers_file,
        data_dir=args.data_dir,
        start=args.start,
        end=args.end,
        interval=args.interval,
        batch_size=args.batch_size,
        sleep=args.sleep,
        max_retries=args.max_retries,
        backoff_factor=args.backoff_factor,
    )


if __name__ == "__main__":
    cfg = parse_args()
    sync_all(cfg)

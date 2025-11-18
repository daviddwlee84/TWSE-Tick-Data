"""
Examples for Reading TWSE Data from Different Sources.

This script demonstrates how to read data from:
1. Feather files (organized by date/instrument)
2. Parquet catalog (NautilusTrader catalog)
3. Raw binary files (direct parsing)

Each method has different use cases and performance characteristics.
"""

from pathlib import Path
import pandas as pd
import time
from collections import defaultdict

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.serialization.base import register_serializable_type
from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader


def example_feather_by_date():
    """
    Example 1: Read from Feather files (by date/instrument).

    Best for:
    - Daily analysis
    - Single instrument focus
    - Direct pandas usage
    - Simple file organization
    """
    print(f"\n{'='*80}")
    print("Example 1: Reading Feather Files (by Date/Instrument)")
    print(f"{'='*80}\n")

    feather_root = Path(__file__).parent / "feather_data"

    if not feather_root.exists():
        print(f"❌ Feather directory not found: {feather_root}")
        print(f"Run convert_to_feather_by_date.py first")
        return

    # ========================================================================
    # 1. Read specific instrument on specific date
    # ========================================================================
    print("1. Read specific instrument on specific date")
    print("-" * 40)

    date = "2024-11-11"
    instrument_id = "0050.TWSE"

    file_path = feather_root / date / f"{instrument_id}.feather"

    if file_path.exists():
        start_time = time.time()
        df = pd.read_feather(file_path)
        elapsed = time.time() - start_time

        print(f"File: {file_path.relative_to(feather_root)}")
        print(f"Loaded: {len(df)} rows in {elapsed*1000:.2f}ms")
        print(f"\nFirst 3 rows:")
        print(
            df[["instrument_id", "display_time", "trade_price", "trade_volume"]].head(3)
        )
    else:
        print(f"File not found: {file_path}")

    # ========================================================================
    # 2. Read all instruments for a specific date
    # ========================================================================
    print(f"\n2. Read all instruments for a date ({date})")
    print("-" * 40)

    date_dir = feather_root / date
    if date_dir.exists():
        start_time = time.time()

        dfs = {}
        for feather_file in sorted(date_dir.glob("*.feather")):
            instrument_id = feather_file.stem
            dfs[instrument_id] = pd.read_feather(feather_file)

        # Combine all instruments
        daily_df = pd.concat(dfs.values(), ignore_index=True)
        elapsed = time.time() - start_time

        print(f"Loaded {len(dfs)} instruments")
        print(f"Total rows: {len(daily_df)}")
        print(f"Time: {elapsed*1000:.2f}ms")
        print(f"\nInstruments: {list(dfs.keys())}")
        print(f"\nRecords per instrument:")
        for inst_id, df in dfs.items():
            print(f"  {inst_id}: {len(df)} rows")

    # ========================================================================
    # 3. Read one instrument across all dates
    # ========================================================================
    print(f"\n3. Read one instrument across all dates")
    print("-" * 40)

    instrument_id = "0050.TWSE"
    start_time = time.time()

    dfs = []
    dates_found = []
    for date_dir in sorted(feather_root.iterdir()):
        if date_dir.is_dir():
            feather_file = date_dir / f"{instrument_id}.feather"
            if feather_file.exists():
                df = pd.read_feather(feather_file)
                dfs.append(df)
                dates_found.append(date_dir.name)

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = full_df.sort_values("ts_event")
        elapsed = time.time() - start_time

        print(f"Instrument: {instrument_id}")
        print(f"Dates: {', '.join(dates_found)}")
        print(f"Total rows: {len(full_df)}")
        print(f"Time: {elapsed*1000:.2f}ms")
    else:
        print(f"No data found for {instrument_id}")

    # ========================================================================
    # 4. Calculate statistics
    # ========================================================================
    print(f"\n4. Calculate statistics (spread analysis)")
    print("-" * 40)

    file_path = feather_root / date / f"{instrument_id}.feather"
    if file_path.exists():
        df = pd.read_feather(file_path)

        # Calculate spread
        df["best_bid"] = df["buy_price_1"]
        df["best_ask"] = df["sell_price_1"]
        df["spread"] = df["best_ask"] - df["best_bid"]
        df["spread_bps"] = (df["spread"] / df["best_bid"]) * 10000

        # Filter valid spreads
        valid_spreads = df[df["spread"] > 0]

        if len(valid_spreads) > 0:
            print(f"Valid spread observations: {len(valid_spreads)}")
            print(f"Average spread: {valid_spreads['spread'].mean():.2f}")
            print(f"Average spread (bps): {valid_spreads['spread_bps'].mean():.2f}")
            print(f"Min spread: {valid_spreads['spread'].min():.2f}")
            print(f"Max spread: {valid_spreads['spread'].max():.2f}")


def example_parquet_catalog():
    """
    Example 2: Read from Parquet Catalog (NautilusTrader).

    Best for:
    - SQL-like queries
    - Cross-instrument analysis
    - NautilusTrader integration
    - Complex filtering
    """
    print(f"\n{'='*80}")
    print("Example 2: Reading Parquet Catalog (NautilusTrader)")
    print(f"{'='*80}\n")

    catalog_path = Path(__file__).parent / "twse_catalog"

    if not catalog_path.exists():
        print(f"❌ Catalog not found: {catalog_path}")
        print(f"Run convert_to_catalog_direct.py first")
        return

    # Register serialization
    register_serializable_type(
        TWSESnapshotData, TWSESnapshotData.to_dict, TWSESnapshotData.from_dict
    )
    register_arrow(
        TWSESnapshotData,
        TWSESnapshotData.schema(),
        TWSESnapshotData.to_catalog,
        TWSESnapshotData.from_catalog,
    )

    catalog = ParquetDataCatalog(str(catalog_path))

    # ========================================================================
    # 1. Query all data
    # ========================================================================
    print("1. Query all data")
    print("-" * 40)

    start_time = time.time()
    all_data = catalog.query(data_cls=TWSESnapshotData)
    elapsed = time.time() - start_time

    print(f"Total snapshots: {len(all_data)}")
    print(f"Time: {elapsed*1000:.2f}ms")

    if all_data:
        print(f"First snapshot: {all_data[0].data}")

    # ========================================================================
    # 2. Query with WHERE clause
    # ========================================================================
    print(f"\n2. Query with WHERE clause (trades only)")
    print("-" * 40)

    start_time = time.time()
    trades = catalog.query(
        data_cls=TWSESnapshotData, where="match_flag = 'Y' AND trade_price > 0"
    )
    elapsed = time.time() - start_time

    print(f"Trades found: {len(trades)}")
    print(f"Time: {elapsed*1000:.2f}ms")

    # ========================================================================
    # 3. Query specific instrument
    # ========================================================================
    print(f"\n3. Query specific instrument")
    print("-" * 40)

    start_time = time.time()
    tsmc = catalog.query(data_cls=TWSESnapshotData, where="instrument_id = '2330.TWSE'")
    elapsed = time.time() - start_time

    print(f"2330.TWSE snapshots: {len(tsmc)}")
    print(f"Time: {elapsed*1000:.2f}ms")

    # ========================================================================
    # 4. Read as pandas directly
    # ========================================================================
    print(f"\n4. Read Parquet directly with pandas")
    print("-" * 40)

    # Find parquet files
    parquet_files = list(catalog_path.rglob("*.parquet"))
    if parquet_files:
        parquet_file = parquet_files[0]
        start_time = time.time()
        df = pd.read_parquet(parquet_file)
        elapsed = time.time() - start_time

        print(f"File: {parquet_file.relative_to(catalog_path)}")
        print(f"Loaded: {len(df)} rows")
        print(f"Time: {elapsed*1000:.2f}ms")
        print(f"Columns: {len(df.columns)}")


def example_raw_binary():
    """
    Example 3: Read from Raw Binary Files.

    Best for:
    - Initial data exploration
    - One-time parsing
    - Understanding file format
    - When no pre-processed data exists
    """
    print(f"\n{'='*80}")
    print("Example 3: Reading Raw Binary Files")
    print(f"{'='*80}\n")

    binary_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"

    if not binary_file.exists():
        print(f"❌ Binary file not found: {binary_file}")
        return

    loader = TWSEDataLoader(binary_file)

    # ========================================================================
    # 1. Parse all records
    # ========================================================================
    print("1. Parse all records")
    print("-" * 40)

    start_time = time.time()
    snapshots = list(loader.read_records())
    elapsed = time.time() - start_time

    print(f"Total snapshots: {len(snapshots)}")
    print(f"Parsing time: {elapsed:.2f}s")
    print(f"Throughput: {len(snapshots)/elapsed:.0f} snapshots/sec")

    if snapshots:
        print(f"\nFirst snapshot: {snapshots[0]}")

    # ========================================================================
    # 2. Parse with limit
    # ========================================================================
    print(f"\n2. Parse with limit (first 10)")
    print("-" * 40)

    start_time = time.time()
    limited = list(loader.read_records(limit=10))
    elapsed = time.time() - start_time

    print(f"Loaded: {len(limited)} snapshots")
    print(f"Time: {elapsed*1000:.2f}ms")

    for i, snapshot in enumerate(limited[:3], 1):
        print(f"\n{i}. {snapshot.instrument_id} @ {snapshot.display_time}")
        print(f"   Price: {snapshot.trade_price}, Volume: {snapshot.trade_volume}")

    # ========================================================================
    # 3. Convert to pandas
    # ========================================================================
    print(f"\n3. Convert to pandas DataFrame")
    print("-" * 40)

    start_time = time.time()
    snapshots = list(loader.read_records(limit=100))
    data_dicts = [s.to_dict() for s in snapshots]
    df = pd.DataFrame(data_dicts)
    elapsed = time.time() - start_time

    print(f"Created DataFrame with {len(df)} rows")
    print(f"Time: {elapsed*1000:.2f}ms")
    print(f"\nColumns: {list(df.columns[:5])}... (showing first 5)")
    print(f"\nFirst 3 rows:")
    print(df[["instrument_id", "display_time", "trade_price", "trade_volume"]].head(3))


def compare_performance():
    """
    Compare performance of different reading methods.
    """
    print(f"\n{'='*80}")
    print("Performance Comparison")
    print(f"{'='*80}\n")

    results = {}

    # 1. Feather
    feather_root = Path(__file__).parent / "feather_data"
    if feather_root.exists():
        feather_files = list(feather_root.rglob("*.feather"))
        if feather_files:
            file_path = feather_files[0]
            start = time.time()
            df = pd.read_feather(file_path)
            results["feather"] = {
                "time_ms": (time.time() - start) * 1000,
                "rows": len(df),
                "method": "Feather (date/instrument)",
            }

    # 2. Parquet catalog
    catalog_path = Path(__file__).parent / "twse_catalog"
    if catalog_path.exists():
        parquet_files = list(catalog_path.rglob("*.parquet"))
        if parquet_files:
            file_path = parquet_files[0]
            start = time.time()
            df = pd.read_parquet(file_path)
            results["parquet"] = {
                "time_ms": (time.time() - start) * 1000,
                "rows": len(df),
                "method": "Parquet (catalog)",
            }

    # 3. Raw binary
    binary_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    if binary_file.exists():
        loader = TWSEDataLoader(binary_file)
        start = time.time()
        snapshots = list(loader.read_records(limit=40))
        results["binary"] = {
            "time_ms": (time.time() - start) * 1000,
            "rows": len(snapshots),
            "method": "Raw binary parsing",
        }

    # Display results
    if results:
        print("Reading ~40 rows:")
        print()
        print(f"{'Method':<30} {'Time (ms)':>12} {'Rows':>8} {'Speed':>15}")
        print("-" * 70)

        for key, data in sorted(results.items(), key=lambda x: x[1]["time_ms"]):
            method = data["method"]
            time_ms = data["time_ms"]
            rows = data["rows"]
            speed = f"{rows/time_ms*1000:.0f} rows/sec"
            print(f"{method:<30} {time_ms:>12.2f} {rows:>8} {speed:>15}")

        print()
        fastest = min(results.values(), key=lambda x: x["time_ms"])
        print(f"🏆 Fastest: {fastest['method']} ({fastest['time_ms']:.2f}ms)")


def main():
    """Run all examples."""
    print(f"\n{'='*80}")
    print("TWSE Data Reading Examples")
    print(f"{'='*80}")
    print()
    print("This script demonstrates reading TWSE data from three sources:")
    print("  1. Feather files (by date/instrument)")
    print("  2. Parquet catalog (NautilusTrader)")
    print("  3. Raw binary files")

    # Run examples
    example_feather_by_date()
    example_parquet_catalog()
    example_raw_binary()
    compare_performance()

    # Summary
    print(f"\n{'='*80}")
    print("Summary: When to Use Each Format")
    print(f"{'='*80}\n")

    print("📁 Feather (by date/instrument):")
    print("  ✓ Daily backtesting")
    print("  ✓ Per-instrument analysis")
    print("  ✓ Fast pandas reading")
    print("  ✓ Simple file organization")
    print("  ✓ Best for: Single day, single instrument workflows")
    print()

    print("📊 Parquet Catalog (NautilusTrader):")
    print("  ✓ SQL-like WHERE queries")
    print("  ✓ Cross-instrument analysis")
    print("  ✓ NautilusTrader integration")
    print("  ✓ Column-level compression")
    print("  ✓ Best for: Complex queries, production systems")
    print()

    print("🔧 Raw Binary:")
    print("  ✓ Initial exploration")
    print("  ✓ One-time parsing")
    print("  ✓ No pre-processing needed")
    print("  ✓ Understanding data format")
    print("  ✓ Best for: Development, debugging")

    print(f"\n{'='*80}")
    print("Recommendation:")
    print(f"{'='*80}")
    print("For daily backtesting: Use Feather (by date/instrument)")
    print("For research & complex queries: Use Parquet Catalog")
    print("For debugging & exploration: Parse Raw Binary directly")


if __name__ == "__main__":
    main()

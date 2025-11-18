"""
Example: Query TWSE Snapshot Data from Catalog.

This script demonstrates how to efficiently query TWSE data from
the Parquet catalog without parsing raw binary files.

Prerequisites:
    Run convert_to_catalog.py first to create the catalog.
"""

from pathlib import Path
from collections import Counter

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.serialization.base import register_serializable_type

from twse_snapshot_data import TWSESnapshotData


def setup():
    """Register serialization for TWSESnapshotData."""
    register_serializable_type(
        TWSESnapshotData, TWSESnapshotData.to_dict, TWSESnapshotData.from_dict
    )

    register_arrow(
        TWSESnapshotData,
        TWSESnapshotData.schema(),
        TWSESnapshotData.to_catalog,
        TWSESnapshotData.from_catalog,
    )


def main():
    """Demonstrate various catalog queries."""
    setup()

    # Initialize catalog
    catalog_path = Path(__file__).parent / "twse_catalog"

    if not catalog_path.exists():
        print(f"❌ Catalog not found at: {catalog_path}")
        print(f"Please run convert_to_catalog.py first to create the catalog.")
        return

    print(f"\n{'='*80}")
    print("TWSE Snapshot Data: Catalog Query Examples")
    print(f"{'='*80}\n")

    catalog = ParquetDataCatalog(str(catalog_path))
    print(f"✓ Loaded catalog from: {catalog_path}\n")

    # ========================================================================
    # Example 1: Get all data
    # ========================================================================
    print(f"{'='*80}")
    print("Example 1: Query All Snapshots")
    print(f"{'='*80}\n")

    all_snapshots = catalog.query(data_cls=TWSESnapshotData)
    print(f"Total snapshots: {len(all_snapshots)}")

    if all_snapshots:
        print(f"\nFirst snapshot:")
        print(f"  {all_snapshots[0].data}")
        print(f"\nLast snapshot:")
        print(f"  {all_snapshots[-1].data}")

    # ========================================================================
    # Example 2: Filter by match flag (trades only)
    # ========================================================================
    print(f"\n{'='*80}")
    print("Example 2: Query Snapshots with Trades (match_flag='Y')")
    print(f"{'='*80}\n")

    trade_snapshots = catalog.query(
        data_cls=TWSESnapshotData,
        where="match_flag = 'Y'",
    )
    print(f"Snapshots with trades: {len(trade_snapshots)}")

    if trade_snapshots:
        print(f"\nExample trade snapshot:")
        snapshot = trade_snapshots[0].data
        print(f"  Instrument: {snapshot.instrument_id}")
        print(f"  Time: {snapshot.display_time}")
        print(f"  Price: {snapshot.trade_price}")
        print(f"  Volume: {snapshot.trade_volume}")

    # ========================================================================
    # Example 3: Query specific instrument
    # ========================================================================
    print(f"\n{'='*80}")
    print("Example 3: Query Specific Instrument (0050.TWSE)")
    print(f"{'='*80}\n")

    instrument_snapshots = catalog.query(
        data_cls=TWSESnapshotData,
        where="instrument_id = '0050.TWSE'",
    )
    print(f"Snapshots for 0050.TWSE: {len(instrument_snapshots)}")

    if instrument_snapshots:
        print(f"\nFirst 5 snapshots for 0050.TWSE:")
        for i, snapshot in enumerate(instrument_snapshots[:5], 1):
            s = snapshot.data
            print(
                f"  {i}. Time: {s.display_time}, "
                f"Price: {s.trade_price}, "
                f"Volume: {s.trade_volume}"
            )

    # ========================================================================
    # Example 4: Statistics per instrument
    # ========================================================================
    print(f"\n{'='*80}")
    print("Example 4: Statistics by Instrument")
    print(f"{'='*80}\n")

    # Count snapshots per instrument
    instrument_counts = Counter(str(s.data.instrument_id) for s in all_snapshots)

    print("Snapshot count by instrument:")
    for instrument_id, count in instrument_counts.most_common():
        print(f"  {instrument_id}: {count} snapshots")

    # ========================================================================
    # Example 5: Order book analysis
    # ========================================================================
    print(f"\n{'='*80}")
    print("Example 5: Order Book Analysis")
    print(f"{'='*80}\n")

    # Get snapshots with non-empty order books
    snapshots_with_books = [
        s for s in all_snapshots if s.data.buy_levels and s.data.sell_levels
    ]

    print(f"Snapshots with full order book: {len(snapshots_with_books)}")

    if snapshots_with_books:
        snapshot = snapshots_with_books[0].data
        print(f"\nExample order book for {snapshot.instrument_id}:")
        print(f"  Time: {snapshot.display_time}")
        print(f"\n  Bid Levels:")
        for i, level in enumerate(snapshot.buy_levels[:5], 1):
            print(f"    {i}. {level.price:>8.2f} x {level.volume:>10}")
        print(f"\n  Ask Levels:")
        for i, level in enumerate(snapshot.sell_levels[:5], 1):
            print(f"    {i}. {level.price:>8.2f} x {level.volume:>10}")

        # Calculate spread
        if snapshot.buy_levels and snapshot.sell_levels:
            best_bid = snapshot.buy_levels[0].price
            best_ask = snapshot.sell_levels[0].price
            spread = best_ask - best_bid
            spread_bps = (spread / best_bid) * 10000
            print(f"\n  Spread: {spread:.2f} ({spread_bps:.2f} bps)")

    # ========================================================================
    # Example 6: Time range query
    # ========================================================================
    print(f"\n{'='*80}")
    print("Example 6: Time Range Analysis")
    print(f"{'='*80}\n")

    # Group by display date
    dates = Counter(s.data.display_date for s in all_snapshots)

    print("Snapshots by date:")
    for date, count in sorted(dates.items()):
        print(f"  {date}: {count} snapshots")

    # ========================================================================
    # Summary
    # ========================================================================
    print(f"\n{'='*80}")
    print("Performance Benefits")
    print(f"{'='*80}\n")

    print("✓ No need to parse raw binary files")
    print("✓ Fast column-based queries with Parquet")
    print("✓ Efficient filtering with WHERE clauses")
    print("✓ Data is stored in compressed, optimized format")
    print("✓ Can integrate with pandas/polars for analysis")

    print(f"\n{'='*80}")
    print("Integration with Pandas")
    print(f"{'='*80}\n")

    print("You can also load data into pandas DataFrame:")
    print("```python")
    print("import pandas as pd")
    print("from pyarrow import parquet as pq")
    print()
    print("# Read parquet files directly")
    print("table = catalog.query(data_cls=TWSESnapshotData, as_nautilus=False)")
    print("df = table.to_pandas()")
    print()
    print("# Or use pandas directly")
    print("df = pd.read_parquet(catalog.path / 'data' / 'twse_snapshot_data.parquet')")
    print("```")


if __name__ == "__main__":
    main()

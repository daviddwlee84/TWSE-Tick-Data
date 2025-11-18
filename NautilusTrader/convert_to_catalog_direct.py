"""
Convert TWSE Snapshot Raw Binary to Parquet Catalog (Direct Method).

This script uses the direct write_data() method instead of streaming,
which is simpler and more reliable for custom data types.

Workflow:
1. Parse raw binary file
2. Write directly to Parquet catalog
3. Query data efficiently

Based on: https://nautilustrader.io/docs/latest/concepts/data#feather-streaming-and-conversion
"""

from pathlib import Path
import time

from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.base import register_serializable_type
from nautilus_trader.serialization.arrow.serializer import register_arrow

from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader


def setup_catalog(catalog_path: Path) -> ParquetDataCatalog:
    """
    Initialize and configure the data catalog.

    Parameters
    ----------
    catalog_path : Path
        Directory path for the catalog

    Returns
    -------
    ParquetDataCatalog
        Initialized catalog instance
    """
    print(f"\n{'='*80}")
    print("Step 1: Setting up Data Catalog")
    print(f"{'='*80}\n")

    # Create catalog directory if it doesn't exist
    catalog_path.mkdir(parents=True, exist_ok=True)

    # Initialize catalog
    catalog = ParquetDataCatalog(str(catalog_path))
    print(f"✓ Catalog initialized at: {catalog_path}")

    # Register serialization for custom data type
    register_serializable_type(
        TWSESnapshotData, TWSESnapshotData.to_dict, TWSESnapshotData.from_dict
    )
    print("✓ Registered TWSESnapshotData serialization (dict/bytes)")

    # Register Arrow serialization for Parquet support
    register_arrow(
        TWSESnapshotData,
        TWSESnapshotData.schema(),
        TWSESnapshotData.to_catalog,
        TWSESnapshotData.from_catalog,
    )
    print("✓ Registered TWSESnapshotData Arrow serialization (Parquet)")

    return catalog


def convert_binary_to_catalog(
    data_file: Path,
    catalog: ParquetDataCatalog,
    limit: int | None = None,
    batch_size: int = 1000,
) -> int:
    """
    Convert TWSE snapshot binary data directly to Parquet catalog.

    Parameters
    ----------
    data_file : Path
        Path to the raw TWSE snapshot binary file
    catalog : ParquetDataCatalog
        The data catalog for storage
    limit : int, optional
        Maximum number of records to process
    batch_size : int
        Number of records to write in each batch

    Returns
    -------
    int
        Total number of records written
    """
    print(f"\n{'='*80}")
    print("Step 2: Converting Binary to Parquet Catalog")
    print(f"{'='*80}\n")

    # Load and process data
    print(f"Reading data from: {data_file}")
    loader = TWSEDataLoader(data_file)

    total_records = 0
    batch = []
    start_time = time.time()

    print(f"⏳ Processing and writing data (batch size: {batch_size})...\n")

    for snapshot in loader.read_records(limit=limit):
        batch.append(snapshot)
        total_records += 1

        # Write batch when it reaches batch_size
        if len(batch) >= batch_size:
            catalog.write_data(batch)
            print(
                f"  ✓ Wrote batch: {total_records - len(batch) + 1}-{total_records} snapshots"
            )
            batch = []

    # Write remaining records
    if batch:
        catalog.write_data(batch)
        print(
            f"  ✓ Wrote final batch: {total_records - len(batch) + 1}-{total_records} snapshots"
        )

    elapsed = time.time() - start_time
    print(f"\n✓ Conversion completed in {elapsed:.2f}s")
    print(f"  - Total records: {total_records}")
    print(f"  - Throughput: {total_records/elapsed:.0f} snapshots/sec")

    return total_records


def verify_catalog(catalog: ParquetDataCatalog) -> None:
    """
    Verify data was written correctly and show examples.

    Parameters
    ----------
    catalog : ParquetDataCatalog
        The data catalog
    """
    print(f"\n{'='*80}")
    print("Step 3: Verifying Catalog Data")
    print(f"{'='*80}\n")

    # Query all data
    all_data = catalog.query(data_cls=TWSESnapshotData)
    print(f"✓ Total snapshots in catalog: {len(all_data)}")

    if not all_data:
        print("⚠️  No data found in catalog!")
        return

    # Show first and last snapshots
    print(f"\nFirst snapshot:")
    print(f"  {all_data[0]}")
    print(f"\nLast snapshot:")
    print(f"  {all_data[-1]}")

    # Query snapshots with trades
    trade_data = catalog.query(
        data_cls=TWSESnapshotData,
        where="match_flag = 'Y'",
    )
    print(f"\n✓ Snapshots with trades (match_flag='Y'): {len(trade_data)}")

    if trade_data:
        print(f"  Example trade: {trade_data[0]}")

    # Count by instrument
    from collections import Counter

    instrument_counts = Counter(str(s.data.instrument_id) for s in all_data)

    print(f"\n✓ Snapshots by instrument:")
    for instrument_id, count in instrument_counts.most_common():
        print(f"  {instrument_id}: {count} snapshots")


def show_usage_examples(catalog_path: Path) -> None:
    """
    Show how to use the catalog in other scripts.

    Parameters
    ----------
    catalog_path : Path
        Path to the catalog
    """
    print(f"\n{'='*80}")
    print("Step 4: Usage Examples")
    print(f"{'='*80}\n")

    print("Now you can query data efficiently without parsing binary files!")
    print("\nPython code:")
    print("```python")
    print("from nautilus_trader.persistence.catalog import ParquetDataCatalog")
    print("from nautilus_trader.serialization.arrow.serializer import register_arrow")
    print("from nautilus_trader.serialization.base import register_serializable_type")
    print("from twse_snapshot_data import TWSESnapshotData")
    print()
    print("# Register serialization")
    print("register_serializable_type(")
    print("    TWSESnapshotData,")
    print("    TWSESnapshotData.to_dict,")
    print("    TWSESnapshotData.from_dict")
    print(")")
    print("register_arrow(")
    print("    TWSESnapshotData,")
    print("    TWSESnapshotData.schema(),")
    print("    TWSESnapshotData.to_catalog,")
    print("    TWSESnapshotData.from_catalog")
    print(")")
    print()
    print(f"# Load catalog")
    print(f"catalog = ParquetDataCatalog('{catalog_path}')")
    print()
    print("# Query all data")
    print("all_data = catalog.query(data_cls=TWSESnapshotData)")
    print()
    print("# Query with filter")
    print("trades = catalog.query(")
    print("    data_cls=TWSESnapshotData,")
    print("    where=\"match_flag = 'Y'\"")
    print(")")
    print()
    print("# Query specific instrument")
    print("tsmc_data = catalog.query(")
    print("    data_cls=TWSESnapshotData,")
    print("    where=\"instrument_id = '2330.TWSE'\"")
    print(")")
    print("```")

    print("\n\nPandas integration:")
    print("```python")
    print("import pandas as pd")
    print()
    print("# Convert to DataFrame")
    print("df = pd.DataFrame([s.to_dict() for s in all_data])")
    print()
    print("# Or read Parquet directly")
    print(f"df = pd.read_parquet('{catalog_path}/data/twse_snapshot_data.parquet')")
    print("```")


def main():
    """
    Main workflow: Binary → Parquet Catalog → Query
    """
    print(f"\n{'='*80}")
    print("TWSE Snapshot Data: Direct Binary to Parquet Conversion")
    print(f"{'='*80}\n")
    print("Workflow: Raw Binary → Parquet Catalog → Query")
    print("Using direct write_data() method for reliability with custom data types")

    # Configuration
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    catalog_path = Path(__file__).parent / "twse_catalog"

    # Process settings
    limit = None  # Set to None to process all records, or a number for testing
    batch_size = 1000  # Write in batches for better performance

    if not data_file.exists():
        print(f"\n❌ Error: Data file not found: {data_file}")
        return

    # Step 1: Setup catalog
    catalog = setup_catalog(catalog_path)

    # Step 2: Convert binary to catalog
    total_records = convert_binary_to_catalog(
        data_file, catalog, limit=limit, batch_size=batch_size
    )

    # Step 3: Verify data
    verify_catalog(catalog)

    # Step 4: Show usage examples
    show_usage_examples(catalog_path)

    print(f"\n{'='*80}")
    print("✓ Conversion Complete!")
    print(f"{'='*80}\n")
    print(f"Converted {total_records} snapshots to Parquet catalog")
    print(f"Catalog location: {catalog_path.absolute()}")
    print(f"\nRun query_catalog_example.py to see more query examples")


if __name__ == "__main__":
    main()

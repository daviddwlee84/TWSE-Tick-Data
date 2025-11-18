"""
Convert TWSE Snapshot Raw Binary to Parquet Catalog.

This script demonstrates the feather streaming workflow:
1. Stream data from raw binary files during a "dummy" backtest
2. Automatically save to temporary feather files
3. Convert feather files to permanent parquet catalog
4. Query data efficiently from the catalog

Based on: https://nautilustrader.io/docs/latest/concepts/data#feather-streaming-and-conversion
"""

from pathlib import Path
import time

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money, Currency
from nautilus_trader.model.enums import OmsType, AccountType
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.config import StreamingConfig
from nautilus_trader.serialization.base import register_serializable_type
from nautilus_trader.serialization.arrow.serializer import register_arrow

from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader
from twse_adapter import TWSEInstrumentProvider


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


def stream_data_to_feather(
    data_file: Path, catalog: ParquetDataCatalog, limit: int | None = None
) -> str:
    """
    Stream TWSE snapshot data to feather files using backtest engine.

    This function creates a "dummy" backtest that processes data through
    the engine, automatically streaming it to temporary feather files.

    Parameters
    ----------
    data_file : Path
        Path to the raw TWSE snapshot binary file
    catalog : ParquetDataCatalog
        The data catalog for storage
    limit : int, optional
        Maximum number of records to process

    Returns
    -------
    str
        Instance ID of the backtest run (needed for conversion)
    """
    print(f"\n{'='*80}")
    print("Step 2: Streaming Data to Feather Files")
    print(f"{'='*80}\n")

    # Load data
    print(f"Loading data from: {data_file}")
    loader = TWSEDataLoader(data_file)
    snapshots = list(loader.read_records(limit=limit))
    print(f"✓ Loaded {len(snapshots)} snapshots")

    # Configure streaming
    streaming_config = StreamingConfig(
        catalog_path=str(catalog.path),
        include_types=[TWSESnapshotData],  # Types to stream
        flush_interval_ms=1000,  # Flush to disk every 1 second
    )
    print(f"✓ Configured streaming:")
    print(f"  - Include types: {[t.__name__ for t in streaming_config.include_types]}")
    print(f"  - Flush interval: {streaming_config.flush_interval_ms}ms")

    # Create backtest engine with streaming enabled
    engine_config = BacktestEngineConfig(
        logging=LoggingConfig(log_level="WARNING"),  # Reduce noise
        streaming=streaming_config,
    )
    engine = BacktestEngine(config=engine_config)

    # Setup venue and instruments
    venue = Venue("TWSE")
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=Currency.from_str("TWD"),
        starting_balances=[Money.from_str("1000000 TWD")],
    )

    # Add instruments
    instrument_provider = TWSEInstrumentProvider(load_all_on_start=True)
    for instrument in instrument_provider.get_all().values():
        engine.add_instrument(instrument)

    print(f"✓ Added {len(instrument_provider.get_all())} instruments")

    # Add custom data (this will be streamed to feather)
    print(f"\n⏳ Processing and streaming {len(snapshots)} snapshots...")
    start_time = time.time()

    for snapshot in snapshots:
        engine.add_data([snapshot])

    # Run the engine (finalizes streaming)
    engine.run()

    elapsed = time.time() - start_time
    print(f"✓ Streaming completed in {elapsed:.2f}s")
    print(f"  - Throughput: {len(snapshots)/elapsed:.0f} snapshots/sec")

    # Get instance ID for conversion
    instance_id = str(engine.kernel.instance_id)
    print(f"✓ Backtest instance ID: {instance_id}")

    return instance_id


def convert_feather_to_parquet(catalog: ParquetDataCatalog, instance_id: str) -> None:
    """
    Convert streamed feather files to permanent parquet catalog.

    Parameters
    ----------
    catalog : ParquetDataCatalog
        The data catalog
    instance_id : str
        The backtest instance ID from streaming
    """
    print(f"\n{'='*80}")
    print("Step 3: Converting Feather to Parquet Catalog")
    print(f"{'='*80}\n")

    print(f"⏳ Converting feather files for instance: {instance_id}")
    start_time = time.time()

    # Convert streamed data to permanent catalog
    catalog.convert_stream_to_data(
        instance_id=instance_id,
        data_cls=TWSESnapshotData,
    )

    elapsed = time.time() - start_time
    print(f"✓ Conversion completed in {elapsed:.2f}s")
    print(f"✓ Data is now available in the catalog for querying")


def query_catalog_example(catalog: ParquetDataCatalog) -> None:
    """
    Demonstrate querying data from the catalog.

    Parameters
    ----------
    catalog : ParquetDataCatalog
        The data catalog
    """
    print(f"\n{'='*80}")
    print("Step 4: Querying Data from Catalog")
    print(f"{'='*80}\n")

    # Example 1: Query all data
    print("Example 1: Query all TWSE snapshot data")
    all_data = catalog.query(data_cls=TWSESnapshotData)
    print(f"✓ Total snapshots in catalog: {len(all_data)}")

    if all_data:
        print(f"✓ First snapshot: {all_data[0]}")
        print(f"✓ Last snapshot: {all_data[-1]}")

    # Example 2: Query with filter
    print("\nExample 2: Query snapshots with trades (match_flag='Y')")
    trade_data = catalog.query(
        data_cls=TWSESnapshotData,
        where="match_flag = 'Y'",
    )
    print(f"✓ Snapshots with trades: {len(trade_data)}")

    if trade_data:
        print(f"✓ First trade snapshot: {trade_data[0]}")

    # Example 3: Query specific instrument
    print("\nExample 3: Query specific instrument (0050.TWSE)")
    instrument_data = catalog.query(
        data_cls=TWSESnapshotData,
        where="instrument_id = '0050.TWSE'",
    )
    print(f"✓ Snapshots for 0050.TWSE: {len(instrument_data)}")

    # Show catalog info
    print(f"\n{'='*80}")
    print("Catalog Information")
    print(f"{'='*80}\n")
    print(f"Catalog path: {catalog.path}")
    print(f"Available instruments: {catalog.instruments()}")


def main():
    """
    Main workflow: Binary → Feather → Parquet → Query
    """
    print(f"\n{'='*80}")
    print("TWSE Snapshot Data: Binary to Parquet Catalog Conversion")
    print(f"{'='*80}\n")
    print("Workflow: Raw Binary → Feather Stream → Parquet Catalog → Query")
    print(
        "Based on: https://nautilustrader.io/docs/latest/concepts/data#feather-streaming-and-conversion"
    )

    # Configuration
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    catalog_path = Path(__file__).parent / "twse_catalog"

    # Process a limited number of records for demo
    # Set to None to process all records
    limit = None  # Process all records from Sample_new

    if not data_file.exists():
        print(f"\n❌ Error: Data file not found: {data_file}")
        return

    # Step 1: Setup catalog
    catalog = setup_catalog(catalog_path)

    # Step 2: Stream data to feather
    instance_id = stream_data_to_feather(data_file, catalog, limit=limit)

    # Step 3: Convert feather to parquet
    convert_feather_to_parquet(catalog, instance_id)

    # Step 4: Query examples
    query_catalog_example(catalog)

    print(f"\n{'='*80}")
    print("✓ Conversion Complete!")
    print(f"{'='*80}\n")
    print(f"You can now query data using:")
    print(f"  from nautilus_trader.persistence.catalog import ParquetDataCatalog")
    print(f"  catalog = ParquetDataCatalog('{catalog_path}')")
    print(f"  data = catalog.query(data_cls=TWSESnapshotData)")
    print(f"\nCatalog location: {catalog_path.absolute()}")


if __name__ == "__main__":
    main()

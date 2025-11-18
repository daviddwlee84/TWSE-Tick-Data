# TWSE Data Integration with NautilusTrader

This project demonstrates how to integrate Taiwan Stock Exchange (TWSE) tick data with NautilusTrader using custom data types.

## Overview

The implementation includes:

1. **Custom Data Type** (`twse_snapshot_data.py`): Defines `TWSESnapshotData` which represents TWSE order book snapshots with 5-level depth
2. **Data Loader** (`twse_data_loader.py`): Parses raw 190-byte fixed-width binary files
3. **Demo** (`demo_backtest.py`): Shows how to use Actor and Strategy to subscribe to custom data

## Features

- Parse TWSE snapshot files (dspYYYYMMDD format)
- 5-level order book depth (bid and ask)
- Trade information (price, volume, flags)
- Support for Actor and Strategy patterns
- Full NautilusTrader integration with event bus

## Setup

This project uses `uv` for dependency management. The virtual environment should already be set up.

```bash
# Activate the virtual environment
cd NautilusTrader
source .venv/bin/activate

# Verify installation
python -c "import nautilus_trader; print(nautilus_trader.__version__)"
```

## Data Format

TWSE snapshot files contain 190-byte fixed-width records with the following structure:

| Field           | Type  | Position | Length | Description                             |
| --------------- | ----- | -------- | ------ | --------------------------------------- |
| Securities Code | X(06) | 1-6      | 6      | Stock symbol                            |
| Display Time    | X(12) | 7-18     | 12     | Time in HHMMSSMS format                 |
| Remark          | X(01) | 19       | 1      | Space: Normal, T: Trial, S: Stabilizing |
| Trend Flag      | X(01) | 20       | 1      | Space/R/F/C                             |
| Match Flag      | X(01) | 21       | 1      | Space: No match, Y: Match               |
| Trade Price     | 9(06) | 23-28    | 6      | Price * 100                             |
| Trade Volume    | 9(08) | 29-36    | 8      | Volume in lots                          |
| Buy Levels      | -     | 39-108   | 70     | 5 levels (price + volume)               |
| Sell Levels     | -     | 111-180  | 70     | 5 levels (price + volume)               |
| Display Date    | 9(08) | 181-188  | 8      | YYYYMMDD                                |

## Usage

### Test Data Loader

```bash
python twse_data_loader.py ../snapshot/Sample_new
```

This will parse and display the first 10 records from the sample file.

### Run Actor Demo

```bash
python demo_backtest.py
```

This runs a demo using an `Actor` to subscribe to TWSE snapshot data.

### Run Strategy Demo

```bash
python demo_backtest.py strategy
```

This runs a demo using a `Strategy` to subscribe to TWSE snapshot data.

### Run Both Demos

```bash
python demo_backtest.py
```

This will run both the Actor and Strategy demos sequentially.

## Code Structure

### TWSESnapshotData

The custom data class inherits from `nautilus_trader.core.Data` and includes:

- Required properties: `ts_event` and `ts_init` (UNIX nanosecond timestamps)
- Serialization methods: `to_dict`, `from_dict`, `to_bytes`, `from_bytes`
- Catalog methods: `to_catalog`, `from_catalog`, `schema` for PyArrow integration

### TWSEDataLoader

Provides functionality to:

- Read raw binary files (190 bytes per record)
- Parse fixed-width fields according to TWSE format
- Convert to `TWSESnapshotData` objects
- Handle timestamps and data types

### Actor/Strategy Integration

Both `TWSESnapshotActor` and `TWSESnapshotStrategy` demonstrate:

1. **Subscription**:
```python
self.subscribe_data(data_type=DataType(TWSESnapshotData))
```

2. **Handling data**:
```python
def on_data(self, data):
    if isinstance(data, TWSESnapshotData):
        # Process snapshot data
        pass
```

## Example Output

```
Total records: 40
Loaded 40 snapshots for demo

Instruments in data: ['0050.TWSE', '9958.TWSE']

Running backtest with Actor...
[INFO] TWSESnapshotActor started
[INFO] Subscribed to TWSESnapshotData
[INFO] Snapshot: 0050.TWSE @ 083004446448, Price: 0.0, Volume: 0, Bid Levels: 5, Ask Levels: 1
[INFO]   Best Bid: 199.5 x 29
[INFO]   Best Ask: 203.0 x 2
...
```

## Real-world Data

Full data files are available from TWSE. Example file sizes:

- Daily file: ~8-11 GB uncompressed
- Compressed (gzip -9): ~400 MB (95.8% reduction)
- Records per day: ~50 million

To process full files, modify the `limit` parameter in the demo:

```python
# Load all records (may take time)
snapshots = list(loader.read_records())  # No limit
```

## Advanced Usage

### Publishing Custom Data

From an adapter or data client:

```python
from nautilus_trader.model.data import DataType

self.publish_data(
    DataType(TWSESnapshotData),
    snapshot_data
)
```

### Storing in Catalog

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import register_arrow

# Register serialization
register_arrow(
    TWSESnapshotData,
    TWSESnapshotData.schema(),
    TWSESnapshotData.to_catalog,
    TWSESnapshotData.from_catalog
)

# Write to catalog
catalog = ParquetDataCatalog('.')
catalog.write_data([snapshot_data])
```

## References

- [NautilusTrader Custom Data Documentation](https://nautilustrader.io/docs/latest/concepts/data#custom-data)
- [TWSE Data Format Documentation](../snapshot/README_new.md)
- [NautilusTrader GitHub](https://github.com/nautechsystems/nautilus_trader)

## License

This code is provided as-is for educational purposes.

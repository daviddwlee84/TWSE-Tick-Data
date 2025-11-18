# TWSE Data Integration with NautilusTrader

This project demonstrates how to integrate Taiwan Stock Exchange (TWSE) tick data with NautilusTrader using custom data types and the adapter pattern.

## Overview

The implementation includes:

1. **Custom Data Type** (`twse_snapshot_data.py`): `TWSESnapshotData` represents TWSE order book snapshots with 5-level depth
2. **Data Loader** (`twse_data_loader.py`): Parses raw 190-byte fixed-width binary files
3. **Instrument Provider** (`twse_adapter/providers.py`): Provides TWSE instrument definitions
4. **Data Client** (`twse_adapter/data.py`): Streams data through NautilusTrader's message bus
5. **Demos**: Multiple examples showing different integration approaches

## Features

- ✅ Parse TWSE snapshot files (dspYYYYMMDD format)
- ✅ 5-level order book depth (bid and ask)
- ✅ Trade information (price, volume, flags)
- ✅ Full NautilusTrader adapter implementation
- ✅ Support for Actor and Strategy patterns
- ✅ Instrument provider for TWSE securities
- 📖 Comprehensive documentation on adapter patterns

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

### Run Original Demo (Backtest)

```bash
python demo_backtest.py
```

This runs the original demo using `engine.add_data()` (has known limitations with custom data routing).

### Run Adapter Demo

```bash
# Simplified adapter demo (recommended for backtesting)
python demo_simple_adapter.py

# Full async adapter demo (for live trading)
python demo_adapter.py
```

### Convert Binary to Efficient Formats

Two options for pre-processing data:

#### Option 1: Feather by Date/Instrument (Recommended for Daily Backtesting)

```bash
# Convert to feather_data/yyyy-MM-dd/instrument_id.feather
python convert_to_feather_by_date.py
```

**Format:** `feather_data/2024-11-11/0050.TWSE.feather`

**Benefits:**
- ✅ Simple file organization (date → instrument)
- ✅ Fast pandas reading (~3-5ms per file)
- ✅ Perfect for daily backtesting
- ✅ Easy to understand and navigate

#### Option 2: Parquet Catalog (Recommended for Complex Queries)

```bash
# Convert to NautilusTrader catalog
python convert_to_catalog_direct.py

# Query with examples
python query_catalog_example.py
```

**Benefits:**
- ✅ SQL-like WHERE queries
- ✅ Column-level compression
- ✅ NautilusTrader integration
- ✅ Cross-instrument analysis

#### Compare All Formats

```bash
# See performance comparison and usage examples
python query_feather_example.py
```

This demonstrates reading from:
1. Feather files (by date/instrument)
2. Parquet catalog (NautilusTrader)
3. Raw binary files

## Architecture

### 1. Custom Data Type

`TWSESnapshotData` inherits from `nautilus_trader.core.Data`:

```python
class TWSESnapshotData(Data):
    def __init__(self, instrument_id, display_time, ...):
        self.instrument_id = instrument_id
        self._ts_event = ts_event
        self._ts_init = ts_init
        # ... other fields
```

**Key Features:**
- Required properties: `ts_event` and `ts_init` (UNIX nanosecond timestamps)
- Serialization methods: `to_dict`, `from_dict`, `to_bytes`, `from_bytes`
- Catalog methods: `to_catalog`, `from_catalog`, `schema` for PyArrow

### 2. Instrument Provider

`TWSEInstrumentProvider` provides instrument definitions:

```python
provider = TWSEInstrumentProvider(load_all_on_start=True)
instruments = provider.get_all()  # Dict[InstrumentId, Equity]
```

**Supported Instruments:**
- 0050 - 元大台灣50 (Yuanta Taiwan Top 50 ETF)
- 2330 - 台積電 (TSMC)
- 9958 - 世紀鋼 (Century Iron and Steel)
- And more...

### 3. Data Client (Adapter)

`TWSEDataClient` streams data through the message bus:

```python
class TWSEDataClient(LiveMarketDataClient):
    async def _stream_data(self):
        for snapshot in self._loader.read_records():
            self._msgbus.publish(
                topic=f"data.{self._venue}.{snapshot.__class__.__name__}",
                msg=snapshot,
            )
```

**Benefits:**
- Proper message bus routing
- Works for both backtest and live trading
- Enables actor/strategy subscriptions

### 4. Actor/Strategy Integration

Subscribe to custom data:

```python
class TWSESnapshotActor(Actor):
    def on_start(self):
        self.subscribe_data(data_type=DataType(TWSESnapshotData))
    
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

Running backtest...
[INFO] TWSESnapshotActor started
[INFO] Subscribed to TWSESnapshotData
[INFO] Snapshot: 0050.TWSE @ 083004446448, Price: 0.0, Volume: 0
[INFO]   Best Bid: 199.5 x 29
[INFO]   Best Ask: 203.0 x 2
...
```

## Real-world Data

Full data files are available from TWSE. Example file sizes:

- Daily file: ~8.2 GB uncompressed
- Compressed (gzip -9): ~397 MB (95.8% reduction)
- Records per day: ~43 million (50M snapshots)

### Compression Support ⭐

The data loader now supports **multiple compression formats** with streaming decompression:

```python
# Auto-detects compression format by extension
loader = TWSEDataLoader('snapshot/dsp20241104.gz')   # gzip
loader = TWSEDataLoader('snapshot/dsp20241104.zst')  # zstandard
snapshots = list(loader.read_records())

# No memory explosion - decompresses on-the-fly!
# RAM usage: ~10-20 MB (same as uncompressed)
```

**Supported Formats:**
- ✅ **gzip (.gz)** - Python built-in, 95% compression
- ✅ **Zstandard (.zst)** - Better compression (96%), faster decompression

**Benefits:**
- ✅ **95-96% storage savings** (8.2 GB → 397-330 MB)
- ✅ **Streaming decompression** (no RAM explosion)
- ✅ **No code changes needed** (auto-detects format)
- ✅ **Sequential reading preserved**

**Performance:**
| Format       | File Size | Compression | Speed           | RAM Usage |
| ------------ | --------- | ----------- | --------------- | --------- |
| Uncompressed | 8.2 GB    | -           | ~40,000 rec/sec | 10 MB     |
| gzip (.gz)   | 397 MB    | 95.2%       | ~5,000 rec/sec  | 10 MB     |
| zstd (.zst)  | 330 MB    | 96.0%       | ~8,000 rec/sec  | 10 MB     |

See [`docs/CompressionAnalysis.md`](docs/CompressionAnalysis.md) for detailed analysis.

## Advanced Topics

### Adapter Pattern

See [`docs/AdapterPattern.md`](docs/AdapterPattern.md) for comprehensive guide on:

- Why use adapters vs direct data addition
- Adapter components (InstrumentProvider, DataClient)
- Message bus routing
- Backtest vs live trading differences
- Best practices and common pitfalls

### Storing in Catalog

The project includes tools to convert raw binary files to efficient Parquet catalogs:

```bash
# Convert all data to catalog (run once)
python convert_to_catalog_direct.py
```

This creates a queryable Parquet catalog at `twse_catalog/`. Query examples:

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.serialization.base import register_serializable_type
from twse_snapshot_data import TWSESnapshotData

# Register serialization
register_serializable_type(
    TWSESnapshotData,
    TWSESnapshotData.to_dict,
    TWSESnapshotData.from_dict
)
register_arrow(
    TWSESnapshotData,
    TWSESnapshotData.schema(),
    TWSESnapshotData.to_catalog,
    TWSESnapshotData.from_catalog
)

# Load catalog
catalog = ParquetDataCatalog('twse_catalog')

# Query all data
all_data = catalog.query(data_cls=TWSESnapshotData)

# Query with filter (WHERE clause)
trades = catalog.query(
    data_cls=TWSESnapshotData,
    where="match_flag = 'Y'"
)

# Query specific instrument
tsmc_data = catalog.query(
    data_cls=TWSESnapshotData,
    where="instrument_id = '2330.TWSE'"
)

# Access the actual data
for item in all_data:
    snapshot = item.data  # Extract TWSESnapshotData from CustomData wrapper
    print(snapshot.instrument_id, snapshot.trade_price)
```

**Direct Pandas Integration:**

```python
import pandas as pd

# Method 1: Convert catalog query results
df = pd.DataFrame([item.data.to_dict() for item in all_data])

# Method 2: Read Parquet directly
df = pd.read_parquet('twse_catalog/data/twse_snapshot_data.parquet')
```

## Known Limitations

- ⚠️ Backtest mode data routing requires further investigation of NautilusTrader's subscription mechanisms
- ⚠️ The adapter pattern works best for live trading; backtesting with custom data needs special handling
- ⚠️ See [`IMPLEMENTATION_NOTES.md`](IMPLEMENTATION_NOTES.md) for detailed discussion

## File Structure

```
NautilusTrader/
├── twse_snapshot_data.py                # Custom data type (5-level order book)
├── twse_data_loader.py                  # Binary file parser
├── twse_adapter/                        # Full adapter implementation
│   ├── __init__.py
│   ├── providers.py                     # TWSEInstrumentProvider
│   └── data.py                          # TWSEDataClient
├── demo_backtest.py                     # Original backtest demo
├── demo_adapter.py                      # Full async adapter demo
├── demo_simple_adapter.py               # Simplified adapter demo
├── convert_to_catalog_direct.py         # Binary → Parquet catalog ⭐
├── convert_to_feather_by_date.py        # Binary → Feather by date ⭐ NEW
├── query_catalog_example.py             # Catalog query examples
├── query_feather_example.py             # Format comparison examples ⭐ NEW
├── docs/
│   ├── CustomData.md                    # NautilusTrader custom data guide
│   └── AdapterPattern.md                # Complete adapter pattern guide
├── twse_catalog/                        # Parquet catalog (after conversion)
├── feather_data/                        # Feather files by date ⭐ NEW
│   └── yyyy-MM-dd/
│       └── instrument_id.feather
├── CATALOG_GUIDE.md                     # Catalog usage guide
├── README.md                            # This file (user guide)
└── IMPLEMENTATION_NOTES.md              # Technical details
```

**⭐ Recommended workflows:**

**For daily backtesting:**
```bash
python convert_to_feather_by_date.py  # Run once
# Then read: feather_data/2024-11-11/0050.TWSE.feather
```

**For complex queries & research:**
```bash
python convert_to_catalog_direct.py  # Run once
# Then query with WHERE clauses
```

## Documentation

### Core Documentation
- [`README.md`](README.md) - This file (user guide)
- [`IMPLEMENTATION_NOTES.md`](IMPLEMENTATION_NOTES.md) - Technical details and implementation notes
- [`COMPRESSION_SUMMARY.md`](COMPRESSION_SUMMARY.md) - Compression analysis summary (中文) ⭐ NEW

### Technical Guides
- [`docs/CustomData.md`](docs/CustomData.md) - NautilusTrader custom data guide
- [`docs/AdapterPattern.md`](docs/AdapterPattern.md) - Complete adapter pattern guide
- [`docs/CompressionAnalysis.md`](docs/CompressionAnalysis.md) - Detailed compression analysis ⭐
- [`docs/CompressionQuickStart.md`](docs/CompressionQuickStart.md) - Compression quick reference ⭐
- [`CATALOG_GUIDE.md`](CATALOG_GUIDE.md) - Parquet catalog usage guide

### Conversion Tools
- [`convert_to_catalog_direct.py`](convert_to_catalog_direct.py) - Binary to Parquet conversion
- [`convert_to_feather_by_date.py`](convert_to_feather_by_date.py) - Binary to Feather conversion

### Query Examples
- [`query_catalog_example.py`](query_catalog_example.py) - Catalog query patterns
- [`query_feather_example.py`](query_feather_example.py) - Format comparison examples

## References

- [NautilusTrader Documentation](https://nautilustrader.io/docs/latest/)
- [NautilusTrader Adapters](https://nautilustrader.io/docs/latest/concepts/adapters)
- [NautilusTrader Custom Data](https://nautilustrader.io/docs/latest/concepts/data#custom-data)
- [TWSE Data Format Specification](../snapshot/README_new.md)
- [NautilusTrader GitHub](https://github.com/nautechsystems/nautilus_trader)

## License

This code is provided as-is for educational purposes.

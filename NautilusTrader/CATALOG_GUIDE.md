# TWSE Data Catalog Guide

## Quick Start

### 1. One-Time Conversion

Convert raw binary files to efficient Parquet catalog:

```bash
cd NautilusTrader
python convert_to_catalog_direct.py
```

**What this does:**
- Parses raw 190-byte binary snapshot files
- Converts to columnar Parquet format
- Creates queryable catalog at `twse_catalog/`
- Typical throughput: ~1,000-2,000 snapshots/sec

### 2. Query Data

Use the catalog in your strategies:

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.serialization.base import register_serializable_type
from twse_snapshot_data import TWSESnapshotData

# Register serialization (once per script)
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
print(f"Total snapshots: {len(all_data)}")

# Access data
for item in all_data[:5]:
    snapshot = item.data  # Extract TWSESnapshotData from CustomData wrapper
    print(f"{snapshot.instrument_id}: {snapshot.trade_price} @ {snapshot.display_time}")
```

## Query Examples

### Filter by Match Flag (Trades Only)

```python
trades = catalog.query(
    data_cls=TWSESnapshotData,
    where="match_flag = 'Y'"
)
print(f"Found {len(trades)} trades")
```

### Query Specific Instrument

```python
tsmc_data = catalog.query(
    data_cls=TWSESnapshotData,
    where="instrument_id = '2330.TWSE'"
)
```

### Query by Time Range

```python
morning_data = catalog.query(
    data_cls=TWSESnapshotData,
    where="display_date = '20241111'"  # Format: YYYYMMDD
)
```

### Complex Filters

```python
# Get trades for TSMC with price > 500
expensive_trades = catalog.query(
    data_cls=TWSESnapshotData,
    where="instrument_id = '2330.TWSE' AND match_flag = 'Y' AND trade_price > 500.0"
)
```

## Pandas Integration

### Method 1: From Catalog Query

```python
import pandas as pd

# Query and convert to DataFrame
all_data = catalog.query(data_cls=TWSESnapshotData)
df = pd.DataFrame([item.data.to_dict() for item in all_data])

# Now you can use pandas operations
print(df.head())
print(df['trade_price'].describe())
print(df.groupby('instrument_id')['trade_volume'].sum())
```

### Method 2: Direct Parquet Read

```python
import pandas as pd

# Read Parquet directly (fastest method)
df = pd.read_parquet('twse_catalog/data/twse_snapshot_data.parquet')

# Filter using pandas
tsmc = df[df['instrument_id'] == '2330.TWSE']
trades = df[df['match_flag'] == 'Y']
```

## Performance Comparison

### Before (Parsing Binary Each Time)

```python
# Load and parse binary file
loader = TWSEDataLoader('snapshot/Sample_new')
snapshots = list(loader.read_records())  # ~1,000 snapshots/sec

# Filter in Python
tsmc_snapshots = [s for s in snapshots if str(s.instrument_id) == '2330.TWSE']
```

**Time for 50M records:** ~14 hours of parsing

### After (Using Catalog)

```python
# Query from catalog with predicate pushdown
tsmc_data = catalog.query(
    data_cls=TWSESnapshotData,
    where="instrument_id = '2330.TWSE'"  # Filtered at storage layer
)
```

**Time for 50M records:** Seconds (only reads matching rows)

## Workflow Integration

### For Backtesting

```python
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.persistence.catalog import ParquetDataCatalog

# Load data from catalog
catalog = ParquetDataCatalog('twse_catalog')
all_data = catalog.query(data_cls=TWSESnapshotData)

# Use in backtest
engine = BacktestEngine(config=config)
# ... setup venue, instruments ...

for item in all_data:
    engine.add_data([item.data])

engine.run()
```

### For Strategy Development

```python
from nautilus_trader.trading.strategy import Strategy

class MyStrategy(Strategy):
    def on_start(self):
        # Load historical data for analysis
        catalog = ParquetDataCatalog('twse_catalog')
        
        # Get data for your instruments
        historical = catalog.query(
            data_cls=TWSESnapshotData,
            where="instrument_id = '2330.TWSE'"
        )
        
        # Analyze and set strategy parameters
        self.calculate_indicators(historical)
    
    def calculate_indicators(self, data):
        # Process historical data
        for item in data:
            snapshot = item.data
            # ... your analysis ...
```

## Data Schema

The Parquet catalog stores data with flattened structure:

| Column | Type | Description |
|--------|------|-------------|
| instrument_id | string | Security ID (e.g., "2330.TWSE") |
| display_time | string | Time in HHMMSSMS format |
| display_date | string | Date in YYYYMMDD format |
| match_flag | string | 'Y' for trades, ' ' for updates |
| trade_price | float | Last trade price (price * 100) |
| trade_volume | int | Trade volume |
| buy_price_1 to buy_price_5 | float | Bid prices (5 levels) |
| buy_volume_1 to buy_volume_5 | int | Bid volumes (5 levels) |
| sell_price_1 to sell_price_5 | float | Ask prices (5 levels) |
| sell_volume_1 to sell_volume_5 | int | Ask volumes (5 levels) |
| ts_event | int64 | Event timestamp (nanoseconds) |
| ts_init | int64 | Init timestamp (nanoseconds) |

## Best Practices

### 1. Register Serialization Once

```python
# At the top of your script
from twse_snapshot_data import TWSESnapshotData
from nautilus_trader.serialization.arrow.serializer import register_arrow
from nautilus_trader.serialization.base import register_serializable_type

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
```

### 2. Use WHERE Clauses for Filtering

```python
# ✅ Good: Filter at storage layer
data = catalog.query(
    data_cls=TWSESnapshotData,
    where="instrument_id = '2330.TWSE' AND match_flag = 'Y'"
)

# ❌ Bad: Load all then filter in Python
all_data = catalog.query(data_cls=TWSESnapshotData)
data = [d for d in all_data if d.data.match_flag == 'Y']
```

### 3. Access Data Correctly

```python
# Query returns CustomData wrappers
results = catalog.query(data_cls=TWSESnapshotData)

# Access the actual TWSESnapshotData
for item in results:
    snapshot = item.data  # Extract from wrapper
    print(snapshot.instrument_id, snapshot.trade_price)
```

### 4. Batch Processing

```python
# For large datasets, process in batches
def process_batch(snapshots):
    for item in snapshots:
        # Process snapshot
        pass

# Process in chunks
chunk_size = 10000
all_data = catalog.query(data_cls=TWSESnapshotData)

for i in range(0, len(all_data), chunk_size):
    batch = all_data[i:i+chunk_size]
    process_batch(batch)
```

## Troubleshooting

### Catalog Not Found

```
❌ Catalog not found at: twse_catalog
```

**Solution:** Run `convert_to_catalog_direct.py` first to create the catalog.

### Empty Results

```python
data = catalog.query(data_cls=TWSESnapshotData)
print(len(data))  # 0
```

**Possible causes:**
1. Catalog is empty (check `twse_catalog/data/` directory)
2. Data class not registered (call `register_serializable_type` and `register_arrow`)
3. Wrong data class specified

### AttributeError on Query Results

```
AttributeError: 'nautilus_trader.model.data.CustomData' object has no attribute 'instrument_id'
```

**Solution:** Access the `.data` attribute first:

```python
# ❌ Wrong
for item in results:
    print(item.instrument_id)  # CustomData doesn't have this

# ✅ Correct
for item in results:
    snapshot = item.data  # Get TWSESnapshotData
    print(snapshot.instrument_id)
```

## Advanced Usage

### PyArrow Direct Access

```python
import pyarrow.parquet as pq

# Read Parquet table directly
table = pq.read_table('twse_catalog/data/twse_snapshot_data.parquet')

# Use PyArrow compute functions
import pyarrow.compute as pc
filtered = table.filter(pc.field('trade_price') > 500)

# Convert to pandas
df = filtered.to_pandas()
```

### Custom Aggregations

```python
import pandas as pd

# Load data
df = pd.read_parquet('twse_catalog/data/twse_snapshot_data.parquet')

# Calculate VWAP by instrument
vwap = df.groupby('instrument_id').apply(
    lambda x: (x['trade_price'] * x['trade_volume']).sum() / x['trade_volume'].sum()
)

# Find best bid-ask spread
df['spread'] = df['sell_price_1'] - df['buy_price_1']
best_spread = df.groupby('instrument_id')['spread'].min()
```

## Summary

**Key Benefits:**
- 🚀 **Fast**: Columnar format enables efficient queries
- 💾 **Compressed**: Parquet compression reduces storage by ~50-90%
- 🔍 **Queryable**: SQL-like WHERE clauses
- 🐼 **Pandas-ready**: Direct integration with pandas/polars
- ♻️ **Reusable**: Convert once, query many times

**Recommended for:**
- Strategy backtesting with large datasets
- Historical data analysis
- Research and exploration
- Production trading systems

**See also:**
- [convert_to_catalog_direct.py](convert_to_catalog_direct.py) - Conversion script
- [query_catalog_example.py](query_catalog_example.py) - More query examples
- [NautilusTrader Data Catalog Documentation](https://nautilustrader.io/docs/latest/concepts/data#data-catalog)


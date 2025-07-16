# TWSE Snapshot Parser v2 (data_v2.py)

A simplified and focused parser for TWSE (Taiwan Stock Exchange) snapshot data, specifically designed for processing DSP (揭示檔) files.

## 🎯 Features

- **Dual Format Support**: Handles both 186-byte (old) and 190-byte (new) snapshot formats automatically
- **Flattened Structure**: Converts 5-level order book data into separate bid/ask price/volume columns
- **Unified DateTime**: Combines display date and time into a single datetime column
- **Categorical Optimization**: Converts flag fields to categorical data type for memory efficiency
- **Organized Output**: Saves data in `date/securities_code.parquet` structure for easy querying
- **Simple API**: Clean, focused interface with only essential functionality

## 🏗️ Architecture

```python
class SnapshotParser:
    - parse_snapshot_line()    # Parse single line (186 or 190 bytes)
    - flatten_snapshot()       # Convert 5-level data to flat structure
    - load_dsp_file()         # Load entire DSP file to DataFrame
    - save_by_securities()    # Save with date/securities organization
```

## 🚀 Quick Start

```python
from data_v2 import SnapshotParser

# Initialize parser
parser = SnapshotParser()

# Load snapshot data (auto-detects 186 or 190 byte format)
df = parser.load_dsp_file("snapshot/Sample_new")

# Save organized by date and securities
parser.save_by_securities(df, "output/processed")
```

## 📊 Data Structure

### Input Formats

| Format | Bytes | Schema | Example File |
|--------|-------|---------|--------------|
| Old | 186 | Legacy format | `snapshot/Sample` |
| New | 190 | Current format | `snapshot/Sample_new` |

### Output Schema

The parser converts raw snapshot data into a flattened DataFrame with these columns:

#### Basic Fields
- `securities_code` (str): Security identifier (e.g., "0050", "9958")
- `datetime` (datetime): Combined display date and time
- `trade_price` (float): Last trade price (divided by 100)
- `transaction_volume` (int): Number of shares traded

#### Flag Fields (Categorical)
- `remark` (category): Display remark (' ', 'T', 'S')
- `trend_flag` (category): Trend flag (' ', 'R', 'F', 'C')  
- `match_flag` (category): Match flag (' ', 'Y', 'S')
- `trade_upper_lower_limit` (category): Price limit flag
- `buy_upper_lower_limit` (category): Buy limit flag
- `sell_upper_lower_limit` (category): Sell limit flag

#### Market Depth (Flattened 5-Level)
- `bid_price_1` to `bid_price_5` (float): Buy prices (levels 1-5)
- `bid_volume_1` to `bid_volume_5` (int): Buy volumes (levels 1-5)
- `ask_price_1` to `ask_price_5` (float): Sell prices (levels 1-5)  
- `ask_volume_1` to `ask_volume_5` (int): Sell volumes (levels 1-5)

#### Technical Fields
- `buy_tick_size`, `sell_tick_size` (int): Number of price levels
- `match_staff` (category): Market maker identifier

## 📁 Output Structure

```
output/
├── processed/
│   ├── 2024-11-11/           # Date-based directories
│   │   ├── 0050.parquet      # Individual security files
│   │   ├── 9958.parquet
│   │   └── ...
│   ├── 2024-11-12/
│   │   ├── 0050.parquet
│   │   └── ...
│   └── ...
```

## 🔧 Usage Examples

### Basic Usage

```python
from data_v2 import SnapshotParser

parser = SnapshotParser()

# Load new format (190 bytes)
df_new = parser.load_dsp_file("snapshot/Sample_new")
print(f"Loaded {len(df_new)} records")
print(f"Securities: {df_new['securities_code'].unique()}")

# Load old format (186 bytes) 
df_old = parser.load_dsp_file("snapshot/Sample")

# Save with organized structure
parser.save_by_securities(df_new, "output/new_format")
parser.save_by_securities(df_old, "output/old_format")
```

### Advanced Options

```python
# Disable categorical conversion for flag fields
df = parser.load_dsp_file("snapshot/Sample_new", use_categorical=False)

# Check data quality
print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
print(f"Price range: {df['trade_price'].min()} to {df['trade_price'].max()}")
print(f"Volume range: {df['transaction_volume'].min()} to {df['transaction_volume'].max()}")
```

### Data Analysis

```python
import pandas as pd

# Load specific security and date
df = pd.read_parquet("output/processed/2024-11-11/0050.parquet")

# Analyze bid-ask spread
df['spread'] = df['ask_price_1'] - df['bid_price_1']
print(f"Average spread: {df['spread'].mean():.4f}")

# Market depth analysis
total_bid_volume = df[['bid_volume_1', 'bid_volume_2', 'bid_volume_3', 
                      'bid_volume_4', 'bid_volume_5']].sum(axis=1)
print(f"Average bid depth: {total_bid_volume.mean():.0f} shares")
```

## 🔄 Migration from data.py

### Key Differences

| Feature | data.py | data_v2.py |
|---------|---------|------------|
| **Scope** | Full suite (Order, Transaction, Snapshot) | Snapshot only |
| **Complexity** | Complex with multiple models | Simple single class |
| **Dependencies** | Pydantic, struct, tqdm | pandas, datetime, pathlib |
| **Output** | Raw + typed models | Direct DataFrame |
| **File Organization** | Manual grouping | Automatic date/securities structure |

### Migration Steps

```python
# Old way (data.py)
from data import TwseTickParser
parser = TwseTickParser(raw=False)
records = parser.load_dsp_file("snapshot/Sample_new", flatten=True)
df = pd.DataFrame(records)

# New way (data_v2.py)  
from data_v2 import SnapshotParser
parser = SnapshotParser()
df = parser.load_dsp_file("snapshot/Sample_new")
parser.save_by_securities(df, "output")
```

## ⚡ Performance & Benefits

### Performance Characteristics
- **Memory Efficient**: Categorical encoding for flag fields
- **Fast I/O**: Direct pandas DataFrame creation
- **Compressed Storage**: Parquet format with efficient types

### Storage Benefits
- **Organized Access**: Quick access by date and security
- **Columnar Format**: Efficient for analytical queries
- **Type Optimization**: Proper data types reduce storage size

### Example Performance
```
Input:  40 records (190 bytes/record) = 7.6 KB raw
Output: 2 files × ~12 KB = 24 KB parquet (compressed)
        33 columns including flattened market depth
```

## 🧪 Testing

Run the included test script:

```bash
python test_data_v2.py
```

The test will:
1. Test individual parsing functions
2. Process both format types (if files exist)  
3. Create organized output structure
4. Display sample results and statistics

## 🔍 Troubleshooting

### Common Issues

1. **File Not Found**: Ensure snapshot files exist in `snapshot/` directory
2. **Encoding Errors**: Files should be binary, parser handles UTF-8 decoding
3. **Line Length**: Only 186 and 190 byte lines are processed, others skipped
4. **Memory Usage**: Large files are processed line-by-line for efficiency

### Data Validation

```python
# Check for data quality issues
def validate_data(df):
    print(f"Missing values: {df.isnull().sum().sum()}")
    print(f"Negative prices: {(df['trade_price'] < 0).sum()}")
    print(f"Zero bid prices: {(df['bid_price_1'] == 0).sum()}")
    print(f"DateTime range: {df['datetime'].min()} to {df['datetime'].max()}")
```

## 📝 Notes

- **Linter Warning**: There may be a harmless linter warning about GroupBy iteration that can be ignored
- **Time Precision**: New format supports microsecond precision, old format second precision
- **Price Scaling**: All prices are automatically divided by 100 (e.g., 002900 → 29.00)
- **Volume Units**: Transaction volume is in shares, bid/ask volumes in "zhang" (1000 shares)

## 🎯 Future Enhancements

Potential improvements for future versions:
- Gzip file support for compressed inputs
- Batch processing for multiple files
- Data validation and quality checks
- Memory optimization for very large files
- Integration with time-series databases 
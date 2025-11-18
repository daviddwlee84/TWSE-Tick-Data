"""
Convert TWSE Snapshot Binary to Feather by Date and Instrument.

This script organizes data in a date/instrument hierarchy:
    custom_data_root/yyyy-MM-dd/instrument_id.feather

This format is ideal for:
- Daily backtesting
- Per-instrument analysis
- Direct pandas reading
- Simple file organization

Format: custom_data_root/2024-11-11/0050.TWSE.feather
"""

from pathlib import Path
import time
from collections import defaultdict
import pandas as pd

from twse_data_loader import TWSEDataLoader


def convert_binary_to_feather(
    data_file: Path,
    output_root: Path,
    limit: int | None = None,
) -> dict:
    """
    Convert TWSE snapshot binary data to feather files organized by date/instrument.

    Parameters
    ----------
    data_file : Path
        Path to the raw TWSE snapshot binary file
    output_root : Path
        Root directory for feather files
    limit : int, optional
        Maximum number of records to process

    Returns
    -------
    dict
        Statistics about the conversion (files written, records per file, etc.)
    """
    print(f"\n{'='*80}")
    print("Converting Binary to Feather by Date/Instrument")
    print(f"{'='*80}\n")

    # Create output directory
    output_root.mkdir(parents=True, exist_ok=True)
    print(f"Output root: {output_root}")

    # Load data
    print(f"Reading data from: {data_file}")
    loader = TWSEDataLoader(data_file)

    # Group snapshots by date and instrument
    print(f"⏳ Parsing and grouping data...\n")
    start_time = time.time()

    # Structure: {date: {instrument_id: [snapshots]}}
    grouped_data = defaultdict(lambda: defaultdict(list))
    total_records = 0

    for snapshot in loader.read_records(limit=limit):
        date = snapshot.display_date  # YYYYMMDD format
        instrument_id = str(snapshot.instrument_id)

        # Convert to dict for pandas (with flattened order book)
        data_dict = snapshot.to_dict()

        # Flatten order book levels (same as Parquet schema)
        for i in range(5):
            if i < len(snapshot.buy_levels):
                data_dict[f"buy_price_{i+1}"] = snapshot.buy_levels[i].price
                data_dict[f"buy_volume_{i+1}"] = snapshot.buy_levels[i].volume
            else:
                data_dict[f"buy_price_{i+1}"] = 0.0
                data_dict[f"buy_volume_{i+1}"] = 0

        for i in range(5):
            if i < len(snapshot.sell_levels):
                data_dict[f"sell_price_{i+1}"] = snapshot.sell_levels[i].price
                data_dict[f"sell_volume_{i+1}"] = snapshot.sell_levels[i].volume
            else:
                data_dict[f"sell_price_{i+1}"] = 0.0
                data_dict[f"sell_volume_{i+1}"] = 0

        # Remove nested structures
        del data_dict["buy_levels"]
        del data_dict["sell_levels"]

        grouped_data[date][instrument_id].append(data_dict)

        total_records += 1

        if total_records % 1000 == 0:
            print(f"  Processed {total_records} snapshots...")

    elapsed_parse = time.time() - start_time
    print(f"\n✓ Parsed {total_records} snapshots in {elapsed_parse:.2f}s")
    print(f"  Throughput: {total_records/elapsed_parse:.0f} snapshots/sec")

    # Write feather files
    print(f"\n⏳ Writing feather files...\n")
    start_time = time.time()

    files_written = 0
    stats = {
        "total_records": total_records,
        "dates": len(grouped_data),
        "files": {},
        "dates_processed": [],
    }

    for date, instruments in sorted(grouped_data.items()):
        # Format date: YYYYMMDD -> YYYY-MM-DD
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        date_dir = output_root / formatted_date
        date_dir.mkdir(parents=True, exist_ok=True)

        stats["dates_processed"].append(formatted_date)

        for instrument_id, snapshots in instruments.items():
            # Convert to DataFrame
            df = pd.DataFrame(snapshots)

            # Write to feather
            output_file = date_dir / f"{instrument_id}.feather"
            df.to_feather(output_file)

            files_written += 1
            stats["files"][str(output_file.relative_to(output_root))] = len(snapshots)

            print(
                f"  ✓ {formatted_date}/{instrument_id}.feather: {len(snapshots)} snapshots"
            )

    elapsed_write = time.time() - start_time
    print(f"\n✓ Wrote {files_written} feather files in {elapsed_write:.2f}s")

    total_elapsed = elapsed_parse + elapsed_write
    print(f"\n{'='*80}")
    print("Conversion Summary")
    print(f"{'='*80}")
    print(f"Total records: {total_records}")
    print(f"Total time: {total_elapsed:.2f}s")
    print(f"Overall throughput: {total_records/total_elapsed:.0f} snapshots/sec")
    print(f"Files written: {files_written}")
    print(f"Dates covered: {len(grouped_data)}")

    return stats


def verify_feather_files(output_root: Path) -> None:
    """
    Verify feather files and show structure.

    Parameters
    ----------
    output_root : Path
        Root directory containing feather files
    """
    print(f"\n{'='*80}")
    print("Verifying Feather Files")
    print(f"{'='*80}\n")

    if not output_root.exists():
        print(f"❌ Output directory not found: {output_root}")
        return

    # Find all feather files
    feather_files = list(output_root.rglob("*.feather"))

    if not feather_files:
        print(f"⚠️  No feather files found in {output_root}")
        return

    print(f"Found {len(feather_files)} feather files\n")

    # Group by date
    dates = defaultdict(list)
    for file in feather_files:
        date = file.parent.name
        dates[date].append(file.name)

    print("Directory structure:")
    print(f"{output_root.name}/")
    for date in sorted(dates.keys()):
        print(f"  {date}/")
        for filename in sorted(dates[date]):
            file_path = output_root / date / filename
            df = pd.read_feather(file_path)
            print(f"    {filename} ({len(df)} rows)")

    # Show example data
    if feather_files:
        example_file = feather_files[0]
        print(f"\nExample data from: {example_file.relative_to(output_root)}")
        df = pd.read_feather(example_file)
        print(df.head())
        print(f"\nColumns: {list(df.columns)}")
        print(f"Shape: {df.shape}")


def show_usage_examples(output_root: Path) -> None:
    """
    Show how to read the feather files.

    Parameters
    ----------
    output_root : Path
        Root directory containing feather files
    """
    print(f"\n{'='*80}")
    print("Usage Examples")
    print(f"{'='*80}\n")

    print("Read data for a specific date and instrument:")
    print("```python")
    print("import pandas as pd")
    print("from pathlib import Path")
    print()
    print(f"data_root = Path('{output_root}')")
    print()
    print("# Read specific instrument on specific date")
    print("df = pd.read_feather(data_root / '2024-11-11' / '0050.TWSE.feather')")
    print("print(df.head())")
    print()
    print("# Read all instruments for a date")
    print("date = '2024-11-11'")
    print("date_dir = data_root / date")
    print("dfs = {}")
    print("for feather_file in date_dir.glob('*.feather'):")
    print("    instrument_id = feather_file.stem")
    print("    dfs[instrument_id] = pd.read_feather(feather_file)")
    print()
    print("# Concatenate all instruments for a date")
    print("daily_df = pd.concat(dfs.values(), ignore_index=True)")
    print("```")

    print("\n\nRead data for a specific instrument across multiple dates:")
    print("```python")
    print("# Read 0050.TWSE across all dates")
    print("instrument_id = '0050.TWSE'")
    print("dfs = []")
    print("for date_dir in sorted(data_root.iterdir()):")
    print("    if date_dir.is_dir():")
    print("        feather_file = date_dir / f'{instrument_id}.feather'")
    print("        if feather_file.exists():")
    print("            df = pd.read_feather(feather_file)")
    print("            dfs.append(df)")
    print()
    print("# Combine all dates")
    print("full_df = pd.concat(dfs, ignore_index=True)")
    print("full_df = full_df.sort_values('ts_event')")
    print("```")

    print("\n\nAnalyze order book depth:")
    print("```python")
    print("df = pd.read_feather(data_root / '2024-11-11' / '0050.TWSE.feather')")
    print()
    print("# Best bid/ask prices")
    print("df['best_bid'] = df['buy_price_1']")
    print("df['best_ask'] = df['sell_price_1']")
    print("df['spread'] = df['best_ask'] - df['best_bid']")
    print("df['spread_bps'] = (df['spread'] / df['best_bid']) * 10000")
    print()
    print("print('Average spread:', df['spread'].mean())")
    print("print('Average spread (bps):', df['spread_bps'].mean())")
    print("```")


def main():
    """
    Main workflow: Binary → Feather by Date/Instrument
    """
    print(f"\n{'='*80}")
    print("TWSE Snapshot Data: Binary to Feather by Date/Instrument")
    print(f"{'='*80}\n")
    print("Format: custom_data_root/yyyy-MM-dd/instrument_id.feather")

    # Configuration
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    output_root = Path(__file__).parent / "feather_data"

    # Process settings
    limit = None  # Set to None to process all records

    if not data_file.exists():
        print(f"\n❌ Error: Data file not found: {data_file}")
        return

    # Step 1: Convert
    stats = convert_binary_to_feather(data_file, output_root, limit=limit)

    # Step 2: Verify
    verify_feather_files(output_root)

    # Step 3: Show usage
    show_usage_examples(output_root)

    print(f"\n{'='*80}")
    print("✓ Conversion Complete!")
    print(f"{'='*80}\n")
    print(f"Data saved to: {output_root.absolute()}")
    print(f"Total files: {len(stats['files'])}")
    print(f"Dates: {', '.join(stats['dates_processed'])}")
    print(f"\nRun query_feather_example.py to see more usage examples")


if __name__ == "__main__":
    main()

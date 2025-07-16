import pandas as pd
import datetime
from pathlib import Path
from typing import List, Dict, Any, Iterator, Union, cast
from tqdm.auto import tqdm
import gzip, io

class SnapshotParser:
    """Simplified TWSE Snapshot parser focusing on core functionality."""
    
    def __init__(self):
        self.categorical_fields = ['remark', 'trend_flag', 'match_flag', 
                                 'trade_upper_lower_limit', 'buy_upper_lower_limit', 
                                 'sell_upper_lower_limit']
    
    @staticmethod
    def parse_6digit_price(value: str) -> float:
        """Convert 6-digit string to price with 2 decimals (e.g. "002900" -> 29.0)"""
        return int(value.strip()) / 100.0 if value.strip() else 0.0
    
    @staticmethod
    def parse_time_hhmmss(value: str) -> datetime.time:
        """Parse HHMMSS time string"""
        value = value.strip()[:6]  # Take first 6 digits
        if len(value) < 6:
            value = value.ljust(6, '0')
        hh, mm, ss = int(value[0:2]), int(value[2:4]), int(value[4:6])
        return datetime.time(hour=hh, minute=mm, second=ss)
    
    @staticmethod
    def parse_date_yyyymmdd(value: str) -> datetime.date:
        """Parse YYYYMMDD date string"""
        return datetime.datetime.strptime(value.strip(), "%Y%m%d").date()
    
    @staticmethod
    def parse_5_price_volume(value: str) -> List[Dict[str, float]]:
        """Parse 70-char field containing 5 pairs of (price(6), volume(8))"""
        pairs = []
        if len(value) != 70:
            return [{'price': 0.0, 'volume': 0} for _ in range(5)]
        
        for i in range(5):
            start = i * 14
            price_str = value[start:start + 6]
            volume_str = value[start + 6:start + 14]
            
            price = SnapshotParser.parse_6digit_price(price_str)
            volume = int(volume_str) if volume_str.strip() else 0
            pairs.append({'price': price, 'volume': volume})
        
        return pairs
    
    def parse_snapshot_line(self, line: str) -> Dict[str, Any]:
        """Parse a single snapshot line (handles both 186 and 190 byte formats)"""
        line_len = len(line)
        
        if line_len == 190:
            # New format (190 bytes)
            record = {
                'securities_code': line[0:6].strip(),
                'display_time_raw': line[6:18],  # 12 bytes
                'remark': line[18:19],
                'trend_flag': line[19:20],
                'match_flag': line[20:21],
                'trade_upper_lower_limit': line[21:22],
                'trade_price': self.parse_6digit_price(line[22:28]),
                'transaction_volume': int(line[28:36]) if line[28:36].strip() else 0,
                'buy_tick_size': int(line[36:37]) if line[36:37].strip() else 0,
                'buy_upper_lower_limit': line[37:38],
                'buy_5_price_volume': self.parse_5_price_volume(line[38:108]),
                'sell_tick_size': int(line[108:109]) if line[108:109].strip() else 0,
                'sell_upper_lower_limit': line[109:110],
                'sell_5_price_volume': self.parse_5_price_volume(line[110:180]),
                'display_date_raw': line[180:188],
                'match_staff': line[188:190]
            }
            # For new format, take first 6 digits of 12-byte time field
            time_str = record['display_time_raw'][:6]
        
        elif line_len == 186:
            # Old format (186 bytes)
            record = {
                'securities_code': line[0:6].strip(),
                'display_time_raw': line[6:14],  # 8 bytes
                'remark': line[14:15],
                'trend_flag': line[15:16],
                'match_flag': line[16:17],
                'trade_upper_lower_limit': line[17:18],
                'trade_price': self.parse_6digit_price(line[18:24]),
                'transaction_volume': int(line[24:32]) if line[24:32].strip() else 0,
                'buy_tick_size': int(line[32:33]) if line[32:33].strip() else 0,
                'buy_upper_lower_limit': line[33:34],
                'buy_5_price_volume': self.parse_5_price_volume(line[34:104]),
                'sell_tick_size': int(line[104:105]) if line[104:105].strip() else 0,
                'sell_upper_lower_limit': line[105:106],
                'sell_5_price_volume': self.parse_5_price_volume(line[106:176]),
                'display_date_raw': line[176:184],
                'match_staff': line[184:186]
            }
            time_str = record['display_time_raw']
        
        else:
            raise ValueError(f"Unexpected line length: {line_len}. Expected 186 or 190 bytes.")
        
        # Parse date and time, then combine
        display_date = self.parse_date_yyyymmdd(record['display_date_raw'])
        display_time = self.parse_time_hhmmss(time_str)
        record['datetime'] = datetime.datetime.combine(display_date, display_time)
        
        # Remove raw date/time fields
        del record['display_date_raw']
        del record['display_time_raw']
        
        return record
    
    def flatten_snapshot(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten 5-level price/volume data into separate columns"""
        flattened = record.copy()
        
        # Remove original nested data
        buy_data = flattened.pop('buy_5_price_volume', [])
        sell_data = flattened.pop('sell_5_price_volume', [])
        
        # Flatten buy data (bid)
        for i, pair in enumerate(buy_data, 1):
            flattened[f'bid_price_{i}'] = pair['price']
            flattened[f'bid_volume_{i}'] = pair['volume']
        
        # Flatten sell data (ask)  
        for i, pair in enumerate(sell_data, 1):
            flattened[f'ask_price_{i}'] = pair['price']
            flattened[f'ask_volume_{i}'] = pair['volume']
        
        return flattened
    
    def load_dsp_file(self, filepath: str, use_categorical: bool = True) -> pd.DataFrame:
        """Load and parse a DSP file.

        Parameters
        ----------
        filepath : str
            Path to the DSP file (supports optional ``.gz`` compression).
        use_categorical : bool, default True
            Convert flag fields to ``category`` dtype for memory efficiency.

        Returns
        -------
        pandas.DataFrame
            Parsed snapshot data **OR** an *iterator* of DataFrames when
            ``chunk_size`` is specified (see below).
        """

        # We know chunk_size=None → returns DataFrame
        return cast(pd.DataFrame, self.load_dsp_file_lazy(
            filepath,
            use_categorical=use_categorical,
            chunk_size=None,
        ))

    # ------------------------------------------------------------------
    # Lazy / chunked reader ------------------------------------------------
    # ------------------------------------------------------------------

    def load_dsp_file_lazy(
        self,
        filepath: str,
        *,
        use_categorical: bool = True,
        chunk_size: int | None = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Load a DSP file lazily (optionally in chunks).

        If ``chunk_size`` is ``None`` (default) → returns a single DataFrame.
        If ``chunk_size`` is an integer → returns an *iterator* that yields
        DataFrames of at most ``chunk_size`` rows.  This allows you to process
        very large files without loading the entire dataset in memory.
        """

        def _open_file(path: str):
            """Helper: open plain or gzip file in binary mode."""
            return gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")

        def _apply_categorical(df: pd.DataFrame):
            if use_categorical:
                for field in self.categorical_fields:
                    if field in df.columns:
                        df[field] = df[field].astype("category")
            return df

        if chunk_size is None:
            # ------------------------------------------------------------
            # Non-lazy: return a single DataFrame (previous behaviour)
            # ------------------------------------------------------------
            records: list[dict] = []
            with _open_file(filepath) as f:
                for raw_line in tqdm(f, desc="Reading DSP file"):
                    line = raw_line.rstrip(b"\r\n")
                    if len(line) not in {186, 190}:
                        continue
                    decoded_line = line.decode("utf-8", errors="replace")
                    record = self.parse_snapshot_line(decoded_line)
                    records.append(self.flatten_snapshot(record))

            df = _apply_categorical(pd.DataFrame(records))
            return df

        # ------------------------------------------------------------
        # Lazy mode: yield DataFrame chunks
        # ------------------------------------------------------------

        def _chunk_generator() -> Iterator[pd.DataFrame]:
            chunk_records: list[dict] = []
            with _open_file(filepath) as f:
                for raw_line in tqdm(f, desc="Reading DSP file (lazy)"):
                    line = raw_line.rstrip(b"\r\n")
                    if len(line) not in {186, 190}:
                        continue
                    decoded_line = line.decode("utf-8", errors="replace")
                    record = self.parse_snapshot_line(decoded_line)
                    chunk_records.append(self.flatten_snapshot(record))

                    if len(chunk_records) >= chunk_size:
                        df_chunk = _apply_categorical(pd.DataFrame(chunk_records))
                        yield df_chunk
                        chunk_records.clear()

                # Yield any remainder
                if chunk_records:
                    yield _apply_categorical(pd.DataFrame(chunk_records))

        return _chunk_generator()
    
    def save_by_securities(self, df: pd.DataFrame, output_dir: str) -> None:
        """Save DataFrame grouped by securities_code in date/securities_code.parquet structure"""
        output_path = Path(output_dir)
        
        # Add date column for grouping
        df_with_date = df.copy()
        df_with_date['date'] = df_with_date['datetime'].dt.date
        
        grouped = df_with_date.groupby(['date', 'securities_code'])

        print("Saving grouped parquet files...")

        for name, group_df in grouped:  # name is a tuple (date, securities_code)
            date_str, securities_code = cast(tuple, name)

            # Create directory structure: output_dir/YYYY-MM-DD/
            date_dir = output_path / str(date_str)
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # Remove the temporary date column before saving
            group_df_clean = group_df.drop('date', axis=1)
            
            # Save as securities_code.parquet
            file_path = date_dir / f"{securities_code}.parquet"
            group_df_clean.to_parquet(file_path, index=False)
            
        print(f"Data saved to {output_path}")


def main():
    """Example usage"""
    parser = SnapshotParser()
    
    # Load and process new format (190 bytes)
    print("Processing new format (190 bytes)...")
    df_new = parser.load_dsp_file("snapshot/Sample_new")
    print(f"Loaded {len(df_new)} records")
    print(f"Columns: {list(df_new.columns)}")
    print(f"Date range: {df_new['datetime'].min()} to {df_new['datetime'].max()}")
    print(f"Securities: {df_new['securities_code'].unique()}")
    
    # Save grouped by date/securities
    parser.save_by_securities(df_new, "output/new_format")
    
    # Load and process old format (186 bytes)
    print("\nProcessing old format (186 bytes)...")
    df_old = parser.load_dsp_file("snapshot/Sample")
    print(f"Loaded {len(df_old)} records")
    
    # Save grouped by date/securities
    parser.save_by_securities(df_old, "output/old_format")


if __name__ == "__main__":
    main()

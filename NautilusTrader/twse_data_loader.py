"""
TWSE Data Loader for reading raw binary snapshot files.

This module provides functionality to read and parse TWSE snapshot data
from fixed-width binary files (190 bytes per record).

Supports compression formats:
- gzip (.gz) - built-in Python support
- Zstandard (.zst) - requires 'zstandard' package
"""

from datetime import datetime, timezone
from typing import Iterator, Optional
from pathlib import Path
import struct
import gzip

try:
    # Try Python 3.14+ compression.zstd or backports.zstd
    import sys

    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
    ZSTD_AVAILABLE = True
except ImportError:
    try:
        # Fallback to zstandard package
        import zstandard as zstd

        ZSTD_AVAILABLE = True
    except ImportError:
        ZSTD_AVAILABLE = False
        zstd = None

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.core.datetime import dt_to_unix_nanos, unix_nanos_to_dt
from twse_snapshot_data import TWSESnapshotData, OrderBookLevel


class TWSESnapshotParser:
    """
    Parser for TWSE snapshot data files.

    File format: 190 bytes per record
    - Securities Code: 1-6 (6 bytes)
    - Display Time: 7-18 (12 bytes)
    - Remark: 19 (1 byte)
    - Trend Flag: 20 (1 byte)
    - Match Flag: 21 (1 byte)
    - Trade Upper/Lower Limit: 22 (1 byte)
    - Trade Price: 23-28 (6 bytes)
    - Trade Volume: 29-36 (8 bytes)
    - Buy Tick Size: 37 (1 byte)
    - Buy Upper/Lower Limit: 38 (1 byte)
    - 5 Buy Levels (price + volume): 39-108 (70 bytes, 14 bytes each)
    - Sell Tick Size: 109 (1 byte)
    - Sell Upper/Lower Limit: 110 (1 byte)
    - 5 Sell Levels (price + volume): 111-180 (70 bytes, 14 bytes each)
    - Display Date: 181-188 (8 bytes)
    - Match Staff: 189-190 (2 bytes)
    """

    RECORD_LENGTH = 190
    RECORD_LENGTH_WITH_NEWLINE = 191  # Some files have \n at the end

    @staticmethod
    def parse_price(price_str: str) -> float:
        """Parse price from string (format: 9(06), represents price * 100)."""
        try:
            price_int = int(price_str.strip())
            return price_int / 100.0
        except ValueError:
            return 0.0

    @staticmethod
    def parse_volume(volume_str: str) -> int:
        """Parse volume from string (format: 9(08), in lots)."""
        try:
            return int(volume_str.strip())
        except ValueError:
            return 0

    @staticmethod
    def parse_time(date_str: str, time_str: str) -> int:
        """
        Parse date and time to UNIX nanoseconds.

        Parameters
        ----------
        date_str : str
            Date in format YYYYMMDD
        time_str : str
            Time in format HHMMSSMS (12 chars: HH MM SS MSMSMSMS)

        Returns
        -------
        int
            UNIX timestamp in nanoseconds
        """
        try:
            # Parse date: YYYYMMDD
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            # Parse time: HHMMSSMS (12 chars)
            # HH: 0-1, MM: 2-3, SS: 4-5, MSMSMSMS: 6-11 (microseconds)
            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])
            # Last 6 digits are microseconds (MSMSMSMS in the doc, but actually microseconds)
            microsecond = int(time_str[6:12])

            dt = datetime(
                year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc
            )
            return dt_to_unix_nanos(dt)
        except (ValueError, IndexError) as e:
            # If parsing fails, return current time
            return dt_to_unix_nanos(datetime.now(timezone.utc))

    @classmethod
    def parse_record(cls, record: bytes) -> Optional[TWSESnapshotData]:
        """
        Parse a single 190-byte record.

        Parameters
        ----------
        record : bytes
            Raw 190-byte record (may include trailing newline)

        Returns
        -------
        TWSESnapshotData or None
            Parsed snapshot data or None if parsing fails
        """
        # Handle both 190-byte and 191-byte (with newline) records
        if len(record) == cls.RECORD_LENGTH_WITH_NEWLINE:
            record = record[: cls.RECORD_LENGTH]  # Strip newline
        elif len(record) != cls.RECORD_LENGTH:
            return None

        try:
            # Decode as ASCII
            line = record.decode("ascii", errors="ignore")

            # Parse fields according to schema (positions are 1-based in docs, so subtract 1)
            # Position 1-6 (index 0-5): Securities Code
            securities_code = line[0:6].strip()
            # Position 7-18 (index 6-17): Display Time (12 chars)
            display_time = line[6:18].strip()
            # Position 19 (index 18): Remark
            remark = line[18:19]
            # Position 20 (index 19): Trend Flag
            trend_flag = line[19:20]
            # Position 21 (index 20): Match Flag
            match_flag = line[20:21]
            # Position 22 (index 21): Trade Upper/Lower Limit
            trade_upper_lower_limit = line[21:22]
            # Position 23-28 (index 22-27): Trade Price (6 chars)
            trade_price_str = line[22:28]
            # Position 29-36 (index 28-35): Trade Volume (8 chars)
            trade_volume_str = line[28:36]
            # Position 37 (index 36): Buy Tick Size
            buy_tick_size_str = line[36:37]
            # Position 38 (index 37): Buy Upper/Lower Limit
            buy_upper_lower_limit = line[37:38]

            # Position 39-108 (index 38-107): 5 Buy Levels (70 bytes total, 14 bytes each)
            buy_levels = []
            for i in range(5):
                start = 38 + i * 14
                price_str = line[start : start + 6]
                volume_str = line[start + 6 : start + 14]
                price = cls.parse_price(price_str)
                volume = cls.parse_volume(volume_str)
                if price > 0 or volume > 0:
                    buy_levels.append(OrderBookLevel(price=price, volume=volume))

            # Position 109 (index 108): Sell Tick Size
            sell_tick_size_str = line[108:109]
            # Position 110 (index 109): Sell Upper/Lower Limit
            sell_upper_lower_limit = line[109:110]

            # Position 111-180 (index 110-179): 5 Sell Levels (70 bytes total, 14 bytes each)
            sell_levels = []
            for i in range(5):
                start = 110 + i * 14
                price_str = line[start : start + 6]
                volume_str = line[start + 6 : start + 14]
                price = cls.parse_price(price_str)
                volume = cls.parse_volume(volume_str)
                if price > 0 or volume > 0:
                    sell_levels.append(OrderBookLevel(price=price, volume=volume))

            # Position 181-188 (index 180-187): Display Date (8 chars)
            display_date = line[180:188].strip()
            # Position 189-190 (index 188-189): Match Staff (2 chars)
            match_staff = line[188:190].strip()

            # Parse numerical values
            trade_price = cls.parse_price(trade_price_str)
            trade_volume = cls.parse_volume(trade_volume_str)
            buy_tick_size = int(buy_tick_size_str) if buy_tick_size_str.isdigit() else 0
            sell_tick_size = (
                int(sell_tick_size_str) if sell_tick_size_str.isdigit() else 0
            )

            # Create instrument ID (e.g., "0050.TWSE")
            instrument_id = InstrumentId.from_str(f"{securities_code}.TWSE")

            # Parse timestamps
            ts_event = cls.parse_time(display_date, display_time)
            ts_init = dt_to_unix_nanos(datetime.now(timezone.utc))

            return TWSESnapshotData(
                instrument_id=instrument_id,
                display_time=display_time,
                remark=remark,
                trend_flag=trend_flag,
                match_flag=match_flag,
                trade_upper_lower_limit=trade_upper_lower_limit,
                trade_price=trade_price,
                trade_volume=trade_volume,
                buy_tick_size=buy_tick_size,
                buy_upper_lower_limit=buy_upper_lower_limit,
                buy_levels=buy_levels,
                sell_tick_size=sell_tick_size,
                sell_upper_lower_limit=sell_upper_lower_limit,
                sell_levels=sell_levels,
                display_date=display_date,
                match_staff=match_staff,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        except Exception as e:
            print(f"Error parsing record: {e}")
            return None


class TWSEDataLoader:
    """
    Data loader for reading TWSE snapshot files.

    Automatically detects and handles compressed files (.gz, .zst).

    Parameters
    ----------
    file_path : str or Path
        Path to the TWSE snapshot data file (supports .gz and .zst compression)

    Raises
    ------
    ImportError
        If .zst file is used but zstandard package is not installed
    """

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.parser = TWSESnapshotParser()
        self.compression_format = self._detect_compression()

        # Validate zstd availability
        if self.compression_format == "zst" and not ZSTD_AVAILABLE:
            import sys

            if sys.version_info >= (3, 14):
                msg = "compression.zstd is required for .zst files in Python 3.14+"
            else:
                msg = (
                    "zstd support is required for .zst files. "
                    "Install with: pip install backports.zstd or pip install zstandard"
                )
            raise ImportError(msg)

    def _detect_compression(self) -> str:
        """
        Detect compression format from file extension.

        Returns
        -------
        str
            Compression format: 'gz', 'zst', or 'none'
        """
        suffix = self.file_path.suffix.lower()
        if suffix == ".gz":
            return "gz"
        elif suffix == ".zst":
            return "zst"
        else:
            return "none"

    def _open_file(self):
        """
        Smart file opener - automatically handles compression.

        Returns
        -------
        file object
            Binary file handle (compressed or uncompressed)
        """
        if self.compression_format == "gz":
            return gzip.open(self.file_path, "rb")
        elif self.compression_format == "zst":
            # Zstandard decompression with streaming
            # Both backports.zstd and compression.zstd have open() method
            return zstd.open(self.file_path, "rb")
        else:
            return open(self.file_path, "rb")

    def read_records(self, limit: Optional[int] = None) -> Iterator[TWSESnapshotData]:
        """
        Read and parse records from file (with automatic decompression).

        Supports streaming decompression for compressed files (.gz, .zst) -
        decompresses on-the-fly without loading entire file into memory.

        Parameters
        ----------
        limit : int, optional
            Maximum number of records to read. If None, read all records.

        Yields
        ------
        TWSESnapshotData
            Parsed snapshot data
        """
        count = 0
        with self._open_file() as f:
            while True:
                if limit is not None and count >= limit:
                    break

                # Try to read 191 bytes (190 + newline)
                # For .gz files, this decompresses on-the-fly (streaming)
                record = f.read(TWSESnapshotParser.RECORD_LENGTH_WITH_NEWLINE)
                if not record:
                    break

                snapshot = self.parser.parse_record(record)
                if snapshot:
                    yield snapshot
                    count += 1

                # If we got less than 191 bytes, we're at EOF
                if len(record) < TWSESnapshotParser.RECORD_LENGTH_WITH_NEWLINE:
                    break

    def count_records(self) -> int:
        """
        Count total number of records in file.

        For compressed files (.gz, .zst), this must read through the entire file.
        For uncompressed files, uses file size for instant count.

        Returns
        -------
        int
            Total number of records
        """
        if self.compression_format != "none":
            # Must read through compressed file to count
            count = 0
            with self._open_file() as f:
                while True:
                    record = f.read(TWSESnapshotParser.RECORD_LENGTH_WITH_NEWLINE)
                    if not record or len(record) < TWSESnapshotParser.RECORD_LENGTH:
                        break
                    count += 1
            return count
        else:
            # Fast count for uncompressed files using file size
            file_size = self.file_path.stat().st_size
            # Try with newline first (more common)
            records_with_newline = (
                file_size // TWSESnapshotParser.RECORD_LENGTH_WITH_NEWLINE
            )
            if (
                records_with_newline * TWSESnapshotParser.RECORD_LENGTH_WITH_NEWLINE
                == file_size
            ):
                return records_with_newline
            # Fall back to without newline
            return file_size // TWSESnapshotParser.RECORD_LENGTH


if __name__ == "__main__":
    # Simple test
    import sys

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "../snapshot/Sample_new"

    loader = TWSEDataLoader(file_path)
    print(f"Total records: {loader.count_records()}")
    print("\nFirst 10 records:")

    for i, snapshot in enumerate(loader.read_records(limit=10)):
        print(f"\n{i+1}. {snapshot}")
        if i == 0:
            print(f"   Buy levels: {snapshot.buy_levels}")
            print(f"   Sell levels: {snapshot.sell_levels}")

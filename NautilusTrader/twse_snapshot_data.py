"""
TWSE Snapshot Custom Data for NautilusTrader.

This module provides custom data types for Taiwan Stock Exchange (TWSE)
intra-day snapshot data with 5-level order book depth.
"""

from typing import List
import msgspec
import pyarrow as pa
from nautilus_trader.core.data import Data
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.core.datetime import unix_nanos_to_iso8601


class OrderBookLevel:
    """Represents a single level in the order book."""

    def __init__(self, price: float, volume: int):
        self.price = price
        self.volume = volume

    def to_dict(self):
        return {"price": self.price, "volume": self.volume}

    @classmethod
    def from_dict(cls, data: dict):
        return cls(price=data["price"], volume=data["volume"])

    def __repr__(self):
        return f"OrderBookLevel(price={self.price}, volume={self.volume})"


class TWSESnapshotData(Data):
    """
    TWSE Snapshot data with 5-level order book.

    This data type represents a snapshot of the Taiwan Stock Exchange order book
    with up to 5 levels of bid and ask prices/volumes.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument identifier.
    display_time : str
        Display time in format HHMMSSMS (12 chars).
    remark : str
        Display remark (空白: Normal, T: Trial Calculation, S: Stabilizing Measures).
    trend_flag : str
        Trend flag (空白: No Execute, R: RAISING-TREND, F: FALLING-TREND, C: Intermediate price).
    match_flag : str
        Match flag (空白: No match, Y: Has match, S: Stabilizing Measures).
    trade_upper_lower_limit : str
        Trade limit flag (空白: Normal, R: Limit-up, F: Limit-down).
    trade_price : float
        Trade price.
    trade_volume : int
        Trade volume in lots (1 lot = 1000 shares).
    buy_tick_size : int
        Number of buy levels (0-5).
    buy_upper_lower_limit : str
        Buy limit flag.
    buy_levels : List[OrderBookLevel]
        List of up to 5 buy (bid) levels.
    sell_tick_size : int
        Number of sell levels (0-5).
    sell_upper_lower_limit : str
        Sell limit flag.
    sell_levels : List[OrderBookLevel]
        List of up to 5 sell (ask) levels.
    display_date : str
        Display date in format YYYYMMDD.
    match_staff : str
        Match staff code.
    ts_event : int
        UNIX timestamp (nanoseconds) when the data event occurred.
    ts_init : int
        UNIX timestamp (nanoseconds) when the object was initialized.
    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        display_time: str,
        remark: str,
        trend_flag: str,
        match_flag: str,
        trade_upper_lower_limit: str,
        trade_price: float,
        trade_volume: int,
        buy_tick_size: int,
        buy_upper_lower_limit: str,
        buy_levels: List[OrderBookLevel],
        sell_tick_size: int,
        sell_upper_lower_limit: str,
        sell_levels: List[OrderBookLevel],
        display_date: str,
        match_staff: str,
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.display_time = display_time
        self.remark = remark
        self.trend_flag = trend_flag
        self.match_flag = match_flag
        self.trade_upper_lower_limit = trade_upper_lower_limit
        self.trade_price = trade_price
        self.trade_volume = trade_volume
        self.buy_tick_size = buy_tick_size
        self.buy_upper_lower_limit = buy_upper_lower_limit
        self.buy_levels = buy_levels
        self.sell_tick_size = sell_tick_size
        self.sell_upper_lower_limit = sell_upper_lower_limit
        self.sell_levels = sell_levels
        self.display_date = display_date
        self.match_staff = match_staff
        self._ts_event = ts_event
        self._ts_init = ts_init

    @property
    def ts_event(self) -> int:
        """
        UNIX timestamp (nanoseconds) when the data event occurred.

        Returns
        -------
        int
        """
        return self._ts_event

    @property
    def ts_init(self) -> int:
        """
        UNIX timestamp (nanoseconds) when the object was initialized.

        Returns
        -------
        int
        """
        return self._ts_init

    def __repr__(self):
        return (
            f"TWSESnapshotData("
            f"instrument_id={self.instrument_id}, "
            f"display_time={self.display_time}, "
            f"trade_price={self.trade_price}, "
            f"trade_volume={self.trade_volume}, "
            f"buy_levels={len(self.buy_levels)}, "
            f"sell_levels={len(self.sell_levels)}, "
            f"ts_event={unix_nanos_to_iso8601(self._ts_event)})"
        )

    def to_dict(self):
        """Convert to dictionary for serialization."""
        return {
            "instrument_id": self.instrument_id.value,
            "display_time": self.display_time,
            "remark": self.remark,
            "trend_flag": self.trend_flag,
            "match_flag": self.match_flag,
            "trade_upper_lower_limit": self.trade_upper_lower_limit,
            "trade_price": self.trade_price,
            "trade_volume": self.trade_volume,
            "buy_tick_size": self.buy_tick_size,
            "buy_upper_lower_limit": self.buy_upper_lower_limit,
            "buy_levels": [level.to_dict() for level in self.buy_levels],
            "sell_tick_size": self.sell_tick_size,
            "sell_upper_lower_limit": self.sell_upper_lower_limit,
            "sell_levels": [level.to_dict() for level in self.sell_levels],
            "display_date": self.display_date,
            "match_staff": self.match_staff,
            "ts_event": self._ts_event,
            "ts_init": self._ts_init,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create from dictionary."""
        return cls(
            instrument_id=InstrumentId.from_str(data["instrument_id"]),
            display_time=data["display_time"],
            remark=data["remark"],
            trend_flag=data["trend_flag"],
            match_flag=data["match_flag"],
            trade_upper_lower_limit=data["trade_upper_lower_limit"],
            trade_price=data["trade_price"],
            trade_volume=data["trade_volume"],
            buy_tick_size=data["buy_tick_size"],
            buy_upper_lower_limit=data["buy_upper_lower_limit"],
            buy_levels=[
                OrderBookLevel.from_dict(level) for level in data["buy_levels"]
            ],
            sell_tick_size=data["sell_tick_size"],
            sell_upper_lower_limit=data["sell_upper_lower_limit"],
            sell_levels=[
                OrderBookLevel.from_dict(level) for level in data["sell_levels"]
            ],
            display_date=data["display_date"],
            match_staff=data["match_staff"],
            ts_event=data["ts_event"],
            ts_init=data["ts_init"],
        )

    def to_bytes(self):
        """Serialize to bytes using msgpack."""
        return msgspec.msgpack.encode(self.to_dict())

    @classmethod
    def from_bytes(cls, data: bytes):
        """Deserialize from bytes."""
        return cls.from_dict(msgspec.msgpack.decode(data))

    def to_catalog(self):
        """Convert to PyArrow RecordBatch for catalog storage."""
        data_dict = self.to_dict()
        # Flatten the buy/sell levels for parquet storage
        for i in range(5):
            if i < len(self.buy_levels):
                data_dict[f"buy_price_{i+1}"] = self.buy_levels[i].price
                data_dict[f"buy_volume_{i+1}"] = self.buy_levels[i].volume
            else:
                data_dict[f"buy_price_{i+1}"] = 0.0
                data_dict[f"buy_volume_{i+1}"] = 0

        for i in range(5):
            if i < len(self.sell_levels):
                data_dict[f"sell_price_{i+1}"] = self.sell_levels[i].price
                data_dict[f"sell_volume_{i+1}"] = self.sell_levels[i].volume
            else:
                data_dict[f"sell_price_{i+1}"] = 0.0
                data_dict[f"sell_volume_{i+1}"] = 0

        # Remove nested structures
        del data_dict["buy_levels"]
        del data_dict["sell_levels"]

        return pa.RecordBatch.from_pylist([data_dict], schema=TWSESnapshotData.schema())

    @classmethod
    def from_catalog(cls, table: pa.Table):
        """Load from PyArrow Table."""
        result = []
        for row in table.to_pylist():
            # Reconstruct buy_levels
            buy_levels = []
            for i in range(5):
                price = row.get(f"buy_price_{i+1}", 0.0)
                volume = row.get(f"buy_volume_{i+1}", 0)
                if price > 0 or volume > 0:
                    buy_levels.append(OrderBookLevel(price=price, volume=volume))

            # Reconstruct sell_levels
            sell_levels = []
            for i in range(5):
                price = row.get(f"sell_price_{i+1}", 0.0)
                volume = row.get(f"sell_volume_{i+1}", 0)
                if price > 0 or volume > 0:
                    sell_levels.append(OrderBookLevel(price=price, volume=volume))

            data = cls(
                instrument_id=InstrumentId.from_str(row["instrument_id"]),
                display_time=row["display_time"],
                remark=row["remark"],
                trend_flag=row["trend_flag"],
                match_flag=row["match_flag"],
                trade_upper_lower_limit=row["trade_upper_lower_limit"],
                trade_price=row["trade_price"],
                trade_volume=row["trade_volume"],
                buy_tick_size=row["buy_tick_size"],
                buy_upper_lower_limit=row["buy_upper_lower_limit"],
                buy_levels=buy_levels,
                sell_tick_size=row["sell_tick_size"],
                sell_upper_lower_limit=row["sell_upper_lower_limit"],
                sell_levels=sell_levels,
                display_date=row["display_date"],
                match_staff=row["match_staff"],
                ts_event=row["ts_event"],
                ts_init=row["ts_init"],
            )
            result.append(data)

        return result

    @classmethod
    def schema(cls):
        """PyArrow schema for catalog storage."""
        fields = {
            "instrument_id": pa.string(),
            "display_time": pa.string(),
            "remark": pa.string(),
            "trend_flag": pa.string(),
            "match_flag": pa.string(),
            "trade_upper_lower_limit": pa.string(),
            "trade_price": pa.float64(),
            "trade_volume": pa.int64(),
            "buy_tick_size": pa.int64(),
            "buy_upper_lower_limit": pa.string(),
            "sell_tick_size": pa.int64(),
            "sell_upper_lower_limit": pa.string(),
            "display_date": pa.string(),
            "match_staff": pa.string(),
            "ts_event": pa.int64(),
            "ts_init": pa.int64(),
        }

        # Add flattened buy/sell levels
        for i in range(1, 6):
            fields[f"buy_price_{i}"] = pa.float64()
            fields[f"buy_volume_{i}"] = pa.int64()

        for i in range(1, 6):
            fields[f"sell_price_{i}"] = pa.float64()
            fields[f"sell_volume_{i}"] = pa.int64()

        return pa.schema(fields)

"""
Demo of TWSE Snapshot Data with NautilusTrader.

This demo shows:
1. How to create a custom data adapter that reads TWSE snapshot files
2. How to broadcast data to the event bus
3. How to subscribe to custom data using Actor or Strategy
"""

from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.config import BacktestRunConfig
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.backtest.config import BacktestVenueConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import ClientId, Venue, InstrumentId, Symbol
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Price, Quantity, Money, Currency
from nautilus_trader.model.enums import OmsType, AccountType
from nautilus_trader.common.actor import Actor
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.serialization.base import register_serializable_type

from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader

# Register the custom data type
register_serializable_type(
    TWSESnapshotData, TWSESnapshotData.to_dict, TWSESnapshotData.from_dict
)


class TWSESnapshotActor(Actor):
    """
    Actor that subscribes to TWSE snapshot data and processes it.

    This actor demonstrates how to:
    - Subscribe to custom data types
    - Handle custom data in the on_data callback
    - Track order book updates
    """

    def __init__(self, config=None):
        super().__init__(config)
        self.snapshot_count = 0
        self.instruments_seen = set()

    def on_start(self):
        """Called when actor starts."""
        self.log.info("TWSESnapshotActor started")

        # Subscribe to TWSE snapshot data
        self.subscribe_data(
            data_type=DataType(TWSESnapshotData),
        )
        self.log.info("Subscribed to TWSESnapshotData")

    def on_data(self, data):
        """
        Handle incoming data.

        This method is called for all custom data types.
        """
        if isinstance(data, TWSESnapshotData):
            self.snapshot_count += 1
            self.instruments_seen.add(str(data.instrument_id))

            # Log every 1000 snapshots
            if self.snapshot_count % 1000 == 0:
                self.log.info(
                    f"Processed {self.snapshot_count} snapshots, "
                    f"Instruments: {len(self.instruments_seen)}"
                )

            # Log first snapshot for each instrument
            if self.snapshot_count <= 5:
                self.log.info(
                    f"Snapshot: {data.instrument_id} @ {data.display_time}, "
                    f"Price: {data.trade_price}, Volume: {data.trade_volume}, "
                    f"Bid Levels: {len(data.buy_levels)}, Ask Levels: {len(data.sell_levels)}"
                )

                if data.buy_levels:
                    best_bid = data.buy_levels[0]
                    self.log.info(f"  Best Bid: {best_bid.price} x {best_bid.volume}")

                if data.sell_levels:
                    best_ask = data.sell_levels[0]
                    self.log.info(f"  Best Ask: {best_ask.price} x {best_ask.volume}")

    def on_stop(self):
        """Called when actor stops."""
        self.log.info(
            f"TWSESnapshotActor stopped. "
            f"Total snapshots: {self.snapshot_count}, "
            f"Instruments: {len(self.instruments_seen)}"
        )


class TWSESnapshotStrategy(Strategy):
    """
    Strategy that subscribes to TWSE snapshot data.

    This strategy demonstrates how to:
    - Subscribe to custom data in a trading strategy
    - Analyze order book data
    - Track market microstructure
    """

    def __init__(self, config=None):
        super().__init__(config)
        self.snapshot_count = 0
        self.match_count = 0
        self.last_prices = {}

    def on_start(self):
        """Called when strategy starts."""
        self.log.info("TWSESnapshotStrategy started")

        # Subscribe to TWSE snapshot data
        self.subscribe_data(
            data_type=DataType(TWSESnapshotData),
        )
        self.log.info("Subscribed to TWSESnapshotData")

    def on_data(self, data):
        """Handle incoming data."""
        if isinstance(data, TWSESnapshotData):
            self.snapshot_count += 1

            # Track matches (trades)
            if data.match_flag == "Y":
                self.match_count += 1
                instrument_str = str(data.instrument_id)

                # Calculate price change
                last_price = self.last_prices.get(instrument_str)
                if last_price is not None and last_price != data.trade_price:
                    price_change = data.trade_price - last_price
                    pct_change = (
                        (price_change / last_price) * 100 if last_price > 0 else 0
                    )

                    self.log.info(
                        f"Trade: {data.instrument_id} @ {data.trade_price} "
                        f"(Volume: {data.trade_volume}, Change: {price_change:+.2f}, "
                        f"{pct_change:+.2f}%)"
                    )

                self.last_prices[instrument_str] = data.trade_price

            # Log statistics every 1000 snapshots
            if self.snapshot_count % 1000 == 0:
                self.log.info(
                    f"Stats: {self.snapshot_count} snapshots, "
                    f"{self.match_count} trades, "
                    f"{len(self.last_prices)} instruments"
                )

    def on_stop(self):
        """Called when strategy stops."""
        self.log.info(
            f"TWSESnapshotStrategy stopped. "
            f"Total: {self.snapshot_count} snapshots, {self.match_count} trades"
        )


def create_sample_equity_instrument(instrument_id: str) -> Equity:
    """
    Create a sample equity instrument.

    Parameters
    ----------
    instrument_id : str
        Instrument ID (e.g., "0050.TWSE")

    Returns
    -------
    Equity
        The equity instrument
    """
    return Equity(
        instrument_id=InstrumentId.from_str(instrument_id),
        raw_symbol=Symbol(instrument_id.split(".")[0]),
        currency=Currency.from_str("TWD"),
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        lot_size=Quantity.from_int(1000),  # 1 lot = 1000 shares in Taiwan
        max_quantity=Quantity.from_int(999999000),
        min_quantity=Quantity.from_int(1000),
        ts_event=0,
        ts_init=0,
    )


def run_actor_demo():
    """Run demo using Actor."""
    print("\n" + "=" * 80)
    print("TWSE Snapshot Data - Actor Demo")
    print("=" * 80 + "\n")

    # Setup paths
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"

    if not data_file.exists():
        print(f"Error: Data file not found: {data_file}")
        return

    # Load data
    loader = TWSEDataLoader(data_file)
    total_records = loader.count_records()
    print(f"Loading data from: {data_file}")
    print(f"Total records: {total_records}")

    # Load first N records for demo
    limit = 100
    snapshots = list(loader.read_records(limit=limit))
    print(f"Loaded {len(snapshots)} snapshots for demo\n")

    if not snapshots:
        print("No data loaded!")
        return

    # Get unique instruments
    instruments = set(str(s.instrument_id) for s in snapshots)
    print(f"Instruments in data: {sorted(instruments)}\n")

    # Create backtest engine
    config = BacktestEngineConfig(
        logging=LoggingConfig(log_level="INFO"),
    )
    engine = BacktestEngine(config=config)

    # Add venue
    venue = Venue("TWSE")
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=Currency.from_str("TWD"),
        starting_balances=[
            Money.from_str("1000000 TWD")
        ],  # Starting balance: 1,000,000 TWD
    )

    # Add instruments
    for instrument_id in instruments:
        instrument = create_sample_equity_instrument(instrument_id)
        engine.add_instrument(instrument)

    # Add actor
    actor = TWSESnapshotActor()
    engine.add_actor(actor)

    # Add custom data
    for snapshot in snapshots:
        engine.add_data([snapshot])

    # Run backtest
    print("Running backtest with Actor...")
    engine.run()

    print("\nBacktest completed!")


def run_strategy_demo():
    """Run demo using Strategy."""
    print("\n" + "=" * 80)
    print("TWSE Snapshot Data - Strategy Demo")
    print("=" * 80 + "\n")

    # Setup paths
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"

    if not data_file.exists():
        print(f"Error: Data file not found: {data_file}")
        return

    # Load data
    loader = TWSEDataLoader(data_file)
    total_records = loader.count_records()
    print(f"Loading data from: {data_file}")
    print(f"Total records: {total_records}")

    # Load all records for demo (limited to first 1000 for speed)
    limit = 1000
    snapshots = list(loader.read_records(limit=limit))
    print(f"Loaded {len(snapshots)} snapshots for demo\n")

    if not snapshots:
        print("No data loaded!")
        return

    # Get unique instruments
    instruments = set(str(s.instrument_id) for s in snapshots)
    print(f"Instruments in data: {sorted(instruments)}\n")

    # Create backtest engine
    config = BacktestEngineConfig(
        logging=LoggingConfig(log_level="INFO"),
    )
    engine = BacktestEngine(config=config)

    # Add venue
    venue = Venue("TWSE")
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=Currency.from_str("TWD"),
        starting_balances=[
            Money.from_str("1000000 TWD")
        ],  # Starting balance: 1,000,000 TWD
    )

    # Add instruments
    for instrument_id in instruments:
        instrument = create_sample_equity_instrument(instrument_id)
        engine.add_instrument(instrument)

    # Add strategy
    strategy = TWSESnapshotStrategy()
    engine.add_strategy(strategy)

    # Add custom data
    for snapshot in snapshots:
        engine.add_data([snapshot])

    # Run backtest
    print("Running backtest with Strategy...")
    engine.run()

    print("\nBacktest completed!")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "strategy":
        run_strategy_demo()
    else:
        # Default to actor demo
        run_actor_demo()

        # Also run strategy demo
        print("\n" + "=" * 80)
        print("Now running Strategy demo...")
        print("=" * 80)
        run_strategy_demo()

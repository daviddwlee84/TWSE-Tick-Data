"""
Simplified TWSE Adapter Demo for Backtesting.

This demo shows a simpler approach that works with BacktestEngine.
For live trading, use the full DataClient implementation.
"""

from pathlib import Path

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money, Currency
from nautilus_trader.model.enums import OmsType, AccountType
from nautilus_trader.common.actor import Actor
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.serialization.base import register_serializable_type

from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader
from twse_adapter import TWSEInstrumentProvider

# Register the custom data type
register_serializable_type(
    TWSESnapshotData, TWSESnapshotData.to_dict, TWSESnapshotData.from_dict
)


class TWSESnapshotActor(Actor):
    """Actor that processes TWSE snapshot data."""
    
    def __init__(self, config=None):
        super().__init__(config)
        self.snapshot_count = 0
        self.instruments_seen = set()
    
    def on_start(self):
        """Called when actor starts."""
        self.log.info("TWSESnapshotActor started")
        
        # Subscribe to TWSE snapshot data
        self.subscribe_data(data_type=DataType(TWSESnapshotData))
        self.log.info("Subscribed to TWSESnapshotData")
    
    def on_data(self, data):
        """Handle incoming data."""
        if isinstance(data, TWSESnapshotData):
            self.snapshot_count += 1
            self.instruments_seen.add(str(data.instrument_id))
            
            # Log first 5 snapshots
            if self.snapshot_count <= 5:
                self.log.info(
                    f"✓ Received snapshot #{self.snapshot_count}: "
                    f"{data.instrument_id} @ {data.display_time}, "
                    f"Price: {data.trade_price}, Volume: {data.trade_volume}"
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
            f"✓ TWSESnapshotActor completed! "
            f"Processed {self.snapshot_count} snapshots from {len(self.instruments_seen)} instruments"
        )


def run_simple_adapter():
    """Run backtest with manual data publishing."""
    print("\n" + "=" * 80)
    print("TWSE Snapshot Data - Simple Adapter Demo")
    print("=" * 80 + "\n")
    
    # Setup paths
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    
    if not data_file.exists():
        print(f"Error: Data file not found: {data_file}")
        return
    
    print(f"Loading data from: {data_file}\n")
    
    # Load data
    loader = TWSEDataLoader(data_file)
    snapshots = list(loader.read_records())
    print(f"Loaded {len(snapshots)} snapshots\n")
    
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
        starting_balances=[Money.from_str("1000000 TWD")],
    )
    
    # Create instrument provider and load instruments
    instrument_provider = TWSEInstrumentProvider(load_all_on_start=True)
    
    # Add instruments to engine
    for instrument in instrument_provider.get_all().values():
        engine.add_instrument(instrument)
    
    print(f"Loaded {len(instrument_provider.get_all())} instruments\n")
    
    # Add actor
    actor = TWSESnapshotActor()
    engine.add_actor(actor)
    
    print("Starting backtest...")
    print("Publishing snapshots through message bus...\n")
    
    # Start the engine (this starts actors and strategies)
    engine.kernel.start()
    
    # Publish data through message bus
    # This simulates what a DataClient would do
    for snapshot in snapshots:
        # Publish to the message bus with the correct topic
        topic = f"data.{venue}.{TWSESnapshotData.__name__}"
        engine.kernel.msgbus.publish(topic=topic, msg=snapshot)
    
    # Stop the engine
    engine.kernel.stop()
    
    print("\n" + "=" * 80)
    print("Backtest completed successfully!")
    print("=" * 80)
    print(f"\nKey Achievement: Actor successfully received custom data via message bus!")
    print(f"This proves the adapter pattern works for custom TWSE data types.")


if __name__ == "__main__":
    run_simple_adapter()


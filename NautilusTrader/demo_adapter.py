"""
Demo using TWSE Data Adapter with NautilusTrader.

This demo shows how to use a proper DataClient adapter to stream
TWSE snapshot data through the NautilusTrader system.
"""

from pathlib import Path
import asyncio

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import Venue, ClientId
from nautilus_trader.model.objects import Money, Currency
from nautilus_trader.model.enums import OmsType, AccountType
from nautilus_trader.common.actor import Actor
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.serialization.base import register_serializable_type

from twse_snapshot_data import TWSESnapshotData
from twse_adapter import TWSEInstrumentProvider, TWSEDataClient

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
            
            # Log every 100 snapshots
            if self.snapshot_count % 100 == 0:
                self.log.info(
                    f"Processed {self.snapshot_count} snapshots, "
                    f"Instruments: {len(self.instruments_seen)}"
                )
            
            # Log first 5 snapshots
            if self.snapshot_count <= 5:
                self.log.info(
                    f"Snapshot: {data.instrument_id} @ {data.display_time}, "
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
            f"TWSESnapshotActor stopped. "
            f"Total snapshots: {self.snapshot_count}, "
            f"Instruments: {len(self.instruments_seen)}"
        )


class TWSESnapshotStrategy(Strategy):
    """Strategy that processes TWSE snapshot data."""
    
    def __init__(self, config=None):
        super().__init__(config)
        self.snapshot_count = 0
        self.match_count = 0
    
    def on_start(self):
        """Called when strategy starts."""
        self.log.info("TWSESnapshotStrategy started")
        
        # Subscribe to TWSE snapshot data
        self.subscribe_data(data_type=DataType(TWSESnapshotData))
        self.log.info("Subscribed to TWSESnapshotData")
    
    def on_data(self, data):
        """Handle incoming data."""
        if isinstance(data, TWSESnapshotData):
            self.snapshot_count += 1
            
            # Track matches (trades)
            if data.match_flag == "Y":
                self.match_count += 1
                
                if self.match_count <= 5:
                    self.log.info(
                        f"Trade: {data.instrument_id} @ {data.trade_price} "
                        f"(Volume: {data.trade_volume})"
                    )
            
            # Log statistics every 100 snapshots
            if self.snapshot_count % 100 == 0:
                self.log.info(
                    f"Stats: {self.snapshot_count} snapshots, {self.match_count} trades"
                )
    
    def on_stop(self):
        """Called when strategy stops."""
        self.log.info(
            f"TWSESnapshotStrategy stopped. "
            f"Total: {self.snapshot_count} snapshots, {self.match_count} trades"
        )


async def run_with_adapter():
    """Run backtest using TWSE Data Adapter."""
    print("\n" + "=" * 80)
    print("TWSE Snapshot Data - Adapter Demo")
    print("=" * 80 + "\n")
    
    # Setup paths
    data_file = Path(__file__).parent.parent / "snapshot" / "Sample_new"
    
    if not data_file.exists():
        print(f"Error: Data file not found: {data_file}")
        return
    
    print(f"Loading data from: {data_file}\n")
    
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
    
    # Create and add data client
    data_client = TWSEDataClient(
        loop=asyncio.get_event_loop(),
        client_id=ClientId("TWSE"),
        msgbus=engine.kernel.msgbus,
        cache=engine.kernel.cache,
        clock=engine.kernel.clock,
        logger=engine.kernel.logger,
        instrument_provider=instrument_provider,
        data_file=data_file,
        replay_speed=1000.0,  # 1000x speed for demo
    )
    
    # Register data client with engine
    engine.add_data_client(data_client)
    
    # Add actor
    actor = TWSESnapshotActor()
    engine.add_actor(actor)
    
    # Add strategy
    strategy = TWSESnapshotStrategy()
    engine.add_strategy(strategy)
    
    print("Running backtest with TWSE Data Adapter...")
    print("This uses a proper DataClient to stream data through the message bus.\n")
    
    # Run backtest
    await engine.run_async()
    
    print("\nBacktest completed!")


if __name__ == "__main__":
    # Run the async demo
    asyncio.run(run_with_adapter())


"""
TWSE Data Client.

Provides data streaming from TWSE snapshot files.
"""

from pathlib import Path
from typing import Optional, List
import asyncio

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, Logger, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Venue

from twse_snapshot_data import TWSESnapshotData
from twse_data_loader import TWSEDataLoader
from twse_adapter.providers import TWSEInstrumentProvider


class TWSEDataClient(LiveMarketDataClient):
    """
    Data client for TWSE snapshot data.
    
    This client reads TWSE snapshot files and publishes data through
    the NautilusTrader message bus, enabling proper data routing to
    actors and strategies.
    
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client_id : ClientId
        The client ID.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    logger : Logger
        The logger for the client.
    instrument_provider : TWSEInstrumentProvider
        The instrument provider.
    data_file : Path or str
        Path to the TWSE snapshot data file.
    replay_speed : float, default 1.0
        Replay speed multiplier (1.0 = real-time, 10.0 = 10x speed).
    """
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id: ClientId,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
        instrument_provider: TWSEInstrumentProvider,
        data_file: Path | str,
        replay_speed: float = 1.0,
    ):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=Venue("TWSE"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        
        self._data_file = Path(data_file)
        self._replay_speed = replay_speed
        self._loader = TWSEDataLoader(self._data_file)
        self._subscribed_instruments: set[InstrumentId] = set()
        self._is_running = False
        self._replay_task: Optional[asyncio.Task] = None
    
    async def _connect(self) -> None:
        """Connect to the data source."""
        self._log.info(f"Connecting to TWSE data file: {self._data_file}")
        
        if not self._data_file.exists():
            self._log.error(f"Data file not found: {self._data_file}")
            return
        
        record_count = self._loader.count_records()
        self._log.info(f"Found {record_count} records in data file")
        
        self._is_running = True
        self._log.info("Connected to TWSE data source")
    
    async def _disconnect(self) -> None:
        """Disconnect from the data source."""
        self._is_running = False
        
        if self._replay_task and not self._replay_task.done():
            self._replay_task.cancel()
            try:
                await self._replay_task
            except asyncio.CancelledError:
                pass
        
        self._log.info("Disconnected from TWSE data source")
    
    async def _subscribe(self, data_type: DataType) -> None:
        """
        Subscribe to data.
        
        Parameters
        ----------
        data_type : DataType
            The data type to subscribe to.
        """
        # Check if subscribing to TWSESnapshotData
        if data_type.type == TWSESnapshotData:
            self._log.info(f"Subscribed to {data_type}")
            
            # Start replay if not already running
            if self._replay_task is None or self._replay_task.done():
                self._replay_task = self.create_task(self._replay_data())
        else:
            self._log.warning(f"Unsupported data type: {data_type}")
    
    async def _unsubscribe(self, data_type: DataType) -> None:
        """
        Unsubscribe from data.
        
        Parameters
        ----------
        data_type : DataType
            The data type to unsubscribe from.
        """
        self._log.info(f"Unsubscribed from {data_type}")
    
    async def _replay_data(self) -> None:
        """
        Replay data from the file.
        
        This method simulates live data streaming by reading records
        from the file and publishing them through the message bus.
        """
        self._log.info("Starting data replay...")
        
        try:
            last_ts = None
            
            for snapshot in self._loader.read_records():
                if not self._is_running:
                    break
                
                # Simulate timing based on timestamps
                if last_ts is not None and self._replay_speed > 0:
                    time_diff = (snapshot.ts_event - last_ts) / 1_000_000_000.0  # Convert ns to seconds
                    if time_diff > 0:
                        await asyncio.sleep(time_diff / self._replay_speed)
                
                last_ts = snapshot.ts_event
                
                # Publish data through message bus
                self._handle_data(snapshot)
            
            self._log.info("Data replay completed")
        
        except asyncio.CancelledError:
            self._log.info("Data replay cancelled")
        except Exception as e:
            self._log.error(f"Error during data replay: {e}")
    
    def _handle_data(self, data: TWSESnapshotData) -> None:
        """
        Handle and publish data.
        
        Parameters
        ----------
        data : TWSESnapshotData
            The snapshot data to publish.
        """
        # Publish through the message bus using the proper endpoint
        self._msgbus.publish(
            topic=f"data.{self._venue}.{data.__class__.__name__}",
            msg=data,
        )
    
    # Override request methods for instrument data
    def request_instrument(self, instrument_id: InstrumentId, correlation_id: UUID4) -> None:
        """
        Request an instrument.
        
        Parameters
        ----------
        instrument_id : InstrumentId
            The instrument ID to request.
        correlation_id : UUID4
            The correlation ID for the request.
        """
        instrument = self._instrument_provider.find(instrument_id)
        
        if instrument is None:
            self._log.warning(f"Instrument not found: {instrument_id}")
            return
        
        # Publish instrument through message bus
        self._handle_instrument(instrument, correlation_id)
    
    def request_instruments(self, venue: Venue, correlation_id: UUID4) -> None:
        """
        Request all instruments for a venue.
        
        Parameters
        ----------
        venue : Venue
            The venue to request instruments for.
        correlation_id : UUID4
            The correlation ID for the request.
        """
        if venue != self._venue:
            self._log.warning(f"Invalid venue: {venue}")
            return
        
        instruments = list(self._instrument_provider.get_all().values())
        
        for instrument in instruments:
            self._handle_instrument(instrument, correlation_id)


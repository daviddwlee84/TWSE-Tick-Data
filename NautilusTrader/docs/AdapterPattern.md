# NautilusTrader Adapter Pattern

## Overview

The **Adapter Pattern** is the recommended way to integrate custom data sources with NautilusTrader. Unlike directly adding data through `engine.add_data()`, adapters provide proper data routing through the message bus, enabling actors and strategies to receive data through their `on_data()` callbacks.

## Why Use Adapters?

### Problem with Direct Data Addition

When using `engine.add_data()` in backtest mode:

```python
# ❌ This doesn't route to on_data() callbacks
for snapshot in snapshots:
    engine.add_data([snapshot])
```

**Issues:**
- Data is added to the engine but not routed through the message bus
- Actors/Strategies that subscribe don't receive the data
- `on_data()` callbacks are never invoked
- Only works for built-in NautilusTrader data types

### Solution: Data Adapter

A proper `DataClient` adapter:

```python
# ✅ This routes data correctly
class TWSEDataClient(LiveMarketDataClient):
    def _handle_data(self, data: TWSESnapshotData):
        self._msgbus.publish(
            topic=f"data.{self._venue}.{data.__class__.__name__}",
            msg=data,
        )
```

**Benefits:**
- Data flows through the message bus
- Proper routing to subscribed actors/strategies
- Works with custom data types
- Same code works for backtest and live trading
- Follows NautilusTrader's architecture

## Adapter Components

Based on [NautilusTrader documentation](https://nautilustrader.io/docs/latest/concepts/adapters), a typical adapter includes:

### 1. InstrumentProvider

Provides instrument definitions for the venue.

```python
class TWSEInstrumentProvider(InstrumentProvider):
    def __init__(self, load_all_on_start: bool = True):
        super().__init__()
        self._instruments: Dict[InstrumentId, Equity] = {}
        
    def load_all(self) -> None:
        """Load all available instruments."""
        for symbol in self._symbols:
            instrument = self._create_instrument(symbol)
            self._instruments[instrument.id] = instrument
            self.add(instrument)
```

**Key Points:**
- Inherits from `InstrumentProvider`
- Creates `Instrument` objects (e.g., `Equity`, `Future`)
- Can load all instruments or specific IDs
- Used for both research and live trading

### 2. DataClient

Streams data from the source and publishes to the message bus.

```python
class TWSEDataClient(LiveMarketDataClient):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client_id: ClientId,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
        instrument_provider: TWSEInstrumentProvider,
        # ... custom parameters
    ):
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=Venue("TWSE"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            instrument_provider=instrument_provider,
        )
```

**Required Methods:**
- `_connect()` - Initialize connection to data source
- `_disconnect()` - Clean up resources
- `_subscribe(data_type)` - Handle subscription requests
- `_unsubscribe(data_type)` - Handle unsubscription requests

### 3. Data Publishing

The key to proper data routing:

```python
def _handle_data(self, data: TWSESnapshotData) -> None:
    """Publish data through message bus."""
    self._msgbus.publish(
        topic=f"data.{self._venue}.{data.__class__.__name__}",
        msg=data,
    )
```

**Topic Format:**
- `data.{VENUE}.{DataClassName}` - Custom data type
- `data.{VENUE}.quotes` - Quote ticks
- `data.{VENUE}.trades` - Trade ticks

## Implementation Example

### Step 1: Create InstrumentProvider

```python
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.instruments import Equity

class TWSEInstrumentProvider(InstrumentProvider):
    def __init__(self):
        super().__init__()
        self._venue = Venue("TWSE")
        
    def _create_instrument(self, symbol: str) -> Equity:
        return Equity(
            instrument_id=InstrumentId(Symbol(symbol), self._venue),
            raw_symbol=Symbol(symbol),
            currency=Currency.from_str("TWD"),
            price_precision=2,
            price_increment=Price.from_str("0.01"),
            lot_size=Quantity.from_int(1000),
            # ... other parameters
        )
```

### Step 2: Create DataClient

```python
from nautilus_trader.live.data_client import LiveMarketDataClient

class TWSEDataClient(LiveMarketDataClient):
    async def _connect(self) -> None:
        """Initialize data source."""
        self._log.info("Connecting to TWSE data source")
        # Setup file readers, API connections, etc.
        
    async def _subscribe(self, data_type: DataType) -> None:
        """Handle subscription."""
        if data_type.type == TWSESnapshotData:
            # Start streaming data
            self.create_task(self._stream_data())
    
    async def _stream_data(self) -> None:
        """Stream data and publish."""
        for snapshot in self._loader.read_records():
            self._handle_data(snapshot)
```

### Step 3: Register and Use

```python
# In backtest setup
engine = BacktestEngine(config=config)

# Create and register data client
data_client = TWSEDataClient(
    loop=asyncio.get_event_loop(),
    client_id=...,
    msgbus=engine._kernel.msgbus,
    cache=engine._kernel.cache,
    # ... other parameters
)

engine.add_data_client(data_client)

# Actors/Strategies can now subscribe
actor.subscribe_data(DataType(TWSESnapshotData))
```

## Message Bus Flow

```
DataClient._handle_data(snapshot)
    ↓
MessageBus.publish(topic="data.TWSE.TWSESnapshotData", msg=snapshot)
    ↓
MessageBus routes to subscribers
    ↓
Actor/Strategy.on_data(snapshot)
```

## Backtest vs Live Trading

### Backtest Mode

```python
class TWSEDataClient(LiveMarketDataClient):
    async def _replay_data(self):
        """Replay historical data."""
        for snapshot in self._loader.read_records():
            # Simulate timing
            await asyncio.sleep(time_diff / replay_speed)
            # Publish
            self._handle_data(snapshot)
```

### Live Trading Mode

```python
class TWSEDataClient(LiveMarketDataClient):
    async def _stream_live_data(self):
        """Stream real-time data."""
        async for snapshot in self._api_client.stream():
            self._handle_data(snapshot)
```

**Key Point:** Same `_handle_data()` method works for both modes!

## Best Practices

### 1. Use Async Methods

```python
async def _connect(self) -> None:
    """Always use async for I/O operations."""
    await self._initialize_connection()
```

### 2. Proper Resource Cleanup

```python
async def _disconnect(self) -> None:
    """Cancel tasks and close connections."""
    if self._stream_task:
        self._stream_task.cancel()
        await self._stream_task
```

### 3. Error Handling

```python
async def _stream_data(self):
    try:
        for snapshot in self._loader.read_records():
            self._handle_data(snapshot)
    except asyncio.CancelledError:
        self._log.info("Stream cancelled")
    except Exception as e:
        self._log.error(f"Stream error: {e}")
```

### 4. Timing Simulation

```python
# For backtest with realistic timing
if last_ts is not None:
    time_diff = (snapshot.ts_event - last_ts) / 1e9
    await asyncio.sleep(time_diff / replay_speed)
```

## Comparison: Direct vs Adapter

| Aspect       | Direct (`add_data`)          | Adapter (`DataClient`)        |
| ------------ | ---------------------------- | ----------------------------- |
| Data Routing | ❌ Doesn't route to callbacks | ✅ Routes to `on_data()`       |
| Custom Data  | ❌ Limited support            | ✅ Full support                |
| Message Bus  | ❌ Bypassed                   | ✅ Uses message bus            |
| Live Trading | ❌ Doesn't work               | ✅ Works seamlessly            |
| Code Reuse   | ❌ Separate code needed       | ✅ Same code for backtest/live |
| Subscription | ❌ Ignored                    | ✅ Honors subscriptions        |

## Common Pitfalls

### 1. Forgetting to Register Data Type

```python
# ❌ Missing registration
engine.add_data_client(data_client)

# ✅ Register first
register_serializable_type(
    TWSESnapshotData,
    TWSESnapshotData.to_dict,
    TWSESnapshotData.from_dict
)
engine.add_data_client(data_client)
```

### 2. Incorrect Topic Format

```python
# ❌ Wrong topic format
self._msgbus.publish(topic="custom_data", msg=data)

# ✅ Correct format
self._msgbus.publish(
    topic=f"data.{self._venue}.{data.__class__.__name__}",
    msg=data
)
```

### 3. Not Using create_task()

```python
# ❌ Blocks the event loop
await self._long_running_stream()

# ✅ Create background task
self.create_task(self._long_running_stream())
```

## References

- [NautilusTrader Adapters Documentation](https://nautilustrader.io/docs/latest/concepts/adapters)
- [NautilusTrader Architecture](https://nautilustrader.io/docs/latest/concepts/architecture)
- [Message Bus Concepts](https://nautilustrader.io/docs/latest/concepts/message_bus)
- [Live Trading Guide](https://nautilustrader.io/docs/latest/concepts/live_trading)

## Summary

**Key Takeaway:** For custom data in NautilusTrader, always use a `DataClient` adapter instead of directly adding data. This ensures proper message bus routing and enables seamless transition from backtesting to live trading.

The adapter pattern is the **professional** way to integrate data sources with NautilusTrader, following the platform's architecture and best practices.


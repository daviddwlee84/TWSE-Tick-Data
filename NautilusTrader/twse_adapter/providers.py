"""
TWSE Instrument Provider.

Provides instrument definitions for Taiwan Stock Exchange securities.
"""

from typing import Dict, List, Optional
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Price, Quantity, Currency
from nautilus_trader.common.providers import InstrumentProvider


class TWSEInstrumentProvider(InstrumentProvider):
    """
    Provides TWSE instrument definitions.
    
    This provider creates Equity instruments for Taiwan Stock Exchange securities.
    It can be used standalone for research or integrated into a trading system.
    
    Parameters
    ----------
    load_all_on_start : bool, default True
        If True, load all instruments on initialization.
    """
    
    def __init__(self, load_all_on_start: bool = True):
        super().__init__()
        self._instruments: Dict[InstrumentId, Equity] = {}
        self._venue = Venue("TWSE")
        self._currency = Currency.from_str("TWD")
        
        # Common TWSE stocks for demo purposes
        # In production, this would load from an API or database
        self._common_symbols = {
            "0050": "元大台灣50",  # Yuanta Taiwan Top 50 ETF
            "0056": "元大高股息",   # Yuanta Taiwan Dividend Plus ETF
            "2330": "台積電",      # Taiwan Semiconductor Manufacturing
            "2317": "鴻海",        # Hon Hai Precision (Foxconn)
            "2454": "聯發科",      # MediaTek
            "2303": "聯電",        # United Microelectronics
            "2308": "台達電",      # Delta Electronics
            "9958": "世紀鋼",      # Century Iron and Steel Industrial
        }
        
        if load_all_on_start:
            self.load_all()
    
    def load_all(self) -> None:
        """Load all available instruments."""
        for symbol, name in self._common_symbols.items():
            instrument = self._create_instrument(symbol, name)
            self._instruments[instrument.id] = instrument
            self.add(instrument)
    
    def load_ids(self, instrument_ids: List[InstrumentId]) -> None:
        """
        Load instruments for the given instrument IDs.
        
        Parameters
        ----------
        instrument_ids : List[InstrumentId]
            The instrument IDs to load.
        """
        for instrument_id in instrument_ids:
            if instrument_id.venue != self._venue:
                continue
            
            symbol = instrument_id.symbol.value
            if symbol in self._instruments:
                continue
            
            # Get name from common symbols or use symbol as name
            name = self._common_symbols.get(symbol, symbol)
            instrument = self._create_instrument(symbol, name)
            self._instruments[instrument.id] = instrument
            self.add(instrument)
    
    def _create_instrument(self, symbol: str, name: str) -> Equity:
        """
        Create an Equity instrument for TWSE.
        
        Parameters
        ----------
        symbol : str
            The security symbol (e.g., "0050", "2330")
        name : str
            The instrument name
        
        Returns
        -------
        Equity
            The created instrument
        """
        instrument_id = InstrumentId(
            symbol=Symbol(symbol),
            venue=self._venue,
        )
        
        # TWSE equity specifications
        # Price precision: 2 decimal places
        # Tick size: NT$0.01 for stocks under NT$10
        #           NT$0.05 for stocks NT$10-50
        #           NT$0.10 for stocks NT$50-100
        #           NT$0.50 for stocks NT$100-500
        #           NT$1.00 for stocks NT$500-1000
        #           NT$5.00 for stocks over NT$1000
        # For simplicity, using NT$0.01 as base tick
        
        return Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol(symbol),
            currency=self._currency,
            price_precision=2,
            price_increment=Price.from_str("0.01"),
            lot_size=Quantity.from_int(1000),  # 1 lot = 1000 shares
            max_quantity=Quantity.from_int(999999000),
            min_quantity=Quantity.from_int(1000),
            ts_event=0,
            ts_init=0,
            info={"name": name},  # Store Chinese name in info
        )
    
    def find(self, instrument_id: InstrumentId) -> Optional[Equity]:
        """
        Find an instrument by its ID.
        
        Parameters
        ----------
        instrument_id : InstrumentId
            The instrument ID to find.
        
        Returns
        -------
        Equity or None
            The instrument if found, else None.
        """
        return self._instruments.get(instrument_id)
    
    def get_all(self) -> Dict[InstrumentId, Equity]:
        """
        Get all loaded instruments.
        
        Returns
        -------
        Dict[InstrumentId, Equity]
            All loaded instruments indexed by ID.
        """
        return self._instruments.copy()


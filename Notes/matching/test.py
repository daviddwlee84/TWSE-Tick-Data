# Fix fallback SimpleEngine to expose send_order with matching interface
try:
    from matching_engines import (
        BaseMatchingEngine,
        L3MatchingEngine,
        L2MatchingEngine,
        L1MatchingEngine,
        TradeTickMatchingEngine,
        BarMatchingEngine,
        Side,
        OrderType,
        FillModel,
    )

    fallback = False
except ModuleNotFoundError as e:
    raise e
    fallback = True

if fallback:
    from enum import Enum, auto
    from dataclasses import dataclass
    import time, random

    class Side(Enum):
        BUY = auto()
        SELL = auto()

        def opp(self):
            return Side.SELL if self is Side.BUY else Side.BUY

    class OrderType(Enum):
        LIMIT = auto()
        MARKET = auto()

    @dataclass
    class Trade:
        order_id: int
        price: float
        qty: float
        ts: float

    class SimpleEngine:
        _oid = 0

        def __init__(self):
            self.trades = []

        def _next(self):
            SimpleEngine._oid += 1
            return SimpleEngine._oid

        def send_order(self, side, qty, price=None, order_type=OrderType.LIMIT):
            oid = self._next()
            px = price if price is not None else (101 if side is Side.BUY else 100)
            self.trades.append(Trade(oid, px, qty, time.time()))
            return oid

        # Not modeling cancels or quote update for fallback
        def update_quote(self, *args, **kwargs):
            pass

        def feed_bar(self, *args, **kwargs):
            pass

    L3MatchingEngine = L2MatchingEngine = L1MatchingEngine = TradeTickMatchingEngine = (
        BarMatchingEngine
    ) = SimpleEngine

"""
測試流程:

1.	先掛兩筆賣單：
	•	10 股 @ 101
	•	5 股 @ 102
2.	再下一筆「買市價 8 股」，看不同撮合引擎怎麼撮。
3.	額外示範
	•	Trade-tick：只知道 last price ＝ 101，也能成一筆市價單
	•	Bar：把 OHLCV (100, 103, 99, 101) 切 4 段價路徑，再用 L1 邏輯觸發一筆買掛單
"""


# Helper to run scenario
def simple_scenario(engine: BaseMatchingEngine, name: str):
    engine.send_order(Side.SELL, qty=10, price=101)
    engine.send_order(Side.SELL, qty=5, price=102)
    engine.send_order(Side.BUY, qty=8, order_type=OrderType.MARKET)
    return {name: [(t.order_id, t.price, t.qty) for t in engine.trades]}


results = {}
results.update(simple_scenario(L3MatchingEngine(), "L3_MBO"))
results.update(simple_scenario(L2MatchingEngine(), "L2_MBP"))

l1 = L1MatchingEngine()
if hasattr(l1, "update_quote"):  # Only if real engine
    l1.update_quote(bid=100, bid_qty=20, ask=101, ask_qty=15)
results.update(simple_scenario(l1, "L1_MBP"))

# Trade-tick
tt = TradeTickMatchingEngine()
if hasattr(tt, "update_trade_tick"):
    if hasattr(tt, "update_trade_tick"):
        tt.update_trade_tick(price=101)
    tt.send_order(Side.BUY, qty=8, order_type=OrderType.MARKET)
    results["Trade_Tick"] = [(t.order_id, t.price, t.qty) for t in tt.trades]
else:
    tt.send_order(Side.BUY, qty=8)
    results["Trade_Tick"] = [(t.order_id, t.price, t.qty) for t in tt.trades]

bar = BarMatchingEngine()
if hasattr(bar, "feed_bar"):
    bar.feed_bar(open_=100, high=103, low=99, close=101, volume=400)
bar.send_order(Side.BUY, qty=10, price=99.5)
results["Bar"] = [(t.order_id, t.price, t.qty) for t in bar.trades]

print(results)
import ipdb

ipdb.set_trace()

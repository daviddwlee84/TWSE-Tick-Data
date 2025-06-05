from __future__ import annotations

"""Pure‑Python reference implementations of the matching/fill logic
originally implemented in Nautilus Trader's Cython ``matching_engine.pyx``.

The focus is *clarity of algorithmic flow* rather than raw speed.  Each
engine keeps just enough state to demonstrate how orders are matched under
different market‑data granularities:

* L3_MBO – per‑order FIFO queues (price & time priority)
* L2_MBP – aggregated depth at each price level (no order identity)
* L1_MBP – best‑bid / best‑offer only
* TRADE_TICK – no quote book; fills against last trade with 1‑tick slippage
* BAR       – derives a 4‑step price path from OHLCV and re‑uses L1 logic

They all share the same light‑weight *Order*, *Trade* and *FillModel* data
structures so that you can slot them into a notebook or unit‑test without
any external dependencies.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Deque, Dict, Generator, List, Optional
import heapq
import itertools
import random
import time


################################################################################
#  Common primitives                                                           #
################################################################################
class Side(Enum):
    BUY = auto()
    SELL = auto()

    @property
    def opp(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY


class OrderType(Enum):
    LIMIT = auto()
    MARKET = auto()


@dataclass
class Order:
    order_id: int
    side: Side
    qty: float
    price: Optional[float]  # None for market orders
    order_type: OrderType = OrderType.LIMIT
    ts: float = field(default_factory=time.time)


@dataclass
class Trade:
    order_id: int
    price: float
    qty: float
    ts: float


@dataclass
class FillModel:
    """Simple stochastic post‑fill adjustment.

    * ``prob_fill_on_limit`` – probability that *our* resting limit order at
      a price level actually gets a fill when that price is hit.
    * ``prob_slippage`` – if the order is aggressive (market) or crosses the
      spread, apply ±1 tick of slippage with this probability.
    """

    prob_fill_on_limit: float = 1.0
    prob_slippage: float = 0.0
    tick_size: float = 0.01

    def maybe_fill_resting(self) -> bool:
        return random.random() < self.prob_fill_on_limit

    def apply_slippage(self, px: float, side: Side) -> float:
        if random.random() < self.prob_slippage:
            return px + (self.tick_size * (1 if side is Side.BUY else -1))
        return px


################################################################################
#  Base class                                                                  #
################################################################################
class BaseMatchingEngine:
    """Abstract base – provides ID generation & fill‑model hookup."""

    _id_iter = itertools.count(1)

    def __init__(self, fill_model: Optional[FillModel] = None):
        self.fill = fill_model or FillModel()
        self.trades: List[Trade] = []

    # ---------------------------------------------------------------------
    # API surface expected by strategies
    # ---------------------------------------------------------------------
    def send_order(
        self,
        side: Side,
        qty: float,
        price: Optional[float] = None,
        order_type: OrderType = OrderType.LIMIT,
    ) -> int:
        oid = next(self._id_iter)
        order = Order(oid, side, qty, price, order_type)
        self._process_new_order(order)
        return oid

    def cancel(self, order_id: int):
        self._process_cancel(order_id)

    # ------------------------------------------------------------------
    # Implementation hooks – subclasses override
    # ------------------------------------------------------------------
    def _process_new_order(self, order: Order):
        raise NotImplementedError

    def _process_cancel(self, order_id: int):
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _record_trade(self, order: Order, qty: float, price: float):
        price = self.fill.apply_slippage(price, order.side)
        self.trades.append(Trade(order.order_id, price, qty, time.time()))


################################################################################
#  L3 – Market‑By‑Order (FIFO)                                                 #
################################################################################
class L3MatchingEngine(BaseMatchingEngine):
    """Per‑order FIFO matching."""

    def __init__(self, fill_model: Optional[FillModel] = None):
        super().__init__(fill_model)
        # price -> deque[Order] ; separate books for bid/ask
        self.book: Dict[Side, Dict[float, Deque[Order]]] = {
            Side.BUY: {},
            Side.SELL: {},
        }

    # ---------------- Core private helpers -----------------------------
    def _best_price(self, side: Side) -> Optional[float]:
        book = self.book[side]
        return (max(book) if side is Side.BUY else min(book)) if book else None

    def _pop_head(self, side: Side, px: float) -> Order:
        dq = self.book[side][px]
        order = dq.popleft()
        if not dq:
            del self.book[side][px]
        return order

    # ---------------- Incoming order processing ------------------------
    def _process_new_order(self, order: Order):
        if order.order_type is OrderType.MARKET:
            self._execute_market(order)
        else:
            self._execute_limit(order)

    def _execute_market(self, order: Order):
        opp = order.side.opp
        while order.qty > 0:
            best_px = self._best_price(opp)
            if best_px is None:
                break  # no liquidity
            head = self._pop_head(opp, best_px)
            tradable = min(order.qty, head.qty)
            self._record_trade(order, tradable, best_px)
            order.qty -= tradable
            head.qty -= tradable
            if head.qty > 0:  # head partially consumed – re‑insert
                self.book[opp].setdefault(best_px, deque()).appendleft(head)

    def _execute_limit(self, order: Order):
        opp = order.side.opp
        cross = (
            (lambda bp: bp <= order.price)
            if order.side is Side.BUY
            else (lambda bp: bp >= order.price)
        )
        # match while crosses
        while order.qty > 0:
            best_px = self._best_price(opp)
            if best_px is None or not cross(best_px):
                break
            head = self._pop_head(opp, best_px)
            tradable = min(order.qty, head.qty)
            self._record_trade(order, tradable, best_px)
            order.qty -= tradable
            head.qty -= tradable
            if head.qty > 0:
                self.book[opp].setdefault(best_px, deque()).appendleft(head)
        # any remainder rests in book
        if order.qty > 0:
            dq = self.book[order.side].setdefault(order.price, deque())
            dq.append(order)

    def _process_cancel(self, order_id: int):
        for book in self.book.values():
            for px, dq in list(book.items()):
                for i, o in enumerate(dq):
                    if o.order_id == order_id:
                        dq.remove(o)
                        if not dq:
                            del book[px]
                        return


from collections import deque


################################################################################
#  L2 – Market‑By‑Price (depth only)                                           #
################################################################################
class L2MatchingEngine(BaseMatchingEngine):
    """Size aggregation per price level; no order identity."""

    def __init__(
        self, fill_model: Optional[FillModel] = None, queue_random: bool = True
    ):
        super().__init__(fill_model)
        self.book: Dict[Side, Dict[float, float]] = {
            Side.BUY: {},
            Side.SELL: {},
        }
        self.queue_random = queue_random

    def _best_price(self, side: Side) -> Optional[float]:
        book = self.book[side]
        return (max(book) if side is Side.BUY else min(book)) if book else None

    def _remove_level_if_empty(self, side: Side, px: float):
        if self.book[side][px] <= 0:
            del self.book[side][px]

    # ------------------------------------------------------------------
    def _process_new_order(self, order: Order):
        if order.order_type is OrderType.MARKET:
            self._execute_market(order)
        else:
            self._execute_limit(order)

    def _execute_market(self, order: Order):
        opp = order.side.opp
        while order.qty > 0:
            best_px = self._best_price(opp)
            if best_px is None:
                break
            level_qty = self.book[opp][best_px]
            tradable = min(order.qty, level_qty)
            self._record_trade(order, tradable, best_px)
            self.book[opp][best_px] -= tradable
            self._remove_level_if_empty(opp, best_px)
            order.qty -= tradable

    def _execute_limit(self, order: Order):
        opp = order.side.opp
        cross = (
            (lambda bp: bp <= order.price)
            if order.side is Side.BUY
            else (lambda bp: bp >= order.price)
        )
        while order.qty > 0:
            best_px = self._best_price(opp)
            if best_px is None or not cross(best_px):
                break
            level_qty = self.book[opp][best_px]
            tradable = min(order.qty, level_qty)
            self._record_trade(order, tradable, best_px)
            self.book[opp][best_px] -= tradable
            self._remove_level_if_empty(opp, best_px)
            order.qty -= tradable
        if order.qty > 0:
            self.book[order.side][order.price] = (
                self.book[order.side].get(order.price, 0) + order.qty
            )

    def _process_cancel(self, order_id: int):
        pass  # Not modelled – no per‑order identity in L2


################################################################################
#  L1 – Best‑Bid/Offer only                                                    #
################################################################################
class L1MatchingEngine(BaseMatchingEngine):
    """Maintains only top of book (price & size)."""

    def __init__(self, fill_model: Optional[FillModel] = None):
        super().__init__(fill_model)
        self.best_bid: Optional[float] = None
        self.best_bid_qty: float = 0.0
        self.best_ask: Optional[float] = None
        self.best_ask_qty: float = 0.0

    # "Quote update" interface – strategy or data‑feed can update BBO
    def update_quote(self, bid: float, bid_qty: float, ask: float, ask_qty: float):
        self.best_bid, self.best_bid_qty = bid, bid_qty
        self.best_ask, self.best_ask_qty = ask, ask_qty

    # ------------------------------------------------------------------
    def _process_new_order(self, order: Order):
        if order.order_type is OrderType.MARKET:
            self._execute_market(order)
        else:
            self._execute_limit(order)

    def _execute_market(self, order: Order):
        price = self.best_ask if order.side is Side.BUY else self.best_bid
        size_ref = "best_ask_qty" if order.side is Side.BUY else "best_bid_qty"
        avail = getattr(self, size_ref)
        tradable = min(order.qty, avail)
        if tradable > 0:
            self._record_trade(order, tradable, price)
            setattr(self, size_ref, avail - tradable)

    def _execute_limit(self, order: Order):
        crosses = (
            (order.price >= self.best_ask)
            if order.side is Side.BUY
            else (order.price <= self.best_bid)
        )
        if crosses:
            # treat as market into top qty
            self._execute_market(
                Order(order.order_id, order.side, order.qty, None, OrderType.MARKET)
            )
            return
        # resting order – we only model fill probability when price touched
        if self.fill.maybe_fill_resting():
            # assume complete fill when price is hit (qty not modelled)
            px = order.price
            self._record_trade(order, order.qty, px)

    def _process_cancel(self, order_id: int):
        pass  # Not modelled – rests are virtual


################################################################################
#  Trade‑tick only                                                             #
################################################################################
class TradeTickMatchingEngine(BaseMatchingEngine):
    def __init__(
        self, fill_model: Optional[FillModel] = None, last_price: float | None = None
    ):
        super().__init__(fill_model)
        self.last_price = last_price

    def update_trade_tick(self, price: float):
        self.last_price = price

    def _process_new_order(self, order: Order):
        if self.last_price is None:
            return  # cannot fill – no price yet
        px = self.fill.apply_slippage(self.last_price, order.side)
        self._record_trade(order, order.qty, px)

    def _process_cancel(self, order_id: int):
        pass


################################################################################
#  Bar‑based engine – wraps L1                                                 #
################################################################################
class BarMatchingEngine(BaseMatchingEngine):
    """Creates an internal L1 engine and feeds it synthetic quotes
    following the 4‑step OHLCV rule (Open → High/Low → Low/High → Close)."""

    def __init__(self, tick_size: float = 0.01, fill_model: Optional[FillModel] = None):
        super().__init__(fill_model)
        self.l1 = L1MatchingEngine(fill_model)
        self.tick = tick_size

    def feed_bar(
        self, open_: float, high: float, low: float, close: float, volume: float
    ):
        seq = [open_]
        # decide high/low order: nearer to open comes first to minimise look‑ahead bias
        if abs(high - open_) < abs(low - open_):
            seq += [high, low]
        else:
            seq += [low, high]
        seq.append(close)
        size_slice = volume / 4 if volume else 0

        for px in seq:
            bid = round(px - self.tick / 2, 2)
            ask = round(px + self.tick / 2, 2)
            self.l1.update_quote(bid, size_slice, ask, size_slice)

    # Strategy interface proxies to inner L1
    def _process_new_order(self, order: Order):
        self.l1._process_new_order(order)
        self.trades.extend(self.l1.trades)
        self.l1.trades.clear()

    def _process_cancel(self, order_id: int):
        self.l1._process_cancel(order_id)

from nautilus_trader.model.instruments import Equity

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model import OrderBookDepth10, BookOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.core.nautilus_pyo3 import (
    OrderBookDepth10 as RustOrderBookDepth10,
    BookOrder as RustBookOrder,
    OrderSide as RustOrderSide,
    Price as RustPrice,
    Quantity as RustQuantity,
    InstrumentId as RustInstrumentId,
    Equity as RustEquity,
    Currency as RustCurrency,
)
from decimal import Decimal
import ast
from nautilus_trader.model.currencies import USD

# from typing import Final
# from nautilus_trader.model.objects import Currency

# Fiat currencies
# NTD: Final[Currency] = Currency.from_internal_map("NTD")
NTD = USD  # TODO: fix this ad-hoc workaround
TWSE = Venue("TWSE")


class TWSEProvider:

    @staticmethod
    def stock(symbol: str = "2330") -> Equity:
        return Equity(
            instrument_id=InstrumentId(symbol=Symbol(symbol), venue=TWSE),
            raw_symbol=Symbol(symbol),
            currency=NTD,
            price_precision=2,
            price_increment=Price.from_str("0.01"),
            lot_size=Quantity.from_int(1),
            ts_event=0,
            ts_init=0,
        )


import pandas as pd


class TWSEDataConverter:

    @staticmethod
    def _convert_transaction(raw_trans_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_trans_df.copy()
        df["timestamp"] = pd.to_datetime(df["trade_date"] + " " + df["trade_time"])
        df["instrument_id"] = df["securities_code"]
        df["price"] = df["trade_price"]
        df["quantity"] = df["trade_volume"]
        df["trade_id"] = df["trade_number"]
        df["side"] = df["buy_sell"].replace({"B": "BUY", "S": "SELL"})
        return df[
            ["timestamp", "instrument_id", "price", "quantity", "trade_id", "side"]
        ].set_index("timestamp")

    @staticmethod
    def _convert_order(raw_order_df: pd.DataFrame) -> pd.DataFrame:
        """
        https://github.com/nautechsystems/nautilus_trader/blob/develop/nautilus_trader/persistence/wranglers.pyx#L179
        https://github.com/nautechsystems/nautilus_trader/blob/develop/nautilus_trader/model/data.pyx#L1825
        """
        df = raw_order_df.copy()
        # TODO: not sure if this is correct...
        df["action"] = "ADD"  # Assuming all are new orders; adjust as necessary
        df["timestamp"] = pd.to_datetime(df["order_date"] + " " + df["order_time"])
        df["instrument_id"] = df["securities_code"]
        df["price"] = df["order_price"]
        df["size"] = df["changed_trade_volume"]

        # NOTE: this need to be an integer
        # df["order_id"] = df["order_number_ii"]
        df["order_id"] = df.index

        df["side"] = df["buy_sell"].map({"B": "BUY", "S": "SELL"})
        df["sequence"] = 0
        df["flags"] = 0
        return df[
            [
                "timestamp",
                "instrument_id",
                "action",
                "side",
                "price",
                "size",
                "order_id",
                "flags",
                "sequence",
            ]
        ].set_index("timestamp")

    @staticmethod
    def _snapshot_wrangler(raw_snapshot_df: pd.DataFrame) -> list[OrderBookDepth10]:
        """
        Improved usability of `OrderBookDepth10` by filling partial levels with null orders and zero counts
        https://github.com/nautechsystems/nautilus_trader/blob/7c25ea0ba187222c872e364a38b00f15e0e4f8d4/RELEASES.md?plain=1#L302
        https://github.com/nautechsystems/nautilus_trader/blob/7c25ea0ba187222c872e364a38b00f15e0e4f8d4/nautilus_core/model/src/python/data/depth.rs#L44-L70
        """
        results = []
        df = raw_snapshot_df.copy()

        for _, row in df.iterrows():
            instrument_id = InstrumentId.from_str(f"{row['securities_code']}.{TWSE}")
            sequence = row.name
            ts_now = int(
                pd.Timestamp(
                    row["display_date"] + " " + row["display_time"]
                ).timestamp()
                * 1e9
            )

            # Parse bid/ask price and volume data from the JSON-like strings
            bid_data = ast.literal_eval(row["buy_5_price_volume"])
            ask_data = ast.literal_eval(row["sell_5_price_volume"])

            # Create BookOrder objects for active bid levels (first 5)
            bids = [
                BookOrder(
                    side=OrderSide.BUY,
                    price=Price(Decimal(str(item["price"])), 2),
                    size=Quantity(Decimal(str(item["volume"])), 1),
                    order_id=sequence * 1000 + i,
                )
                for i, item in enumerate(bid_data, 1)
            ]

            # Add empty orders for remaining bid levels (with zero quantity)
            # Use the last valid price for empty levels
            last_valid_bid_price = bid_data[-1]["price"] if bid_data else 0

            # Fill remaining levels to reach 10 total
            remaining_bid_levels = 10 - len(bids)
            if remaining_bid_levels > 0:
                bids.extend(
                    [
                        BookOrder(
                            side=OrderSide.BUY,
                            price=Price(Decimal(str(last_valid_bid_price)), 2),
                            size=Quantity(Decimal("0"), 1),
                            order_id=sequence * 1000 + len(bid_data) + i,
                        )
                        for i in range(1, remaining_bid_levels + 1)
                    ]
                )

            # Create BookOrder objects for active ask levels (first 5)
            asks = [
                BookOrder(
                    side=OrderSide.SELL,
                    price=Price(Decimal(str(item["price"])), 2),
                    size=Quantity(Decimal(str(item["volume"])), 1),
                    order_id=sequence * 1000 + i + 500,
                )
                for i, item in enumerate(ask_data, 1)
            ]

            # Add empty orders for remaining ask levels (with zero quantity)
            # Use the last valid price for empty levels
            last_valid_ask_price = ask_data[-1]["price"] if ask_data else 0

            # Fill remaining levels to reach 10 total
            remaining_ask_levels = 10 - len(asks)
            if remaining_ask_levels > 0:
                asks.extend(
                    [
                        BookOrder(
                            side=OrderSide.SELL,
                            price=Price(Decimal(str(last_valid_ask_price)), 2),
                            size=Quantity(Decimal("0"), 1),
                            order_id=sequence * 1000 + len(ask_data) + i + 500,
                        )
                        for i in range(1, remaining_ask_levels + 1)
                    ]
                )

            # Create bid_counts and ask_counts lists
            bid_counts = [item["volume"] for item in bid_data]
            # Fill with zeros to reach 10 total
            bid_counts.extend([0] * (10 - len(bid_counts)))

            ask_counts = [item["volume"] for item in ask_data]
            # Fill with zeros to reach 10 total
            ask_counts.extend([0] * (10 - len(ask_counts)))

            # Create OrderBookDepth10 object with BookOrder objects
            results.append(
                # https://nautilustrader.io/docs/latest/api_reference/model/data/#class-orderbookdepth10
                OrderBookDepth10(
                    instrument_id=instrument_id,
                    bids=bids,
                    asks=asks,
                    bid_counts=bid_counts,
                    ask_counts=ask_counts,
                    # flags (uint8_t) – The record flags bit field, indicating event end and data information. A value of zero indicates no flags.
                    flags=0,  # TODO: not sure what's this
                    sequence=sequence,
                    ts_event=ts_now,
                    ts_init=ts_now,
                )
            )

        return results

    @staticmethod
    def _snapshot_wrangler_pyo3(
        raw_snapshot_df: pd.DataFrame,
    ) -> list[RustOrderBookDepth10]:
        """
        Improved usability of `OrderBookDepth10` by filling partial levels with null orders and zero counts
        https://github.com/nautechsystems/nautilus_trader/blob/7c25ea0ba187222c872e364a38b00f15e0e4f8d4/RELEASES.md?plain=1#L302
        https://github.com/nautechsystems/nautilus_trader/blob/7c25ea0ba187222c872e364a38b00f15e0e4f8d4/nautilus_core/model/src/python/data/depth.rs#L44-L70
        """
        results = []
        df = raw_snapshot_df.copy()

        for _, row in df.iterrows():
            instrument_id = RustInstrumentId.from_str(
                f"{row['securities_code']}.{TWSE}"
            )
            sequence = row.name
            ts_now = int(
                pd.Timestamp(
                    row["display_date"] + " " + row["display_time"]
                ).timestamp()
                * 1e9
            )

            # Parse bid/ask price and volume data from the JSON-like strings
            bid_data = ast.literal_eval(row["buy_5_price_volume"])
            ask_data = ast.literal_eval(row["sell_5_price_volume"])

            # Create BookOrder objects for active bid levels (first 5)
            bids = [
                RustBookOrder(
                    side=RustOrderSide.BUY,
                    price=RustPrice(Decimal(str(item["price"])), 2),
                    size=RustQuantity(Decimal(str(item["volume"])), 1),
                    order_id=sequence * 1000 + i,
                )
                for i, item in enumerate(bid_data, 1)
            ]

            # Add empty orders for remaining bid levels (with zero quantity)
            # Use the last valid price for empty levels
            last_valid_bid_price = bid_data[-1]["price"] if bid_data else 0

            # Fill remaining levels to reach 10 total
            remaining_bid_levels = 10 - len(bids)
            if remaining_bid_levels > 0:
                bids.extend(
                    [
                        RustBookOrder(
                            side=RustOrderSide.BUY,
                            price=RustPrice(Decimal(str(last_valid_bid_price)), 2),
                            size=RustQuantity(Decimal("0"), 1),
                            order_id=sequence * 1000 + len(bid_data) + i,
                        )
                        for i in range(1, remaining_bid_levels + 1)
                    ]
                )

            # Create BookOrder objects for active ask levels (first 5)
            asks = [
                RustBookOrder(
                    side=RustOrderSide.SELL,
                    price=RustPrice(Decimal(str(item["price"])), 2),
                    size=RustQuantity(Decimal(str(item["volume"])), 1),
                    order_id=sequence * 1000 + i + 500,
                )
                for i, item in enumerate(ask_data, 1)
            ]

            # Add empty orders for remaining ask levels (with zero quantity)
            # Use the last valid price for empty levels
            last_valid_ask_price = ask_data[-1]["price"] if ask_data else 0

            # Fill remaining levels to reach 10 total
            remaining_ask_levels = 10 - len(asks)
            if remaining_ask_levels > 0:
                asks.extend(
                    [
                        RustBookOrder(
                            side=RustOrderSide.SELL,
                            price=RustPrice(Decimal(str(last_valid_ask_price)), 2),
                            size=RustQuantity(Decimal("0"), 1),
                            order_id=sequence * 1000 + len(ask_data) + i + 500,
                        )
                        for i in range(1, remaining_ask_levels + 1)
                    ]
                )

            # Create bid_counts and ask_counts lists
            bid_counts = [item["volume"] for item in bid_data]
            # Fill with zeros to reach 10 total
            bid_counts.extend([0] * (10 - len(bid_counts)))

            ask_counts = [item["volume"] for item in ask_data]
            # Fill with zeros to reach 10 total
            ask_counts.extend([0] * (10 - len(ask_counts)))

            # Create OrderBookDepth10 object with BookOrder objects
            results.append(
                # https://nautilustrader.io/docs/latest/api_reference/model/data/#class-orderbookdepth10
                RustOrderBookDepth10(
                    instrument_id=instrument_id,
                    bids=bids,
                    asks=asks,
                    bid_counts=bid_counts,
                    ask_counts=ask_counts,
                    # flags (uint8_t) – The record flags bit field, indicating event end and data information. A value of zero indicates no flags.
                    flags=0,  # TODO: not sure what's this
                    sequence=sequence,
                    ts_event=ts_now,
                    ts_init=ts_now,
                )
            )

        return results


def _test_transaction():
    nautilus_trans_df = TWSEDataConverter._convert_transaction(
        pd.read_csv(
            "./transaction/transaction.csv", index_col=0, dtype={"securities_code": str}
        )
    )
    print(nautilus_trans_df)
    nautilus_trans_df.to_csv("./transaction/nautilus_9945.csv")

    from nautilus_trader.persistence.loaders import CSVTickDataLoader

    nautilus_trans_df = CSVTickDataLoader.load(
        "./transaction/nautilus_9945.csv", dtype={"instrument_id": str}
    )
    print(nautilus_trans_df)

    from nautilus_trader.persistence.wranglers import TradeTickDataWrangler

    instrument = TWSEProvider.stock("9945")
    print(instrument)
    trans_ticks = TradeTickDataWrangler(instrument).process(nautilus_trans_df)
    print(trans_ticks)

    # TODO

    # import os
    # from nautilus_trader.persistence.catalog import ParquetDataCatalog

    # os.makedirs(catalog_path := "transaction/catalog", exist_ok=True)

    # catalog = ParquetDataCatalog(catalog_path)
    # catalog.write_data([instrument])
    # catalog.write_data(trans_ticks)
    # print(catalog.instruments())
    # print(catalog.trade_ticks())

    # from nautilus_trader.backtest.node import (
    #     BacktestDataConfig,
    #     BacktestVenueConfig,
    #     BacktestRunConfig,
    #     BacktestEngineConfig,
    #     BacktestNode,
    # )
    # from nautilus_trader.config import ImportableStrategyConfig, LoggingConfig
    # from nautilus_trader.model import OrderBookDelta
    # from decimal import Decimal

    # strategies = [
    #     ImportableStrategyConfig(
    #         strategy_path="nautilus_trader.examples.strategies.signal_strategy:SignalStrategy",
    #         config_path="nautilus_trader.examples.strategies.signal_strategy:SignalStrategyConfig",
    #         config={
    #             "instrument_id": instrument.id,
    #         },
    #     ),
    # ]

    import ipdb

    ipdb.set_trace()


def _test_order():
    nautilus_order_df = TWSEDataConverter._convert_order(
        pd.read_csv("./order/order.csv", index_col=0, dtype={"securities_code": str})
    )
    print(nautilus_order_df)
    nautilus_order_df.to_csv("./order/nautilus_0050.csv")

    from nautilus_trader.persistence.loaders import CSVTickDataLoader

    nautilus_order_df = CSVTickDataLoader.load(
        "./order/nautilus_0050.csv", dtype={"instrument_id": str}
    )
    print(nautilus_order_df)

    from nautilus_trader.persistence.wranglers import OrderBookDeltaDataWrangler

    instrument = TWSEProvider.stock("OO50")
    print(instrument)

    order_ticks = OrderBookDeltaDataWrangler(instrument).process(nautilus_order_df)
    print(order_ticks)

    # TODO

    # import os
    # from nautilus_trader.persistence.catalog import ParquetDataCatalog

    # os.makedirs(catalog_path := "order/catalog", exist_ok=True)

    # catalog = ParquetDataCatalog(catalog_path)
    # catalog.write_data([instrument])
    # catalog.write_data(order_ticks)
    # print(catalog.instruments())
    # print(catalog.order_book_deltas())

    # # book_type = "L2_MBP"  # Ensure data book type matches venue book type

    # from nautilus_trader.backtest.node import (
    #     BacktestDataConfig,
    #     BacktestVenueConfig,
    #     BacktestRunConfig,
    #     BacktestEngineConfig,
    #     BacktestNode,
    # )
    # from nautilus_trader.config import ImportableStrategyConfig, LoggingConfig
    # from nautilus_trader.model import OrderBookDelta
    # from decimal import Decimal

    # data_configs = [
    #     BacktestDataConfig(
    #         catalog_path=catalog_path,
    #         data_cls=OrderBookDelta,
    #         instrument_id=instrument.id,
    #         # start_time=start,  # Run across all data
    #         # end_time=end,  # Run across all data
    #     )
    # ]

    # venues_configs = [
    #     BacktestVenueConfig(
    #         name="TWSE",
    #         oms_type="NETTING",
    #         account_type="CASH",
    #         base_currency=None,
    #         starting_balances=["20 USD"],
    #         book_type=book_type,  # <-- Venues book type
    #     )
    # ]

    # strategies = [
    #     ImportableStrategyConfig(
    #         strategy_path="nautilus_trader.examples.strategies.orderbook_imbalance:OrderBookImbalance",
    #         config_path="nautilus_trader.examples.strategies.orderbook_imbalance:OrderBookImbalanceConfig",
    #         config={
    #             "instrument_id": instrument.id,
    #             "book_type": book_type,
    #             "max_trade_size": Decimal("1.000"),
    #             "min_seconds_between_triggers": 1.0,
    #         },
    #     ),
    # ]

    # NautilusTrader currently exceeds the rate limit for Jupyter notebook logging (stdout output),
    # this is why the `log_level` is set to "ERROR". If you lower this level to see
    # more logging then the notebook will hang during cell execution. A fix is currently
    # being investigated which involves either raising the configured rate limits for
    # Jupyter, or throttling the log flushing from Nautilus.
    # https://github.com/jupyterlab/jupyterlab/issues/12845
    # https://github.com/deshaw/jupyterlab-limit-output
    # config = BacktestRunConfig(
    #     engine=BacktestEngineConfig(
    #         strategies=strategies,
    #         logging=LoggingConfig(log_level="ERROR"),
    #         # logging=LoggingConfig(log_level="INFO"),
    #     ),
    #     data=data_configs,
    #     venues=venues_configs,
    # )

    # # BlockingIOError: [Errno 35] write could not complete without blocking
    # print(config)

    # import ipdb

    # ipdb.set_trace()

    # node = BacktestNode(configs=[config])

    # result = node.run()

    # print(result)

    # import ipdb

    # ipdb.set_trace()

    # from nautilus_trader.backtest.engine import BacktestEngine
    # from nautilus_trader.config import RiskEngineConfig

    # engine = BacktestEngine(
    #     config=BacktestEngineConfig(
    #         trader_id="Test-Trader",
    #         logging=LoggingConfig(log_level="INFO"),
    #         risk_engine=RiskEngineConfig(bypass=True),
    #     )
    # )
    # print(engine.trader)

    import ipdb

    ipdb.set_trace()


def _test_snapshot():

    def print_orderbook_depth(depth: OrderBookDepth10):
        print(f"\nOrder Book Update (Sequence: {depth.sequence})")
        print("Bids".ljust(30) + "Asks")
        print("-" * 60)

        for i in range(10):
            bid = f"${depth.bids[i].price.as_double():.2f} @ {depth.bids[i].size.as_double():.4f} ({depth.bid_counts[i]})"
            ask = f"${depth.asks[i].price.as_double():.2f} @ {depth.asks[i].size.as_double():.4f} ({depth.ask_counts[i]})"
            print(f"{bid.ljust(30)}{ask}")

    # NOTE: nautilus trader is now rewriting Cython to PyO3

    python_order_book_snapshots = TWSEDataConverter._snapshot_wrangler(
        pd.read_csv(
            "./snapshot/snapshot.csv", index_col=0, dtype={"securities_code": str}
        )
    )

    for python_snapshot in python_order_book_snapshots:
        print_orderbook_depth(python_snapshot)

    order_book_snapshots = TWSEDataConverter._snapshot_wrangler_pyo3(
        pd.read_csv(
            "./snapshot/snapshot.csv", index_col=0, dtype={"securities_code": str}
        )
    )

    for snapshot in order_book_snapshots:
        print_orderbook_depth(snapshot)

    import os
    from nautilus_trader.persistence.catalog import ParquetDataCatalog

    os.makedirs(catalog_path := "snapshot/catalog", exist_ok=True)

    catalog = ParquetDataCatalog(catalog_path)
    catalog.write_data(order_book_snapshots)
    print(catalog.order_book_depth10())

    catalog.write_data(
        [
            Equity(
                instrument_id=InstrumentId(
                    symbol=Symbol(snapshot.instrument_id.symbol.value),
                    venue=TWSE,
                ),
                raw_symbol=Symbol(snapshot.instrument_id.symbol.value),
                currency=NTD,
                price_precision=2,
                price_increment=Price.from_str("0.01"),
                lot_size=Quantity.from_int(1),
                ts_event=0,
                ts_init=0,
            )
            # RustEquity(
            #     id=snapshot.instrument_id,
            #     raw_symbol=snapshot.instrument_id.symbol,
            #     currency=RustCurrency.from_str("NTD"),
            #     price_precision=2,
            #     price_increment=RustPrice.from_str("0.01"),
            #     lot_size=RustQuantity.from_int(1),
            #     ts_event=0,
            #     ts_init=0,
            # )
        ]
    )
    print(catalog.instruments())
    import ipdb

    ipdb.set_trace()


if __name__ == "__main__":
    # _test_transaction()
    # _test_order()
    _test_snapshot()

    # https://nautilustrader.io/docs/latest/concepts/strategies#data-handling

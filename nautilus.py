from typing import Final
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity

from nautilus_trader.model.currencies import USD

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
    def _convert_transaction(raw_order_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_order_df.copy()
        df["timestamp"] = pd.to_datetime(df["trade_date"] + " " + df["trade_time"])
        df["price"] = df["trade_price"]
        df["quantity"] = df["trade_volume"]
        df["trade_id"] = df["trade_number"]
        df["side"] = df["buy_sell"].replace({"B": "BUY", "S": "SELL"})
        return df[["timestamp", "price", "quantity", "trade_id", "side"]].set_index(
            "timestamp"
        )


def _test_transaction():
    nautilus_trans_df = TWSEDataConverter._convert_transaction(
        pd.read_csv("./transaction/transaction.csv", index_col=0)
    )
    print(nautilus_trans_df)
    nautilus_trans_df.to_csv("./transaction/nautilus_9945.csv")

    from nautilus_trader.persistence.loaders import CSVTickDataLoader

    nautilus_trans_df = CSVTickDataLoader.load("./transaction/nautilus_9945.csv")
    print(nautilus_trans_df)

    from nautilus_trader.persistence.wranglers import TradeTickDataWrangler

    instrument = TWSEProvider.stock("9945")
    print(instrument)
    trans_ticks = TradeTickDataWrangler(instrument).process(nautilus_trans_df)
    print(trans_ticks)

    import ipdb

    ipdb.set_trace()


if __name__ == "__main__":
    _test_transaction()

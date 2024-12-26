from typing import List, Literal, Union
from pydantic import BaseModel, Field, field_validator
import datetime
import pandas as pd


class Helper:
    @staticmethod
    def parse_sign_and_int(value: str) -> int:
        """
        Takes a string with leading + or - and converts the rest to int.
        Example: "+0000001000" -> 1000, "-0000000500" -> -500
        """
        if not value:
            return 0
        sign_char = value[0]
        numeric_str = value[1:]
        magnitude = int(numeric_str)
        return magnitude if sign_char != "-" else -magnitude

    @staticmethod
    def parse_6digit_price(value: str) -> float:
        """
        Interpret a 6-digit numeric string as a price with 2 decimals.
        E.g. "002900" -> int("002900") -> 2900 -> /100.0 -> 29.0
        E.g. "007835" -> 7835 -> /100.0 -> 78.35
        """
        value_str = value.strip()
        if not value_str:
            return 0.0
        int_val = int(value_str)
        return int_val / 100.0

    @staticmethod
    def parse_numeric_float_direct(value: str) -> float:
        """
        Parse a string as a float directly (e.g. "0078.35" -> 78.35).
        Sometimes your data might actually contain a decimal point.
        """
        return float(value.strip())

    @staticmethod
    def parse_date_yyyymmdd(value: str) -> datetime.date:
        """
        Parse YYYYMMDD into datetime.date
        e.g. "20161230" -> date(2016,12,30)
        """
        return datetime.datetime.strptime(value.strip(), "%Y%m%d").date()

    @staticmethod
    def parse_time_hhmmss(value: str) -> datetime.time:
        """
        Parse HHMMSS into datetime.time (ignoring extra characters)
        e.g. "08300110" -> 08:30:01 (we discard last 2 digits if used for partial seconds)
        """
        # We'll take the first 6 digits only, ignoring anything beyond.
        main_part = value.strip()[:6]  # "HHMMSS"
        if len(main_part) < 6:
            # If incomplete, parse as best we can or raise an error
            # For simplicity, we handle the short case
            main_part = main_part.ljust(6, "0")  # pad to HHMMSS
        return datetime.datetime.strptime(main_part, "%H%M%S").time()

    @staticmethod
    def parse_5_price_volume(value: str) -> List[dict]:
        """
        Parse a 70-char field containing 5 pairs of (price(6), volume(8)).
        Return a list of 5 dictionaries, each with keys {"price": float, "volume": int}.

        We'll parse the 6-digit price as "2 decimal digits".
        e.g. "002860" -> 28.60
        """
        pairs = []
        if len(value) != 70:
            return pairs  # or raise an error

        for i in range(5):
            start = i * 14
            price_str = value[start : start + 6]  # 6 chars
            volume_str = value[start + 6 : start + 14]  # 8 chars

            p_float = Helper.parse_6digit_price(price_str)
            v_int = int(volume_str)

            pairs.append({"price": p_float, "volume": v_int})
        return pairs

    @staticmethod
    def list_model_into_df(model_list: List[BaseModel]) -> pd.DataFrame:
        return pd.DataFrame([record.model_dump() for record in model_list])


class TwseRawOrderBook(BaseModel):
    """
    Pydantic model for TWSE Order Book Log (委託檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    # 1-8 (length=8)
    order_date: str = Field(..., description="委託日期 (YYYYMMDD)")

    # 9-14 (length=6)
    securities_code: str = Field(..., description="證券代號")

    # 15 (length=1)
    buy_sell: str = Field(..., description="買賣別 (B=買, S=賣)")

    # 16 (length=1)
    trade_type_code: str = Field(
        ..., description="交易種類代號 (0=普通, 1=鉅額, 2=零股)"
    )

    # 17-24 (length=8)
    order_time: str = Field(..., description="委託時間 (HHMMSSxx)")

    # 25-29 (length=5)
    order_number_ii: str = Field(..., description="成家及委託檔連結代碼二")

    # 30 (length=1)
    changed_trade_code: str = Field(..., description="更改後交易代號")

    # 31-37 (length=7)
    order_price: str = Field(..., description="委託價格")

    # 38-48 (length=11)
    changed_trade_volume: str = Field(..., description="更改後股數 (正負號+數字)")

    # 49 (length=1)
    order_type_code: str = Field(..., description="委託種類代號")

    # 50 (length=1)
    notes_investors_order_channel: str = Field(..., description="下單通路註記")

    # 51-54 (length=4)
    order_report_print: str = Field(..., description="委託回報印表機")

    # 55 (length=1)
    type_of_investor: str = Field(..., description="投資人屬性")

    # 56-59 (length=4)
    order_number_i: str = Field(..., description="成家及委託檔連結代碼一 (證券商代號)")


class TwseOrderBook(BaseModel):
    """
    Pydantic model for TWSE Order Book Log (委託檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    order_date: datetime.date = Field(..., description="委託日期 (YYYYMMDD)")
    securities_code: str = Field(..., description="證券代號")
    # https://docs.pydantic.dev/1.10/usage/types/#literal-type
    buy_sell: Literal["B", "S"] = Field(..., description="買賣別 (B=買, S=賣)")
    trade_type_code: Literal["0", "1", "2"] = Field(
        ..., description="交易種類代號 (0=普通, 1=鉅額, 2=零股)"
    )
    order_time: datetime.time = Field(..., description="委託時間 (HHMMSSxx)")
    order_number_ii: str = Field(..., description="成家及委託檔連結代碼二")
    changed_trade_code: Literal["1", "2", "3", "4", "5", "6"] = Field(
        ..., description="更改後交易代號"
    )
    order_price: float = Field(..., description="委託價格")
    changed_trade_volume: int = Field(..., description="更改後股數 (正負號+數字)")
    order_type_code: str = Field(..., description="委託種類代號")
    notes_investors_order_channel: str = Field(..., description="下單通路註記")
    order_report_print: str = Field(..., description="委託回報印表機")
    type_of_investor: str = Field(..., description="投資人屬性")
    order_number_i: str = Field(..., description="成家及委託檔連結代碼一 (證券商代號)")

    # Add Pydantic validators
    # https://docs.pydantic.dev/2.10/concepts/validators/#field-before-validator
    # https://docs.pydantic.dev/2.10/concepts/validators/#using-the-decorator-pattern
    @field_validator("order_date", mode="before")
    @classmethod
    def parse_order_date(cls, v: str) -> datetime.date:
        return Helper.parse_date_yyyymmdd(v)

    @field_validator("securities_code", mode="before")
    @classmethod
    def strip_securities_code(cls, v: str) -> str:
        return v.rstrip()

    @field_validator("order_time", mode="before")
    @classmethod
    def parse_order_time(cls, v: str) -> datetime.time:
        return Helper.parse_time_hhmmss(v)

    @field_validator("order_price", mode="before")
    @classmethod
    def parse_order_price(cls, v: str) -> float:
        # If your data has a decimal point, do this:
        return Helper.parse_numeric_float_direct(v)
        # If your data is strictly 6-digit numeric, do:
        # return Helper.parse_6digit_price(v)

    @field_validator("changed_trade_volume", mode="before")
    @classmethod
    def parse_changed_trade_volume(cls, v: str) -> int:
        return Helper.parse_sign_and_int(v)


class TwseRawSnapshot(BaseModel):
    """
    Pydantic model for TWSE 5-level snapshot (揭示檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    # 1-6 (length=6)
    securities_code: str = Field(..., description="證券代號")

    # 7-14 (length=8)
    display_time: str = Field(..., description="揭示時間 (HHMMSSxx or similar)")

    # 15 (length=1)
    remark: str = Field(..., description="揭示註記 (空白, T, S, A)")

    # 16 (length=1)
    trend_flag: str = Field(..., description="趨勢註記 (空白, R, F)")

    # 17 (length=1)
    match_flag: str = Field(..., description="成交揭示 (空白, Y, S)")

    # 18 (length=1)
    trade_upper_lower_limit: str = Field(..., description="成交漲跌停註記 (空白, R, F)")

    # 19-24 (length=6)
    trade_price: str = Field(..., description="成交價格")

    # 25-32 (length=8)
    transaction_volume: str = Field(..., description="成交張數")

    # 33 (length=1)
    buy_tick_size: str = Field(..., description="檔位數(買進)")

    # 34 (length=1)
    buy_upper_lower_limit: str = Field(..., description="漲跌停註記(買進)")

    # 35-104 (length=70)
    buy_5_price_volume: str = Field(..., description="5檔買進價格及張數")

    # 105 (length=1)
    sell_tick_size: str = Field(..., description="檔位數(賣出)")

    # 106 (length=1)
    sell_upper_lower_limit: str = Field(..., description="漲跌停註記(賣出)")

    # 107-176 (length=70)
    sell_5_price_volume: str = Field(..., description="5檔賣出價格及張數")

    # 177-184 (length=8)
    display_date: str = Field(..., description="揭示日期 (YYYYMMDD)")

    # 185-186 (length=2)
    match_staff: str = Field(..., description="撮合人員 (2 characters)")


class TwseSnapshot(BaseModel):
    """
    Pydantic model for TWSE 5-level snapshot (揭示檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    securities_code: str = Field(..., description="證券代號")
    display_time: datetime.time = Field(
        ..., description="揭示時間 (HHMMSSxx or similar)"
    )
    remark: Literal[" ", "T", "S", "A"] = Field(
        ..., description="揭示註記 (空白, T, S, A)"
    )
    trend_flag: Literal[" ", "R", "F"] = Field(..., description="趨勢註記 (空白, R, F)")
    match_flag: Literal[" ", "Y", "S"] = Field(..., description="成交揭示 (空白, Y, S)")
    trade_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="成交漲跌停註記 (空白, R, F)"
    )
    trade_price: float = Field(..., description="成交價格")
    transaction_volume: int = Field(..., description="成交張數")
    buy_tick_size: int = Field(..., description="檔位數(買進)")
    buy_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="漲跌停註記(買進)"
    )
    buy_5_price_volume: List[dict] = Field(
        ..., description="List of 5 dict: [{price, volume}, ...]"
    )
    sell_tick_size: int = Field(..., description="檔位數(賣出)")
    sell_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="漲跌停註記(賣出)"
    )
    sell_5_price_volume: List[dict] = Field(
        ..., description="List of 5 dict: [{price, volume}, ...]"
    )
    display_date: datetime.date = Field(..., description="揭示日期 (YYYYMMDD)")
    match_staff: str = Field(..., description="撮合人員 (2 characters)")

    @field_validator("securities_code", mode="before")
    @classmethod
    def strip_securities_code(cls, v: str) -> str:
        return v.rstrip()

    @field_validator("display_time", mode="before")
    @classmethod
    def parse_display_time(cls, v: str) -> datetime.time:
        return Helper.parse_time_hhmmss(v)

    @field_validator("trade_price", mode="before")
    @classmethod
    def parse_trade_price(cls, v: str) -> float:
        # For 6-digit numeric
        return Helper.parse_6digit_price(v)
        # If there's a real decimal point, you might do:
        # return Helper.parse_numeric_float_direct(v)

    @field_validator("transaction_volume", mode="before")
    @classmethod
    def parse_transaction_volume(cls, v: str) -> int:
        return int(v)

    @field_validator("buy_tick_size", "sell_tick_size", mode="before")
    @classmethod
    def _parse_tick_size(cls, v: str) -> int:
        return int(v.strip() or 0)

    @field_validator("buy_5_price_volume", "sell_5_price_volume", mode="before")
    @classmethod
    def _parse_5_price_volume(cls, v: str) -> List[dict]:
        return Helper.parse_5_price_volume(v)

    @field_validator("display_date", mode="before")
    @classmethod
    def parse_display_date(cls, v: str) -> datetime.date:
        return Helper.parse_date_yyyymmdd(v)


class TwseRawTransaction(BaseModel):
    """
    Pydantic model for TWSE transaction (成交檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    # 1-8 (length=8)
    trade_date: str = Field(..., description="成交日期 (YYYYMMDD)")

    # 9-14 (length=6)
    securities_code: str = Field(..., description="證券代號")

    # 15 (length=1)
    buy_sell: str = Field(..., description="買賣別 (B=買, S=賣)")

    # 16 (length=1)
    trade_type_code: str = Field(
        ..., description="交易種類代號 (0=普通, 1=鉅額, 2=零股)"
    )

    # 17-24 (length=8)
    trade_time: str = Field(..., description="成交時間 (HHMMSSxx, etc.)")

    # 25-32 (length=8)
    trade_number: str = Field(..., description="成交序號")

    # 33-37 (length=5)
    order_number_ii: str = Field(..., description="成交及委託檔連結代碼二")

    # 38-44 (length=7)
    trade_price: str = Field(..., description="成交價格")

    # 45-53 (length=9)
    trade_volume: str = Field(..., description="成交股數")

    # 54-57 (length=4)
    trading_report_print: str = Field(..., description="成交回報印表機")

    # 58 (length=1)
    order_type_code: str = Field(..., description="委託種類代號")

    # 59 (length=1)
    type_of_investor: str = Field(..., description="投資人屬性")

    # 60-63 (length=4)
    order_number_i: str = Field(..., description="成交及委託檔連結代碼一")


class TwseTransaction(BaseModel):
    """
    Pydantic model for TWSE transaction (成交檔).
    Each field is mapped according to the 1-based positions specified in the datasheet.
    """

    trade_date: datetime.date = Field(..., description="成交日期 (YYYYMMDD)")
    securities_code: str = Field(..., description="證券代號")
    buy_sell: Literal["B", "S"] = Field(..., description="買賣別 (B=買, S=賣)")
    trade_type_code: Literal["0", "1", "2"] = Field(
        ..., description="交易種類代號 (0=普通, 1=鉅額, 2=零股)"
    )
    trade_time: datetime.time = Field(..., description="成交時間 (HHMMSSxx, etc.)")
    trade_number: str = Field(..., description="成交序號")
    order_number_ii: str = Field(..., description="成交及委託檔連結代碼二")
    trade_price: float = Field(..., description="成交價格")
    trade_volume: int = Field(..., description="成交股數")
    trading_report_print: str = Field(..., description="成交回報印表機")
    order_type_code: Literal["0", "1", "2", "3", "4", "5", "6"] = Field(
        ..., description="委託種類代號"
    )
    type_of_investor: Literal["M", "F", "I", "J"] = Field(..., description="投資人屬性")
    order_number_i: str = Field(..., description="成交及委託檔連結代碼一")

    @field_validator("trade_date", mode="before")
    @classmethod
    def parse_trade_date(cls, v: str) -> datetime.date:
        return Helper.parse_date_yyyymmdd(v)

    @field_validator("securities_code", mode="before")
    @classmethod
    def strip_securities_code(cls, v: str) -> str:
        return v.rstrip()

    @field_validator("trade_time", mode="before")
    @classmethod
    def parse_trade_time(cls, v: str) -> datetime.time:
        return Helper.parse_time_hhmmss(v)

    @field_validator("trade_price", mode="before")
    @classmethod
    def parse_trade_price(cls, v: str) -> float:
        # If 6-digit numeric:
        # return Helper.parse_6digit_price(v)
        # or if your real data has decimal points:
        return Helper.parse_numeric_float_direct(v)

    @field_validator("trade_volume", mode="before")
    @classmethod
    def parse_trade_volume(cls, v: str) -> int:
        return int(v)


class TwseTickParser:

    _ODR_LEN: int = 59
    _DSP_LEN: int = 186
    _MTH_LEN: int = 63

    def __init__(self, raw: bool = False):
        self.raw = raw

    @staticmethod
    def parse_line_to_order(
        line: str, raw: bool = False
    ) -> Union[TwseOrderBook, TwseRawOrderBook]:
        """
        Given a 59-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        cls = TwseRawOrderBook if raw else TwseOrderBook
        return cls(
            order_date=line[0:8],  # 1-8
            securities_code=line[8:14],  # 9-14
            buy_sell=line[14:15],  # 15
            trade_type_code=line[15:16],  # 16
            order_time=line[16:24],  # 17-24
            order_number_ii=line[24:29],  # 25-29
            changed_trade_code=line[29:30],  # 30
            order_price=line[30:37],  # 31-37
            changed_trade_volume=line[37:48],  # 38-48
            order_type_code=line[48:49],  # 49
            notes_investors_order_channel=line[49:50],  # 50
            order_report_print=line[50:54],  # 51-54
            type_of_investor=line[54:55],  # 55
            order_number_i=line[55:59],  # 56-59
        )

    def load_odr_file(
        self, filepath: str
    ) -> List[Union[TwseOrderBook, TwseRawOrderBook]]:
        """
        Open the `odryyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseRawOrderBook model instances.
        """
        records = []
        with open(filepath, "rb") as f:
            for raw_line in f:
                # Strip trailing newline or carriage-return
                line = raw_line.rstrip(b"\r\n")
                # Ensure the line is 59 bytes
                if len(line) != 59:
                    # In production, you might raise an exception or skip
                    continue
                # Decode from bytes to string
                decoded_line = line.decode("utf-8", errors="replace")

                # Parse into our Pydantic model
                record = self.parse_line_to_order(decoded_line, raw=self.raw)
                records.append(record)
        return records

    @staticmethod
    def parse_line_to_snapshot(
        line: str, raw: bool = False
    ) -> Union[TwseSnapshot, TwseRawSnapshot]:
        """
        Given a 186-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        cls = TwseRawSnapshot if raw else TwseSnapshot
        return cls(
            securities_code=line[0:6],  # 1-6
            display_time=line[6:14],  # 7-14
            remark=line[14:15],  # 15
            trend_flag=line[15:16],  # 16
            match_flag=line[16:17],  # 17
            trade_upper_lower_limit=line[17:18],  # 18
            trade_price=line[18:24],  # 19-24
            transaction_volume=line[24:32],  # 25-32
            buy_tick_size=line[32:33],  # 33
            buy_upper_lower_limit=line[33:34],  # 34
            buy_5_price_volume=line[34:104],  # 35-104
            sell_tick_size=line[104:105],  # 105
            sell_upper_lower_limit=line[105:106],  # 106
            sell_5_price_volume=line[106:176],  # 107-176
            display_date=line[176:184],  # 177-184
            match_staff=line[184:186],  # 185-186
        )

    def load_dsp_file(
        self, filepath: str
    ) -> List[Union[TwseSnapshot, TwseRawSnapshot]]:
        """
        Open the `dspyyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseRawSnapshot model instances.
        """
        records = []
        with open(filepath, "rb") as f:
            for raw_line in f:
                # Strip trailing newline or carriage-return
                line = raw_line.rstrip(b"\r\n")
                # Ensure the line is 186 bytes
                if len(line) != 186:
                    # In production, you might raise an exception or skip
                    continue
                # Decode from bytes to string
                decoded_line = line.decode("utf-8", errors="replace")

                # Parse into our Pydantic model
                record = self.parse_line_to_snapshot(decoded_line, raw=self.raw)
                records.append(record)
        return records

    @staticmethod
    def parse_line_to_transaction(
        line: str, raw: bool = False
    ) -> Union[TwseTransaction, TwseRawTransaction]:
        """
        Given a 63-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        cls = TwseRawTransaction if raw else TwseTransaction
        return cls(
            trade_date=line[0:8],  # 1-8
            securities_code=line[8:14],  # 9-14
            buy_sell=line[14:15],  # 15
            trade_type_code=line[15:16],  # 16
            trade_time=line[16:24],  # 17-24
            trade_number=line[24:32],  # 25-32
            order_number_ii=line[32:37],  # 33-37
            trade_price=line[37:44],  # 38-44
            trade_volume=line[44:53],  # 45-53
            trading_report_print=line[53:57],  # 54-57
            order_type_code=line[57:58],  # 58
            type_of_investor=line[58:59],  # 59
            order_number_i=line[59:63],  # 60-63
        )

    def load_mth_file(
        self, filepath: str
    ) -> List[Union[TwseTransaction, TwseRawTransaction]]:
        """
        Open the `mthyyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseRawTransaction model instances.
        """
        records = []
        with open(filepath, "rb") as f:
            for raw_line in f:
                # Strip trailing newline/carriage-return
                line = raw_line.rstrip(b"\r\n")
                # Ensure the line is 63 bytes
                if len(line) != 63:
                    # In production, you might raise an exception or skip
                    continue
                # Decode from bytes to string
                decoded_line = line.decode("utf-8", errors="replace")

                # Parse into our Pydantic model
                record = self.parse_line_to_transaction(decoded_line, raw=self.raw)
                records.append(record)
        return records


if __name__ == "__main__":

    def _load_all_type(parser: TwseTickParser):
        # Print out the parsed results (for demonstration)
        for i, record in enumerate(parser.load_odr_file("order/odr"), start=1):
            print(f"Order Record #{i}:")
            print(record.model_dump_json(indent=2))
            break
        for i, record in enumerate(parser.load_dsp_file("snapshot/Sample"), start=1):
            print(f"Snapshot Record #{i}:")
            print(record.model_dump_json(indent=2))
            break
        for i, record in enumerate(parser.load_mth_file("transaction/mth"), start=1):
            print(f"Transaction Record #{i}:")
            print(record.model_dump_json(indent=2))
            break

    def _load_all_type_as_df(parser: TwseTickParser):
        print(order_df := Helper.list_model_into_df(parser.load_odr_file("order/odr")))
        order_df.to_csv("order/order.csv")
        print(
            market_df := Helper.list_model_into_df(
                parser.load_dsp_file("snapshot/Sample")
            )
        )
        market_df.to_csv("snapshot/snapshot.csv")
        print(
            transaction_df := Helper.list_model_into_df(
                parser.load_mth_file("transaction/mth")
            )
        )
        transaction_df.to_csv("transaction/transaction.csv")

    _load_all_type(raw_parser := TwseTickParser(raw=True))
    _load_all_type_as_df(raw_parser)
    _load_all_type(parser := TwseTickParser(raw=False))
    _load_all_type_as_df(parser)

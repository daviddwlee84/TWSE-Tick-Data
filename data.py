from typing import List, Literal, Union, Tuple
from pydantic import BaseModel, Field, field_validator
import datetime
import pandas as pd
from tqdm.auto import tqdm
import struct


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
    def parse_time_hhmmss(value: str, simplify: bool = False) -> datetime.time:
        """
        Parse a string representing time in the format HHMMSS (6 digits)
        plus optional leftover digits as fractional seconds.

        If `simplify` is True, leftover digits are ignored.
        Otherwise, leftover digits (up to 6) become fractional seconds.

        Examples:
            - "083001" -> 08:30:01 (no leftover; 6 digits)
            - "08300110" -> 08:30:01.10 if simplify=False
            - "083001123456" -> 08:30:01.123456 if simplify=False
            - "0830" (fewer than 6 digits) -> 08:30:00 if simplify=True
        """
        value_str = value.strip()

        # Always parse at least the first 6 digits: HH=0..1, MM=2..3, SS=4..5
        main_part = value_str[:6]  # "HHMMSS"

        if len(main_part) < 6:
            # If not enough digits for HHMMSS and simplify=True, pad with zeros
            if simplify:
                main_part = main_part.ljust(6, "0")  # e.g. "0830" -> "083000"
            else:
                raise ValueError(
                    f"Time string '{value}' is too short to parse HHMMSS without simplify=True."
                )

        # Convert HH, MM, SS to integers
        hh = int(main_part[0:2])
        mm = int(main_part[2:4])
        ss = int(main_part[4:6])

        if simplify:
            # If simplifying, ignore anything beyond first 6 digits
            return datetime.time(hour=hh, minute=mm, second=ss)
        else:
            # Handle leftover digits as fractional seconds
            leftover = value_str[6:]  # everything beyond HHMMSS
            if not leftover:
                # No leftover means no fractional seconds
                return datetime.time(hour=hh, minute=mm, second=ss)

            # If there are leftover digits, treat them as fraction of a second
            # e.g. leftover = "12" => 0.12 seconds => 120,000 microseconds
            # e.g. leftover = "123456" => 0.123456 => 123,456 microseconds
            # Convert leftover to int and scale accordingly
            frac_value = int(leftover)
            frac_len = len(leftover)

            # datetime.time only supports microseconds (up to 6 digits).
            if frac_len > 6:
                # You could decide to truncate or raise an error. Here we raise an error:
                raise ValueError(
                    f"Fractional part '{leftover}' cannot exceed 6 digits."
                )

            # Scale integer to microseconds
            # e.g. leftover="123" -> frac_value=123 -> we need to multiply by 10^(6-3)=10^3=1000
            microsec = frac_value * (10 ** (6 - frac_len))

            return datetime.time(hour=hh, minute=mm, second=ss, microsecond=microsec)

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


class TwseRawSnapshotNew(BaseModel):
    """
    Pydantic model for TWSE 5-level snapshot (揭示檔), 190 bytes total.
    Each field is mapped according to the new schema (positions specified in the datasheet).
    """

    # 1-6 (length=6)
    securities_code: str = Field(..., description="證券代號 (X(06))")

    # 7-18 (length=12)
    display_time: str = Field(
        ..., description="揭示時間 (12 bytes, e.g. HHMMSS + '0000')"
    )

    # 19 (length=1)
    remark: str = Field(..., description="揭示註記 (空白, T, S)")

    # 20 (length=1)
    trend_flag: str = Field(..., description="趨勢註記 (空白, R, F, C)")

    # 21 (length=1)
    match_flag: str = Field(..., description="成交揭示 (空白, Y, S)")

    # 22 (length=1)
    trade_upper_lower_limit: str = Field(..., description="成交漲跌停註記 (空白, R, F)")

    # 23-28 (length=6)
    trade_price: str = Field(..., description="成交價格 (9(06))")

    # 29-36 (length=8)
    transaction_volume: str = Field(..., description="成交張數 (9(08))")

    # 37 (length=1)
    buy_tick_size: str = Field(..., description="檔位數(買進) (9(01))")

    # 38 (length=1)
    buy_upper_lower_limit: str = Field(..., description="漲跌停註記(買進) (空白, R, F)")

    # 39-108 (length=70)
    buy_5_price_volume: str = Field(
        ..., description="5檔買進價格及張數 (5 * (9(06) + 9(08)))"
    )

    # 109 (length=1)
    sell_tick_size: str = Field(..., description="檔位數(賣出) (9(01))")

    # 110 (length=1)
    sell_upper_lower_limit: str = Field(
        ..., description="漲跌停註記(賣出) (空白, R, F)"
    )

    # 111-180 (length=70)
    sell_5_price_volume: str = Field(
        ..., description="5檔賣出價格及張數 (5 * (9(06) + 9(08)))"
    )

    # 181-188 (length=8)
    display_date: str = Field(..., description="揭示日期 (YYYYMMDD, 9(08))")

    # 189-190 (length=2)
    match_staff: str = Field(..., description="撮合人員 (2 characters)")


class TwseSnapshotNew(BaseModel):
    """
    Pydantic model for TWSE 5-level snapshot (揭示檔), 190 bytes total.
    Field positions reflect the new schema (starting 2020/03/01).
    """

    # 1-6 (6 bytes)
    securities_code: str = Field(..., description="證券代號 (X(06))")

    # 7-18 (12 bytes)
    display_time: datetime.time = Field(
        ...,
        description="揭示時間 (12 bytes, e.g. HHMMSS + '0000' for pre-2020/03/23 data)",
    )

    # 19 (1 byte)
    remark: Literal[" ", "T", "S"] = Field(
        ...,
        description=(
            "揭示註記: "
            "' ' (空白)=一般揭示, "
            "'T'=開盤前或收盤前試算, "
            "'S'=穩定措施"
        ),
    )

    # 20 (1 byte)
    trend_flag: Literal[" ", "R", "F", "C"] = Field(
        ...,
        description=(
            "趨勢註記: "
            "' ' (空白)=未實施穩定措施, "
            "'R'=趨漲, 'F'=趨跌, 'C'=逐筆交易中間價(五檔全為0)"
        ),
    )

    # 21 (1 byte)
    match_flag: Literal[" ", "Y", "S"] = Field(
        ...,
        description=("成交揭示: " "' ' (空白)=未成交, " "'Y'=成交, " "'S'=穩定措施"),
    )

    # 22 (1 byte)
    trade_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="成交漲跌停註記: ' ' (空白)=Normal, 'R'=漲停, 'F'=跌停"
    )

    # 23-28 (6 bytes)
    trade_price: float = Field(..., description="成交價格 (9(06))")

    # 29-36 (8 bytes)
    transaction_volume: int = Field(..., description="成交張數 (9(08))")

    # 37 (1 byte)
    buy_tick_size: int = Field(..., description="檔位數(買進) (9(01))")

    # 38 (1 byte)
    buy_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="漲跌停註記(買進): ' ' (空白)=Normal, 'R'=漲停, 'F'=跌停"
    )

    # 39-108 (70 bytes total, 5 * (6+8))
    buy_5_price_volume: List[dict] = Field(
        ..., description="List of 5 dict: [{price, volume}, ...] (5檔買進價格及張數)"
    )

    # 109 (1 byte)
    sell_tick_size: int = Field(..., description="檔位數(賣出) (9(01))")

    # 110 (1 byte)
    sell_upper_lower_limit: Literal[" ", "R", "F"] = Field(
        ..., description="漲跌停註記(賣出): ' ' (空白)=Normal, 'R'=漲停, 'F'=跌停"
    )

    # 111-180 (70 bytes total, 5 * (6+8))
    sell_5_price_volume: List[dict] = Field(
        ..., description="List of 5 dict: [{price, volume}, ...] (5檔賣出價格及張數)"
    )

    # 181-188 (8 bytes)
    display_date: datetime.date = Field(..., description="揭示日期 (YYYYMMDD, 9(08))")

    # 189-190 (2 bytes)
    match_staff: str = Field(..., description="撮合人員 (X(02))")

    # ---------------------------
    # Field validators / parsers
    # ---------------------------

    @field_validator("securities_code", mode="before")
    @classmethod
    def strip_securities_code(cls, v: str) -> str:
        return v.strip()

    @field_validator("display_time", mode="before")
    @classmethod
    def parse_display_time(cls, v: str) -> datetime.time:
        """
        For the new 12-byte Display Time, we typically have HHMMSS + '0000'
        prior to the start of continuous trading (2020/03/23).
        We'll parse only the first 6 digits as HHMMSS.
        """
        # e.g. "0930000000" => "09:30:00"
        return Helper.parse_time_hhmmss(v[:6])

    @field_validator("trade_price", mode="before")
    @classmethod
    def parse_trade_price(cls, v: str) -> float:
        # For 6-digit numeric (e.g. 001234 => 12.34)
        return Helper.parse_6digit_price(v)

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

    @staticmethod
    def parse_line_to_new_snapshot(
        line: str, raw: bool = False
    ) -> Union[TwseSnapshotNew, TwseRawSnapshotNew]:
        """
        Given a 190-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        cls = TwseRawSnapshotNew if raw else TwseSnapshotNew  # Use updated schema

        return cls(
            securities_code=line[0:6],  # 1-6
            display_time=line[6:18],  # 7-18 (12 bytes)
            remark=line[18:19],  # 19
            trend_flag=line[19:20],  # 20
            match_flag=line[20:21],  # 21
            trade_upper_lower_limit=line[21:22],  # 22
            trade_price=line[22:28],  # 23-28
            transaction_volume=line[28:36],  # 29-36
            buy_tick_size=line[36:37],  # 37
            buy_upper_lower_limit=line[37:38],  # 38
            buy_5_price_volume=line[38:108],  # 39-108 (70 bytes)
            sell_tick_size=line[108:109],  # 109
            sell_upper_lower_limit=line[109:110],  # 110
            sell_5_price_volume=line[110:180],  # 111-180 (70 bytes)
            display_date=line[180:188],  # 181-188 (8 bytes)
            match_staff=line[188:190],  # 189-190 (2 bytes)
        )

    @staticmethod
    def parse_bytes_to_snapshot(
        record_bytes: bytes, raw: bool = False
    ) -> Union[TwseSnapshotNew, TwseRawSnapshotNew, TwseSnapshot, TwseRawSnapshot]:
        """
        Given either a 190-byte or 186-byte record in `record_bytes`,
        parse each field via struct.unpack according to the new or old schema
        and return the appropriate Pydantic model instance.

        NOTE: this example use Python `struct` module
        https://docs.python.org/3/library/struct.html
        https://phoenixnap.com/kb/python-struct

        :param record_bytes: The complete, fixed-width record (190 or 186 bytes).
        :param raw: If True, parse into the 'raw' model variant (string fields only).
                    If False, parse into the typed model variant (e.g. floats, ints, etc.).
        :returns: One of the four Pydantic models (new/old × raw/typed).
        """

        # -----------------------------
        # 1) Try the new (190-byte) format
        # -----------------------------
        try:
            cls = TwseRawSnapshotNew if raw else TwseSnapshotNew  # Use updated schema

            # Each position-length pair as "N s" (N characters)
            # Byte offsets (1-indexed in your doc):
            #   1-6   => 6s
            #   7-18  => 12s
            #   19    => 1s
            #   20    => 1s
            #   21    => 1s
            #   22    => 1s
            #   23-28 => 6s
            #   29-36 => 8s
            #   37    => 1s
            #   38    => 1s
            #   39-108   => 70s
            #   109      => 1s
            #   110      => 1s
            #   111-180  => 70s
            #   181-188  => 8s
            #   189-190  => 2s
            #
            # Total = 190
            TWSE_NEW_SNAPSHOT_STRUCT_FORMAT = (
                "6s"  # securities_code
                "12s"  # display_time
                "1s"  # remark
                "1s"  # trend_flag
                "1s"  # match_flag
                "1s"  # trade_upper_lower_limit
                "6s"  # trade_price
                "8s"  # transaction_volume
                "1s"  # buy_tick_size
                "1s"  # buy_upper_lower_limit
                "70s"  # buy_5_price_volume
                "1s"  # sell_tick_size
                "1s"  # sell_upper_lower_limit
                "70s"  # sell_5_price_volume
                "8s"  # display_date
                "2s"  # match_staff
            )
            result: Tuple[bytes] = struct.unpack(
                TWSE_NEW_SNAPSHOT_STRUCT_FORMAT, record_bytes
            )

        # -----------------------------
        # 2) Fall back to the old (186-byte) format if struct raises an error
        # -----------------------------
        except struct.error:
            cls = TwseRawSnapshot if raw else TwseSnapshot  # Use old schema

            # Total = 186
            TWSE_SNAPSHOT_STRUCT_FORMAT = (
                "6s"  # securities_code
                "8s"  # display_time
                "1s"  # remark
                "1s"  # trend_flag
                "1s"  # match_flag
                "1s"  # trade_upper_lower_limit
                "6s"  # trade_price
                "8s"  # transaction_volume
                "1s"  # buy_tick_size
                "1s"  # buy_upper_lower_limit
                "70s"  # buy_5_price_volume
                "1s"  # sell_tick_size
                "1s"  # sell_upper_lower_limit
                "70s"  # sell_5_price_volume
                "8s"  # display_date
                "2s"  # match_staff
            )
            result: Tuple[bytes] = struct.unpack(
                TWSE_SNAPSHOT_STRUCT_FORMAT, record_bytes
            )

        # TODO: maybe not all the field need to be decoded into string first (e.g. int or float can directly parse from bytes)
        result = [x.decode("ascii") for x in result]

        return cls(
            securities_code=result[0],
            display_time=result[1],
            remark=result[2],
            trend_flag=result[3],
            match_flag=result[4],
            trade_upper_lower_limit=result[5],
            trade_price=result[6],
            transaction_volume=result[7],
            buy_tick_size=result[8],
            buy_upper_lower_limit=result[9],
            buy_5_price_volume=result[10],
            sell_tick_size=result[11],
            sell_upper_lower_limit=result[12],
            sell_5_price_volume=result[13],
            display_date=result[14],
            match_staff=result[15],
        )

    def load_dsp_file(
        self, filepath: str, flatten: bool = False, decode_first: bool = True
    ) -> List[
        Union[TwseSnapshot, TwseRawSnapshot, TwseSnapshotNew, TwseRawSnapshotNew, dict]
    ]:
        """
        Open the `dspyyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseRawSnapshot model instances.

        NOTE: decode first then slicing is faster than parse bytes using struct.unpack then decode
        Decode-then-slice took: 0.0369 seconds for 200000 records.
        struct.unpack+decode took: 0.1657 seconds for 200000 records.
        Ratio = 4.49x
        """
        records = []
        with open(filepath, "rb") as f:
            for raw_line in tqdm(f):
                # Strip trailing newline or carriage-return
                line = raw_line.rstrip(b"\r\n")
                # Ensure the line is 186 bytes
                if len(line) not in {186, 190}:
                    # In production, you might raise an exception or skip
                    continue

                if decode_first:
                    # Decode from bytes to string
                    decoded_line = line.decode("utf-8", errors="replace")

                    # Parse into our Pydantic model
                    if len(line) == 186:
                        record = self.parse_line_to_snapshot(decoded_line, raw=self.raw)
                    else:
                        record = self.parse_line_to_new_snapshot(
                            decoded_line, raw=self.raw
                        )
                else:
                    record = self.parse_bytes_to_snapshot(line)

                if flatten and not self.raw:
                    records.append(self.flatten_snapshot(record))
                else:
                    records.append(record)
        return records

    @staticmethod
    def flatten_snapshot(snapshot: TwseSnapshot) -> dict:
        """
        Convert a TwseSnapshot into a flattened dictionary
        where buy_5_price_volume and sell_5_price_volume
        become separate columns (bid_price_1, bid_volume_1, etc.).
        """
        d = snapshot.model_dump()  # Pydantic v2 -> returns dict
        # or .dict() in Pydantic v1

        # Extract buy/sell arrays
        buy_pairs = d.pop("buy_5_price_volume", [])
        sell_pairs = d.pop("sell_5_price_volume", [])

        # Flatten buy pairs => bid_price_1, bid_volume_1, etc.
        for i, pair in enumerate(buy_pairs, start=1):
            d[f"bid_price_{i}"] = pair["price"]
            d[f"bid_volume_{i}"] = pair["volume"]

        # Flatten sell pairs => ask_price_1, ask_volume_1, etc.
        for i, pair in enumerate(sell_pairs, start=1):
            d[f"ask_price_{i}"] = pair["price"]
            d[f"ask_volume_{i}"] = pair["volume"]

        return d

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
        for i, record in enumerate(
            parser.load_dsp_file("snapshot/Sample_new"), start=1
        ):
            print(f"New Snapshot Record #{i}:")
            print(record.model_dump_json(indent=2))
            break

    def _load_all_type_as_df(parser: TwseTickParser):
        print(order_df := Helper.list_model_into_df(parser.load_odr_file("order/odr")))
        order_df.to_csv("order/order.csv")
        print(
            market_df := Helper.list_model_into_df(
                parser.load_dsp_file("snapshot/Sample", flatten=False)
            )
        )
        market_df.to_csv("snapshot/snapshot.csv")
        if not parser.raw:
            print(
                flatten_market_df := pd.DataFrame(
                    parser.load_dsp_file("snapshot/Sample", flatten=True)
                )
            )
            flatten_market_df.to_csv("snapshot/snapshot_flatten.csv")
        print(
            transaction_df := Helper.list_model_into_df(
                parser.load_mth_file("transaction/mth")
            )
        )
        transaction_df.to_csv("transaction/transaction.csv")

    def _load_new_snapshot(parser: TwseTickParser):
        print(
            market_df := Helper.list_model_into_df(
                parser.load_dsp_file("snapshot/Sample_new", flatten=False)
            )
        )
        market_df.to_csv("snapshot/snapshot_new.csv")
        print(
            flatten_market_df := pd.DataFrame(
                parser.load_dsp_file("snapshot/Sample_new", flatten=True)
            )
        )
        flatten_market_df.to_csv("snapshot/snapshot_flatten_new.csv")

    def _load_new_snapshot_to_parquet(
        filepath: str = "snapshot/Sample_new",
    ) -> pd.DataFrame:
        parser = TwseTickParser(raw=False)
        df = pd.DataFrame(parser.load_dsp_file(filepath, flatten=True))
        df.to_parquet(filepath + ".parquet")
        import ipdb

        ipdb.set_trace()
        return df

    _load_all_type(raw_parser := TwseTickParser(raw=True))
    _load_all_type_as_df(raw_parser)
    _load_all_type(parser := TwseTickParser(raw=False))
    _load_all_type_as_df(parser)
    _load_new_snapshot(parser)

    _load_new_snapshot_to_parquet()

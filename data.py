from typing import List
from pydantic import BaseModel, Field


class TwseOrderBook(BaseModel):
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


class TwseSnapshot(BaseModel):
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


class TwseTransaction(BaseModel):
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


class TwseTickParser:

    _ODR_LEN: int = 59
    _DSP_LEN: int = 186
    _MTH_LEN: int = 63

    def __init__(self):
        pass

    @staticmethod
    def parse_line_to_order(line: str) -> TwseOrderBook:
        """
        Given a 59-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        return TwseOrderBook(
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

    def load_odr_file(self, filepath: str) -> List[TwseOrderBook]:
        """
        Open the `odryyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseOrderBook model instances.
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
                record = self.parse_line_to_order(decoded_line)
                records.append(record)
        return records

    @staticmethod
    def parse_line_to_snapshot(line: str) -> TwseSnapshot:
        """
        Given a 186-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        return TwseSnapshot(
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

    def load_dsp_file(self, filepath: str) -> List[TwseSnapshot]:
        """
        Open the `dspyyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseSnapshot model instances.
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
                record = self.parse_line_to_snapshot(decoded_line)
                records.append(record)
        return records

    @staticmethod
    def parse_line_to_transaction(line: str) -> TwseTransaction:
        """
        Given a 63-character line (already stripped of newlines),
        parse each field based on fixed positions and return a Pydantic model.
        """
        return TwseTransaction(
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

    def load_mth_file(self, filepath: str) -> List[TwseTransaction]:
        """
        Open the `mthyyyymmdd` file in binary mode, split by lines, and parse each line.
        Returns a list of TwseTransaction model instances.
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
                record = self.parse_line_to_transaction(decoded_line)
                records.append(record)
        return records


if __name__ == "__main__":
    parser = TwseTickParser()
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

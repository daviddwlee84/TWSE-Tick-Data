

 **臺灣證券交易所網路資訊商店**

**資訊檔案格式**

**TWSE Data E-Shop File Formats**

**檔案名稱：** 成交檔

**Name：** Securities intra-day data with order type

**傳輸檔案名稱：**mthyyyymmdd

**Download file：**mthyyyymmdd

**檔案長度/Record length：**63 BYTES

| 項目名稱  Fields | 屬性  Type | 位置-長度  Position-Length | 說明  Description |
| --- | --- | --- | --- |
| 成交日期  Date | 9(08) | 1- 8 |  |
| 證券代號  Securities Code | X(06) | 9- 6 |  |
| 買賣別  BUY-SELL | X(01) | 15- 1 | B：買/BUY  S：賣/SELL |
| 交易種類代號  Trade Type Code | X(01) | 16- 1 | 0：普通 /Regular Trading  1：鉅額/Block Trading  2：零股/Odd-lot Trading |
| 成交時間  Trade Time | X(08) | 17- 8 |  |
| 成交序號  Trade Number | 9(08) | 25- 8 |  |
| 成交及委託檔連結代碼二  Order NumberⅡ | X(05) | 33- 5 | 該證券商委託書編號。因委託與成交為一對多關係，如有需要知道該筆委託的相對應成交紀錄，可用此欄位連接。(但因提供的成交資料已經過錯帳調整與綜合帳戶重分配作業，並非每一筆成交資料都有相對應的委託)。  The serial number of the brokerage order ticket. Since the commission and the transaction are one-to-many, if you need to know the corresponding transaction record of the commission, you can use this field to connect. However, out-trade had carried out and the omnibus accounts was re-allocated, so not every transaction has a corresponding entrustment. |
| 成交價格  Trade Price | X(07) | 38- 7 |  |
| 成交股數  Trade Volume(share) | 9(09) | 45- 9 |  |
| 成交回報印表機  Trading Report Print | 9(04) | 54- 4 |  |
| 委託種類代號  Order Type Code | 9(01) | 58- 1 | 0：現券/ Normal  1：證金公司辦理融資/ Purchase on Margin Via Securities Finance  2：證金公司辦理融券/ Short Sell Via Securities Finance  3：證券公司辦理融資  / Purchase on Margin Via Securities Firms  4：證券公司辦理融券  Short Sell Via Securities Firms  5：借券賣出（一般）/ Security Lending  (No Hedge)  6：借券賣出（避險）/ Security Lending  (Hedge) |
| 投資人屬性  Type of Investor | X(01) | 59- 1 | M：投信基金/ Securities Investment Trust  F：外資/ Foreign Investors  I：自然人 / Natural-person Investors  J：其他一般法人/Others |
| 成交及委託檔連結代碼一  Order NumberⅠ | X(04) | 60- 4 | 證券商代號(重新編碼後之證券商代號)。  Securities Code  (Recoded) |

檔案格式制修一覽表

| **中文名稱** | 成交檔 | |
| --- | --- | --- |
| 編號 | 修正日期 | 制修內容摘要 |
| 1 | 2018/11/27 | 重新檢視格式 |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |



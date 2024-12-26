

 **臺灣證券交易所網路資訊商店**

**資訊檔案格式**

**TWSE Data E-Shop File Formats**

**檔案名稱：** 揭示檔

**Name：** Securities intra-day data with 5-level order book

**傳輸檔案名稱：**dspyyyymmdd

**Download file：**dspyyyymmdd

**檔案長度/Record length：**190BYTES

| 項目名稱  Fields | 屬性  Type | 位置-長度  Position-Length | 說明  Description |
| --- | --- | --- | --- |
| 證券代號  Securities Code | X(06) | 1- 6 |  |
| 揭示時間  Display Time | X(12) | 7- 12 |  |
| 揭示註記  Remark | X(01) | 19- 1 | 空白/Space:一般揭示/ Normal  T: 開盤前或收盤前試算/ Trial Calculation  S: 穩定措施/ Stabilizing Measures |
| 趨勢註記  Trend- Flag | X(01) | 20- 1 | 空白/Space: 未實施穩定措施/ No Execute Stabilizing Measures  R：趨漲/RAISING-TREND  F：趨跌/FALLING-TREND  C: 逐筆交易中間價註記(當一個委託產生不同價位成交時，中間委託價量不予表示(揭示五檔數值均以”0”表示)/ Remarks on the intermediate price of continuous trading (when an order produces a deal at different prices, the intermediate order price and volume will not displayed (the “5 Occurs Buy Price and Volume “ field all shows "0").。 |
| 成交揭示  Match-Flag | X(01) | 21- 1 | 空白/Space:未成交/No match  Y:成交/Has match  S: 穩定措施/ Stabilizing Measures |
| 成交漲跌停註記  Trade Upper(Lower) Limit | X(01) | 22- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 成交價格  Trade Price | 9(06) | 23- 6 |  |
| 成交張數  Transaction Volume | 9(08) | 29- 8 |  |
| 檔位數(買進)  Tick Size | 9(01) | 37- 1 |  |
| 漲跌停註記(買進)  Upper(Lower) Limit | X(01) | 38- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 5檔買進價格及張數  5 Occurs Buy Price and Volume | 9(06)  9(08) | 39- 70 |  |
| 檔位數(賣出)  Tick Size | 9(01) | 109- 1 |  |
| 漲跌停註記(賣出)  Upper(Lower) Limit | X(01) | 110- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 5檔賣出價格及張數  5 Occurs Sell Price and Volume | 9(06)  9(08) | 111- 70 |  |
| 揭示日期  Display Date | 9(08) | 181- 8 |  |
| 撮合人員 | X(02) | 189- 2 |  |

檔案格式制修一覽表

| **中文名稱** | 揭示檔 | |
| --- | --- | --- |
| 編號 | 修正日期 | 制修內容摘要 |
| 1 | 2018/11/27 | 重新檢視格式 |
| 2 | 2020/03/01 | 1. 2020/03/01起，揭示檔總長度由186改為190。 2. 2020/03/01起，揭示時間(Display Time)欄位長度從8碼改為12碼，自逐筆交易開始日 2020/03/23起顯示實際揭示時間，逐筆交易開始日前的揭示時間後4碼(9-12)以0000填補。 |
|  |  |  |



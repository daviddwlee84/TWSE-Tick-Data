

 **臺灣證券交易所網路資訊商店**

**資訊檔案格式**

**TWSE Data E-Shop File Formats**

**檔案名稱：** 揭示檔

**Name：** Securities intra-day data with 5-level order book

**傳輸檔案名稱：**dspyyyymmdd

**Download file：**dspyyyymmdd

**檔案長度/Record length：**186BYTES

| 項目名稱  Fields | 屬性  Type | 位置-長度  Position-Length | 說明  Description |
| --- | --- | --- | --- |
| 證券代號  Securities Code | X(06) | 1- 6 |  |
| 揭示時間  Display Time | X(08) | 7- 8 |  |
| 揭示註記  Remark | X(01) | 15- 1 | 空白/Space:一般揭示/ Normal  T: 開盤前或收盤前試算/ Trial Calculation  S: 穩定措施/ Stabilizing Measures  A:人工撮合/Man -Match |
| 趨勢註記  Trend- Flag | X(01) | 16- 1 | 空白/Space: 未實施穩定措施/ No Execute Stabilizing Measures  R：趨漲/RAISING-TREND  F：趨跌/FALLING-TREND |
| 成交揭示  Match-Flag | X(01) | 17- 1 | 空白/Space:未成交/No match  Y:成交/Has match  S: 穩定措施/ Stabilizing Measures |
| 成交漲跌停註記  Trade Upper(Lower) Limit | X(01) | 18- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 成交價格  Trade Price | 9(06) | 19- 6 |  |
| 成交張數  Transaction Volume | 9(08) | 25- 8 |  |
| 檔位數(買進)  Tick Size | 9(01) | 33- 1 |  |
| 漲跌停註記(買進)  Upper(Lower) Limit | X(01) | 34- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 5檔買進價格及張數  5 Occurs Buy Price and Volume | 9(06)  9(08) | 35- 70 |  |
| 檔位數(賣出)  Tick Size | 9(01) | 105- 1 |  |
| 漲跌停註記(賣出)  Upper(Lower) Limit | X(01) | 106- 1 | 空白/Space: Normal  R:漲停/ Limit-up  F: 跌停/Limit-down |
| 5檔賣出價格及張數  5 Occurs Sell Price and Volume | 9(06)  9(08) | 107- 70 |  |
| 揭示日期  Display Date | 9(08) | 177- 8 |  |
| 撮合人員 | X(02) | 185- 2 |  |

檔案格式制修一覽表

| **中文名稱** | 揭示檔 | |
| --- | --- | --- |
| 編號 | 修正日期 | 制修內容摘要 |
| 1 | 2018/11/27 | 重新檢視格式 |
| 2 | 2020/05/19 | 新增欄位 |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |



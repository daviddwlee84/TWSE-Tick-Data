(NOTE: this is actually the newer schema)

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

---

Real world data

```txt
total 201G
-rw-r--r-- 1 daviddwlee84 daviddwlee84 8.1G Jan  8 16:46 aldsp_202411.7z
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.7G Dec  1 02:35 dsp20241101
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.2G Dec  1 02:37 dsp20241104
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.6G Dec  1 02:39 dsp20241105
-rw-rw-r-- 1 daviddwlee84 daviddwlee84  11G Dec  1 02:41 dsp20241106
-rw-rw-r-- 1 daviddwlee84 daviddwlee84  10G Dec  1 02:43 dsp20241107
-rw-rw-r-- 1 daviddwlee84 daviddwlee84  11G Dec  1 02:45 dsp20241108
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.2G Dec  1 02:47 dsp20241111
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.5G Dec  1 02:49 dsp20241112
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.9G Dec  1 02:51 dsp20241113
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.8G Dec  1 02:53 dsp20241114
-rw-rw-r-- 1 daviddwlee84 daviddwlee84  10G Dec  1 02:55 dsp20241115
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.1G Dec  1 02:57 dsp20241118
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.8G Dec  1 02:59 dsp20241119
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.7G Dec  1 03:01 dsp20241120
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.6G Dec  1 03:02 dsp20241121
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.3G Dec  1 03:04 dsp20241122
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.9G Dec  1 03:06 dsp20241125
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.8G Dec  1 03:08 dsp20241126
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.2G Dec  1 03:10 dsp20241127
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 9.6G Dec  1 03:12 dsp20241128
-rw-rw-r-- 1 daviddwlee84 daviddwlee84 8.6G Dec  1 03:14 dsp20241129
```

Single day (`dsp20241111`)

```txt
51712524it [2:07:04, 6782.49it/s]
         securities_code display_time remark trend_flag match_flag  ... sell_tick_size  sell_upper_lower_limit                                sell_5_price_volume  display_date match_staff
0                   0050     08:30:04      T                        ...              1                          [{'price': 203.0, 'volume': 2}, {'price': 0.0,...    2024-11-11          AA
1                   0050     08:30:09      T                        ...              5                          [{'price': 200.0, 'volume': 1}, {'price': 200....    2024-11-11          AA
2                   0050     08:30:14      T                        ...              5                          [{'price': 200.0, 'volume': 2}, {'price': 200....    2024-11-11          AA
3                   0050     08:30:19      T                        ...              5                          [{'price': 200.0, 'volume': 2}, {'price': 200....    2024-11-11          AA
4                   0050     08:30:24      T                        ...              5                          [{'price': 200.0, 'volume': 13}, {'price': 200...    2024-11-11          AA
...                  ...          ...    ...        ...        ...  ...            ...                     ...                                                ...           ...         ...
51712519            9958     13:29:37      T                        ...              5                          [{'price': 185.5, 'volume': 8}, {'price': 186....    2024-11-11          AA
51712520            9958     13:29:47      T                        ...              5                          [{'price': 185.5, 'volume': 8}, {'price': 186....    2024-11-11          AA
51712521            9958     13:29:52      T                        ...              5                          [{'price': 185.5, 'volume': 8}, {'price': 186....    2024-11-11          AA
51712522            9958     13:29:57      T                        ...              5                          [{'price': 185.5, 'volume': 8}, {'price': 186....    2024-11-11          AA
51712523            9958     13:30:00                            Y  ...              5                          [{'price': 185.5, 'volume': 8}, {'price': 186....    2024-11-11          AA

[51712524 rows x 16 columns]
```

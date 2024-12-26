

 **臺灣證券交易所網路資訊商店**

**資訊檔案格式**

**TWSE Data E-Shop File Formats**

**檔案名稱：** 委託檔

**Name：** Order book log

**傳輸檔案名稱：**odryyyymmdd

**Download file：**odryyyymmdd

**檔案長度/Record length：63** BYTES

| 項目名稱  Fields | 屬性  Type | 位置-長度  Position-Length | 說明  Description |
| --- | --- | --- | --- |
| 委託日期  Order Date | 9(08) | 1- 8 |  |
| 證券代號  Securities Code | X(06) | 9- 6 |  |
| 買賣別  BUY-SELL | X(01) | 15- 1 | B：買/BUY  S：賣/SELL |
| 交易種類代號  Trade Type Code | X(01) | 16- 1 | 0：普通 /Regular Trading  1：鉅額/Block Trading  2：零股/Odd-lot Trading |
| 委託時間  Order Time | X(12) | 17- 12 |  |
| 成交及委託檔連結代碼二  Order NumberⅡ | X(05) | 29- 5 | 該證券商委託書編號。因委託與成交為一對多關係，如有需要知道該筆委託的相對應成交紀錄，可用此欄位連接。(但因提供的成交資料已經過錯帳調整與綜合帳戶重分配作業，並非每一筆成交資料都有相對應的委託)。  The serial number of the brokerage order ticket. Since the commission and the transaction are one-to-many, if you need to know the corresponding transaction record of the commission, you can use this field to connect. However, out-trade had carried out and the omnibus accounts was re-allocated, so not every transaction has a corresponding entrustment. |
| 更改後交易代號  Changed Trade Code | X(1) | 34- 1 | 1、2、3：買進/Buy、買進後改量/Adjust、買進後註銷/ Cancel；  4、5、6：賣出/Sell、賣出後改量/ Adjust、賣出後註銷/Cancel  7:改價-買  8:改價-賣 |
| 委託價格  Order Price | X(07) | 35- 7 | 委託價格為0者,  市價單 |
| 更改後股數  Changed the Trade Volume(share) | 9(11) | 42- 11 | S9(10)：S為正負號/±，與十位數字/and 10 numbers  如為負號，代表刪單或改量/ If S=”-“,means cancel or adjust. |
| 委託種類代號  Order Type Code | 9(01) | 53- 1 | 0：現券/ Normal  1：證金公司辦理融資/ Purchase on Margin Via Securities Finance  2：證金公司辦理融券/ Short Sell Via Securities Finance  3：證券公司辦理融資  / Purchase on Margin Via Securities Firms  4：證券公司辦理融券  Short Sell Via Securities Firms  5：借券賣出（一般）/ Security Lending  (No Hedge)  6：借券賣出（避險）/ Security Lending  (Hedge) |
| 下單通路註記  Notes of Investors’ Order  Channel | X(01) | 54- 1 | 【TMP protocol】  空白/Space：一般委託/Normal  A：自動化委託下單/ Automatic  D：電子式專屬線路/DMA  I：網際網路委託下單/Internet  V：語音下單/Voice  P：應用程式/API  【FIX protocol】  1：一般委託/Normal  2：自動化委託下單/ Automatic  3：電子式專屬線路/DMA  4：網際網路委託下單/Internet  5：語音下單/Voice  6：應用程式/API |
| 價格種類  Type of Price | X(01) | 55- 1 | 1:市價; 2限價 |
| 時間限制  Time Restriction | X(01) | 56- 1 | 0: ROD ; 3: IOC; 4: FOK |
| 委託資料種類  Type of Order Data | X(01) | 57- 1 | A:系統產生; B:證券商輸入 |
| 空白  Filler | X(01) | 58- 1 |  |
| 投資人屬性  Type of Investor | X(01) | 59- 1 | M：投信基金/ Securities Investment Trust  F：外資/ Foreign Investors  I：自然人 / Natural-person Investors  J：其他一般法人/Others |
| 成交及委託檔連結代碼一  Order NumberⅠ | X(04) | 60- 4 | 證券商代號(重新編碼後之證券商代號)。  Securities Code  (Recoded) |

檔案格式制修一覽表

| **中文名稱** | 委託檔 | |
| --- | --- | --- |
| 編號 | 修正日期 | 制修內容摘要 |
| 1 | 2018/11/27 | 重新檢視格式 |

| 2 | 2020/03/01 | 1. 2020/03/01起，委託檔總長度由59改為63。 2. 2020/03/01起，委託時間(Order Time)欄位長度從8碼改為12碼，自逐筆交易開始日2020/03/23起顯示實際委託時間，逐筆交易開始日以前的委託時間後4碼(9-12)以0000填補。 |
| --- | --- | --- |

| 3 | 2020/03/23 | 1. 2020/03/23起委託檔取消「委託回報印表機」欄位(長度4碼)，該欄位改為以下4個欄位，   i.價格種類        X(01)   1:市價; 2限價  ii. 時間限制      X(01)   0: ROD ; 3: IOC; 4: FOK  iii. 委託資料種類    X(01)   A:系統產生; B:證券商輸入  iv. 空白            X(01)   1. 2020/03/23前的「委託回報印表機」欄位資料為空白。 |
| --- | --- | --- |



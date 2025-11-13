# FinLab

> Goal: get previous close price / reference price

- [Historical Stats | Fugle Developer Docs](https://developer.fugle.tw/docs/data/http-api/historical/stats)
  - Has `closePrice`

```mermaid
flowchart LR

  subgraph T["交易日 T（今天）"]
    A["連續競價＋收盤集合競價<br>→ 成交價 = 收盤價<br>收盤價 (Closing Price / raw close)<br>時間：約 13:30"]
    A --> B["昨收 / pre-close（給 T+1 用）<br>前一日收盤價 (Previous Close)"]
  end

  subgraph CA["公司行為已事先公告"]
    C["股利、股票股利、現金增資、減資…<br>Corporate Actions"]
  end

  %% 收盤後由收盤價 + 公司行為計算參考價
  A -->|"T 收盤後 下午/晚上"| D["計算參考價<br>除權息參考價 (Ex-rights / Ex-dividend Ref Price)<br>減資恢復買賣參考價 (Capital Reduction Ref Price)"]
  C --> D

  subgraph Tp1["下一交易日 T+1（隔日）"]
    %% 有公司行為 → 由參考價算開盤平盤價
    D --> E["依檔位四捨五入<br>開盤參考價 / 平盤價<br>(Opening Reference Price / Base Price)<br>= 今日參考價 (Today’s Ref Price)"]

    %% 無公司行為 → 直接沿用昨收
    B -->|"若 T+1 無公司行為"| E

    %% 盤前券商 / 資料商可以提供
    E --> F["盤前行情欄位<br>referencePrice / openingRefPrice<br>可用時間：T+1 開盤前 (~09:00 前)"]

    %% 開盤集合競價產生真正的開盤價
    F --> G["開盤集合競價<br>開盤價 (Opening Price)"]
  end

  %% 另外一條支線：用收盤價與公司行為更新歷史還原股價
  A --> H["依公司行為計算調整因子<br>→ 更新歷史價格序列<br>後復權收盤價 (Adjusted Close, back-adjusted)"]
  C --> H
```

- 收盤價：Closing Price / raw close
- 昨收 / 前一日收盤價：Previous Close / Pre-close
- 除權息參考價：Ex-rights / Ex-dividend Reference Price
- 減資恢復買賣參考價：Capital Reduction Reference Price
- 開盤參考價 / 平盤價：Opening Reference Price / Base Price / Today’s reference price
- 開盤價：Opening Price
- 後復權收盤價：Adjusted Close (back-adjusted)

---

[Reference Price for Capital Reduction - Taiwan Stock Exchange Corporation](https://www.twse.com.tw/en/announcement/reduction/twtauu.html)

**Formulas:**

1. The following figures are referred from “Capital Reduction Announcement”  
    (A): Closing Price on The Last Trading Date  
    (B): Cash Amount of Refund Per share  
    (C): Ratio of Post- Reduction Outstanding Shares to Original Outstanding Shares  
    (D): Subscription Price per New Share  
    (E): Subscription Ratio of Post-Reduction to New Shares Issued  
    (F)：Cash dividend
2. Capital reduction by cash refund of capital stock  
    Post-Reduction Reference Price＝ [ (A)-(F)-(B) ] / (C)
3. Capital reduction for purposes of making up losses  
    Post-Reduction Reference Price＝ (A) / (C)
4. Capital Reduction and Cash Injection  
    Post-Reduction Reference Price＝ (A) / (C)  
    Ex-right Reference Price = [Post-Reduction Reference Price +(D)*(E) ] / [ 1+(E) ]

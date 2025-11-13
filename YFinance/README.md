# Yahoo Finance

Goal: use Dividends, Splits, Adj. Close to inference AdjFactor, RefPrice, PreClose

```
AdjFactor   = Adj Close / Close
RefPrice    = Close.shift(1) * (AdjFactor.shift(1) / AdjFactor)
PreCloseRaw = Close.shift(1)
PreCloseCorr= RefPrice
```

```py
import yfinance as yf

# 1. 下載資料：必須關閉自動調整 (auto_adjust=False) 才能拿到原始 Close
# Actions=True 會包含 Dividends 和 Splits 欄位
ticker = "2330.TW"
df = yf.download(ticker, start="2023-06-01", end="2023-07-01", auto_adjust=False, actions=True)
```

- [vectorbt.data.custom.YFData](https://vectorbt.dev/api/data/custom/#vectorbt.data.custom.YFData)
- [ranaroussi/yfinance: Download market data from Yahoo! Finance's API](https://github.com/ranaroussi/yfinance)
  - [yfinance documentation — yfinance](https://ranaroussi.github.io/yfinance/index.html)
    - [yfinance.download — yfinance](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html#yfinance.download)
- [Rate Limits](https://help.yahooinc.com/dsp-api/docs/rate-limits)
  - [Yfinance saying “Too many requests.Rate limited” : r/learnpython](https://www.reddit.com/r/learnpython/comments/1isuc4h/yfinance_saying_too_many_requestsrate_limited/)

---

https://gemini.google.com/share/77c1751c83e2
https://chatgpt.com/share/6915c93a-d594-8012-a375-84047aa2915d

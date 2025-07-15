# TWSE Tick Data

- [歷史交易資料 - 證交所網路資訊商店](https://eshop.twse.com.tw/zh/category/main/7)

| Alias                      | 名稱                                                                                           | Name                                              | 價格                      | 限制                                                   |
| -------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------- | ------------------------- | ------------------------------------------------------ |
| [Snapshot](snapshot)       | [揭示檔](https://eshop.twse.com.tw/zh/product/detail/0000000063afcda50163b1a5bc180006)         | Securities intra-day data with 5-level order book | 單月全部: NT$ 5,000 / 月  | 1.提供自81年1月4日起之資料至上個月                     |
| [Transaction](transaction) | [成交檔](https://eshop.twse.com.tw/zh/product/detail/0000000063ce6ab00163d860b694000a)         | Securities intra-day data with order type         | 單月全部: NT$ 10,000 / 月 | 1.提供自95年1月1日起資料。2.不提供最近一年之資料檔案。 |
| [Order](order)             | [委託檔](https://eshop.twse.com.tw/zh/product/detail/00000000639057100163905e1d7c0001)         | Order book log                                    | 單月全部: NT$ 10,000 / 月 | 1.提供自95年1月1日起資料。2.不提供最近一年之資料檔案。 |
| -                          | [盤中零股揭示檔](https://eshop.twse.com.tw/zh/product/detail/0000000080da7fa70182334eb932009d) | -                                                 | 單月全部: NT$ 1,500 / 月  | 1.提供自109年10月26日起之資料至上個月。                |

## Parser Example

### Python

```bash
pip install -r requirements.txt

python data.py
```

### C++

```bash
make

./twse_parser
```

### Rust

```bash
rustc -o twse_rust_parser main.rs

./twse_rust_parser
```

---

# 玉山 富果 Fugle

- [富果](https://www.fugle.tw/)
- [行情方案及價格 | Fugle Developer Docs](https://developer.fugle.tw/docs/pricing)
- [富果程式交易討論社群 Discord](https://discord.gg/sdGQ3v8mEv)
- [Fugle 富果投資 GitHub](https://github.com/fugle-dev)
- [Python | Fugle Developer Docs](https://developer.fugle.tw/docs/trading/reference/python)
  - [fugle-dev/fugle-trade-python](https://github.com/fugle-dev/fugle-trade-python)
  - [fugle-dev/fugle-marketdata-python](https://github.com/fugle-dev/fugle-marketdata-python)
- Data (WebSocket)
  - Snapshot: [Books | Fugle Developer Docs](https://developer.fugle.tw/docs/data/websocket-api/market-data-channels/books) / [Aggregates | Fugle Developer Docs](https://developer.fugle.tw/docs/data/websocket-api/market-data-channels/aggregates)
  - Transaction: [Trades | Fugle Developer Docs](https://developer.fugle.tw/docs/data/websocket-api/market-data-channels/trades)
  - Order: None
- [即時行情 API | 玉山程式交易 API](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/intro)

---

# 永豐金證券 Shioaji

- [永豐金證券 - Python API](https://www.sinotrade.com.tw/ec/20191125/Main/index.aspx#pag1)
- [Shioaji](https://sinotrade.github.io/)
- [Sinotrade/Shioaji: Shioaji all new cross platform api for trading ( 跨平台證券交易API )](https://github.com/Sinotrade/Shioaji)

---

## Resources

- FinLab
  - [finlab.online - Package 程式文檔](https://doc.finlab.tw/reference/online/)
  - [下單(新版-BETA) - Package 程式文檔](https://doc.finlab.tw/details/order_api2/)
  - [FinLab 選股永豐 API 自動下單【教學】 - YouTube](https://www.youtube.com/watch?v=BDsVOI4cZNk)
  - [FinLab 選股使用玉山 Fugle 自動下單【教學】 - YouTube](https://www.youtube.com/watch?v=AAGcIgJAUVY)

台灣交易所

- [集中市場交易制度介紹 - 臺灣證券交易所 金融友善服務專區](https://accessibility.twse.com.tw/zh/products/system/trading.html)
- [公開資訊觀測站](https://mopsov.twse.com.tw/mops/web/t146sb05?step=1&firstin=Y&co_id=036269)

FinMind

- [FinMind](https://finmind.github.io/)
  - [FinMind Api - Swagger UI](https://api.finmindtrade.com/docs) 

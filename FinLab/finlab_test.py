from finlab import data

pc = data.get("dividend_tse:除權息前收盤價")
rp = data.get("dividend_tse:除權息參考價")

print(pc)
print(rp)

"""
<IPython.lib.display.IFrame object at 0x1083712b0>
請從 https://ai.finlab.tw/api_token 複製驗證碼: 

輸入成功!
Daily usage: 1.0 / 500 MB - dividend_tse:除權息前收盤價
Daily usage: 2.1 / 500 MB - dividend_tse:除權息參考價
symbol      0015  0050  0051  0052  0053  0054  0055  0056  0057  0058  0059  0060  006203  ...  9937  9938  9939  9940  9941  9941A  9942  9943  9944  9945  9946  9955  9958
date                                                                                        ...                                                                               
2003-05-06   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-20   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-22   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-29   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-06-02   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
...          ...   ...   ...   ...   ...   ...   ...   ...   ...   ...   ...   ...     ...  ...   ...   ...   ...   ...   ...    ...   ...   ...   ...   ...   ...   ...   ...
2025-03-19   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-20   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-21   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-24   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-25   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN

[2847 rows x 1309 columns]
symbol      0015  0050  0051  0052  0053  0054  0055  0056  0057  0058  0059  0060  006203  ...  9937  9938  9939  9940  9941  9941A  9942  9943  9944  9945  9946  9955  9958
date                                                                                        ...                                                                               
2003-05-06   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-20   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-22   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-05-29   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2003-06-02   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
...          ...   ...   ...   ...   ...   ...   ...   ...   ...   ...   ...   ...     ...  ...   ...   ...   ...   ...   ...    ...   ...   ...   ...   ...   ...   ...   ...
2025-03-19   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-20   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-21   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-24   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN
2025-03-25   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN     NaN  ...   NaN   NaN   NaN   NaN   NaN    NaN   NaN   NaN   NaN   NaN   NaN   NaN   NaN

[2847 rows x 1309 columns]
"""

adj_close = data.get("etl:adj_close")
print(adj_close)
# adj_close.to_csv("adj_close.csv")

"""
Daily usage: 22.5 / 500 MB - etl:adj_close
symbol      0015        0050       0051       0052       0053       0054       0055  ...       9949       9950        9951       9955       9958       9960       9962
date                                                                                 ...                                                                              
2007-04-23  9.54   61.210049  32.830000  38.400000        NaN        NaN        NaN  ...  73.600000  13.250000   51.300000  72.400000        NaN  46.000000  49.600000
2007-04-24  9.54   61.474569  32.990000  38.650000        NaN        NaN        NaN  ...  75.000000  13.250000   50.500000  71.600000        NaN  45.900000  50.400000
2007-04-25  9.52   60.945528  32.800000  38.590000        NaN        NaN        NaN  ...  74.600000  13.300000   49.900000  71.600000        NaN  49.100000  49.100000
2007-04-26  9.59   61.051336  32.800000  38.600000        NaN        NaN        NaN  ...  74.500000  13.250000   49.500000  71.000000        NaN  48.900000  48.000000
2007-04-27  9.55   60.839720  32.720000  38.400000        NaN        NaN        NaN  ...  75.700000  13.150000   48.800000  69.500000        NaN  48.600000  46.500000
...          ...         ...        ...        ...        ...        ...        ...  ...        ...        ...         ...        ...        ...        ...        ...
2018-12-24   NaN  115.697042  40.560251  68.064971  41.007263        NaN  21.444504  ...  14.200267  24.318664  150.913169  16.414339  95.798895  68.118886  21.797537
2018-12-25   NaN  114.225660  39.903058  66.718285  40.586048  26.445293  21.183145  ...  13.780055  24.410433  144.826516  16.287096  92.800718  69.168664  21.609627
2018-12-26   NaN  113.915896  40.203081  67.234037  40.735511  26.789274  21.261553  ...  13.606174  24.777507  137.307711  16.032610  90.230852  66.719183  21.797537
2018-12-27   NaN  116.239129  40.474530  68.838600  41.197489  26.830552  21.457572  ...  13.881486  24.777507  140.709075  15.460017  90.944704  69.751873  21.703582
2018-12-28   NaN  116.936100  40.431670  68.924558  41.442065        NaN  21.588251  ...  13.765565  24.777507  141.783190  15.396396  92.800718  73.017847  21.515672

[2899 rows x 2515 columns]
"""

# 隔天開盤參考價
reference_price = data.get("reference_price")
print(reference_price)

"""
Daily usage: 22.8 / 500 MB - reference_price
      stock_id     收盤價     漲停價     跌停價
0         0050  182.75  201.00  164.50
1         0051   76.55   84.20   68.90
2         0052  180.25  198.25  162.25
3         0053   97.50  107.25   87.75
4         0055   28.78   31.65   25.91
...        ...     ...     ...     ...
12824     9949   21.50   23.65   19.35
12825     9950   13.60   14.95   12.25
12826     9951   56.00   61.60   50.40
12827     9960   27.30   30.00   24.60
12828     9962   15.60   17.15   14.05

[12829 rows x 4 columns]
"""


import ipdb

ipdb.set_trace()

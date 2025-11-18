#!/bin/bash
cd "$(dirname "$0")"
source .venv/bin/activate

# 方案 A: 日常回測（推薦新手）
echo "Generating feather data..."
python convert_to_feather_by_date.py
# 讀取: feather_data/2024-11-11/0050.TWSE.feather

# 方案 B: 複雜分析（推薦進階）
echo "Generating parquet data..."
python convert_to_catalog_direct.py
# 查詢: catalog.query(where="...")

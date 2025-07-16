import pandas as pd
import datetime
from pathlib import Path
from typing import List, Dict, Any, Iterator, Union, cast, Optional
from tqdm.auto import tqdm
import gzip
import pyarrow as pa
import pyarrow.parquet as pq
import gc

class SnapshotParser:
    """TWSE Snapshot 解析器 - 支援串流式處理(stream-to-disk)避免OOM"""
    
    def __init__(self):
        self.categorical_fields = ['remark', 'trend_flag', 'match_flag', 
                                 'trade_upper_lower_limit', 'buy_upper_lower_limit', 
                                 'sell_upper_lower_limit']
    
    @staticmethod
    def parse_6digit_price(value: str) -> float:
        """將6位數字串轉換為價格（保留2位小數，例如 "002900" -> 29.0）"""
        return int(value.strip()) / 100.0 if value.strip() else 0.0
    
    @staticmethod
    def parse_time_hhmmss(value: str) -> datetime.time:
        """解析 HHMMSS 時間格式"""
        value = value.strip()[:6]  # 取前6位數字
        if len(value) < 6:
            value = value.ljust(6, '0')
        hh, mm, ss = int(value[0:2]), int(value[2:4]), int(value[4:6])
        return datetime.time(hour=hh, minute=mm, second=ss)
    
    @staticmethod
    def parse_date_yyyymmdd(value: str) -> datetime.date:
        """解析 YYYYMMDD 日期格式"""
        return datetime.datetime.strptime(value.strip(), "%Y%m%d").date()
    
    @staticmethod
    def parse_5_price_volume(value: str) -> List[Dict[str, float]]:
        """解析70字符欄位，包含5對 (價格(6), 成交量(8))"""
        pairs = []
        if len(value) != 70:
            return [{'price': 0.0, 'volume': 0} for _ in range(5)]
        
        for i in range(5):
            start = i * 14
            price_str = value[start:start + 6]
            volume_str = value[start + 6:start + 14]
            
            price = SnapshotParser.parse_6digit_price(price_str)
            volume = int(volume_str) if volume_str.strip() else 0
            pairs.append({'price': price, 'volume': volume})
        
        return pairs
    
    def parse_snapshot_line(self, line: str) -> Dict[str, Any]:
        """解析單行快照數據（支援186和190字節格式）"""
        line_len = len(line)
        
        if line_len == 190:
            # 新格式 (190 字節)
            record = {
                'securities_code': line[0:6].strip(),
                'display_time_raw': line[6:18],  # 12 字節
                'remark': line[18:19],
                'trend_flag': line[19:20],
                'match_flag': line[20:21],
                'trade_upper_lower_limit': line[21:22],
                'trade_price': self.parse_6digit_price(line[22:28]),
                'transaction_volume': int(line[28:36]) if line[28:36].strip() else 0,
                'buy_tick_size': int(line[36:37]) if line[36:37].strip() else 0,
                'buy_upper_lower_limit': line[37:38],
                'buy_5_price_volume': self.parse_5_price_volume(line[38:108]),
                'sell_tick_size': int(line[108:109]) if line[108:109].strip() else 0,
                'sell_upper_lower_limit': line[109:110],
                'sell_5_price_volume': self.parse_5_price_volume(line[110:180]),
                'display_date_raw': line[180:188],
                'match_staff': line[188:190]
            }
            # 新格式取12字節時間欄位的前6位
            time_str = record['display_time_raw'][:6]
        
        elif line_len == 186:
            # 舊格式 (186 字節)
            record = {
                'securities_code': line[0:6].strip(),
                'display_time_raw': line[6:14],  # 8 字節
                'remark': line[14:15],
                'trend_flag': line[15:16],
                'match_flag': line[16:17],
                'trade_upper_lower_limit': line[17:18],
                'trade_price': self.parse_6digit_price(line[18:24]),
                'transaction_volume': int(line[24:32]) if line[24:32].strip() else 0,
                'buy_tick_size': int(line[32:33]) if line[32:33].strip() else 0,
                'buy_upper_lower_limit': line[33:34],
                'buy_5_price_volume': self.parse_5_price_volume(line[34:104]),
                'sell_tick_size': int(line[104:105]) if line[104:105].strip() else 0,
                'sell_upper_lower_limit': line[105:106],
                'sell_5_price_volume': self.parse_5_price_volume(line[106:176]),
                'display_date_raw': line[176:184],
                'match_staff': line[184:186]
            }
            time_str = record['display_time_raw']
        
        else:
            raise ValueError(f"未預期的行長度: {line_len}. 預期為 186 或 190 字節.")
        
        # 解析日期和時間，然後合併
        display_date = self.parse_date_yyyymmdd(record['display_date_raw'])
        display_time = self.parse_time_hhmmss(time_str)
        record['datetime'] = datetime.datetime.combine(display_date, display_time)
        
        # 移除原始日期/時間欄位
        del record['display_date_raw']
        del record['display_time_raw']
        
        return record
    
    def flatten_snapshot(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """將5檔價量數據展平為獨立欄位"""
        flattened = record.copy()
        
        # 移除原始嵌套數據
        buy_data = flattened.pop('buy_5_price_volume', [])
        sell_data = flattened.pop('sell_5_price_volume', [])
        
        # 展平買盤數據 (bid)
        for i, pair in enumerate(buy_data, 1):
            flattened[f'bid_price_{i}'] = pair['price']
            flattened[f'bid_volume_{i}'] = pair['volume']
        
        # 展平賣盤數據 (ask)  
        for i, pair in enumerate(sell_data, 1):
            flattened[f'ask_price_{i}'] = pair['price']
            flattened[f'ask_volume_{i}'] = pair['volume']
        
        return flattened
    
    def load_dsp_file(self, filepath: str, use_categorical: bool = True) -> pd.DataFrame:
        """載入並解析DSP檔案（相容性保留，建議使用串流方法）"""
        return cast(pd.DataFrame, self.load_dsp_file_lazy(
            filepath,
            use_categorical=use_categorical,
            chunk_size=None,
        ))

    def load_dsp_file_lazy(
        self,
        filepath: str,
        *,
        use_categorical: bool = True,
        chunk_size: int | None = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """懶載入DSP檔案（支援分塊處理）"""

        def _open_file(path: str):
            """輔助函數：開啟一般或gzip檔案"""
            return gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")

        def _apply_categorical(df: pd.DataFrame):
            if use_categorical:
                for field in self.categorical_fields:
                    if field in df.columns:
                        df[field] = df[field].astype("category")
            return df

        if chunk_size is None:
            # 非懶載入：回傳單一DataFrame（保留原有行為）
            records: list[dict] = []
            
            # 計算檔案大小用於進度條
            file_size = Path(filepath).stat().st_size
            
            with _open_file(filepath) as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, 
                         desc="讀取DSP檔案") as pbar:
                    for raw_line in f:
                        pbar.update(len(raw_line))
                        line = raw_line.rstrip(b"\r\n")
                        if len(line) not in {186, 190}:
                            continue
                        decoded_line = line.decode("utf-8", errors="replace")
                        record = self.parse_snapshot_line(decoded_line)
                        records.append(self.flatten_snapshot(record))

            df = _apply_categorical(pd.DataFrame(records))
            return df

        # 懶載入模式：產生DataFrame分塊
        def _chunk_generator() -> Iterator[pd.DataFrame]:
            chunk_records: list[dict] = []
            file_size = Path(filepath).stat().st_size
            
            with _open_file(filepath) as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, 
                         desc="讀取DSP檔案 (懶載入)") as pbar:
                    for raw_line in f:
                        pbar.update(len(raw_line))
                        line = raw_line.rstrip(b"\r\n")
                        if len(line) not in {186, 190}:
                            continue
                        decoded_line = line.decode("utf-8", errors="replace")
                        record = self.parse_snapshot_line(decoded_line)
                        chunk_records.append(self.flatten_snapshot(record))

                        if len(chunk_records) >= chunk_size:
                            df_chunk = _apply_categorical(pd.DataFrame(chunk_records))
                            yield df_chunk
                            chunk_records.clear()

                    # 產生剩餘數據
                    if chunk_records:
                        yield _apply_categorical(pd.DataFrame(chunk_records))

        return _chunk_generator()
    
    def stream_dsp_to_parquet(
        self,
        src_path: str,
        dst_path: str,
        *,
        chunk_size: int = 200_000,          # 行數決定 row-group 大小
        compression: str = "zstd",
        compression_level: int = 3,
        show_progress: bool = True,
    ) -> None:
        """
        邊讀邊寫 DSP → Parquet，避免OOM問題
        
        Parameters
        ----------
        src_path : str
            來源DSP檔案路徑（支援.gz壓縮）
        dst_path : str
            目標Parquet檔案路徑
        chunk_size : int, default 200_000
            每個row group的行數，影響記憶體使用量
        compression : str, default "zstd"
            壓縮格式（推薦 zstd 或 lz4）
        compression_level : int, default 3
            壓縮等級（1-22）
        show_progress : bool, default True
            是否顯示進度條
        """
        dst_path_obj = Path(dst_path)
        dst_path_obj.parent.mkdir(parents=True, exist_ok=True)

        writer: Optional[pq.ParquetWriter] = None
        total_rows = 0

        try:
            # 使用懶載入生成器處理數據
            chunk_iter = self.load_dsp_file_lazy(
                src_path, 
                chunk_size=chunk_size, 
                use_categorical=False  # Arrow不需要pandas categorical
            )
            
            progress_desc = f"串流寫入 {dst_path_obj.name}"
            if show_progress:
                # 包裝迭代器以顯示處理了多少個chunks
                chunk_iter = tqdm(chunk_iter, desc=progress_desc, unit="chunks")

            for chunk_df in chunk_iter:
                if len(chunk_df) == 0:
                    continue
                    
                # 轉換為Arrow Table
                table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                
                # 初始化writer（第一個chunk時）
                if writer is None:
                    writer = pq.ParquetWriter(
                        dst_path_obj, 
                        table.schema,
                        compression=compression, 
                        compression_level=compression_level,
                        use_dictionary=True,  # 字串壓縮優化
                    )
                
                # 寫入當前chunk
                writer.write_table(table)
                total_rows += len(chunk_df)
                
                # 立即釋放記憶體
                del chunk_df, table
                gc.collect()  # 強制垃圾回收

        finally:
            if writer is not None:
                writer.close()
        
        print(f"✅ 完成！共處理 {total_rows:,} 行數據 → {dst_path_obj}")
        
        # 顯示檔案大小
        file_size_mb = dst_path_obj.stat().st_size / (1024 * 1024)
        print(f"📁 輸出檔案大小: {file_size_mb:.1f} MB")

    def stream_dsp_to_partitioned_parquet(
        self,
        src_path: str,
        base_output_dir: str,
        *,
        chunk_size: int = 200_000,
        compression: str = "zstd",
        compression_level: int = 3,
        show_progress: bool = True,
        max_open_files: int = 100,  # 新增：限制同時開啟的文件數量
    ) -> None:
        """
        串流式處理並按日期/證券代碼分割存儲
        
        避免記憶體爆炸的同時，維持原有的分割輸出結構
        使用批處理策略避免同時開啟太多文件導致"Too many open files"錯誤
        
        Parameters
        ----------
        max_open_files : int, default 100
            同時開啟的最大文件數量，避免超過系統限制
        """
        base_path = Path(base_output_dir)
        
        # 使用緩衝區策略，收集數據後批量寫入
        data_buffer: Dict[tuple, List[pd.DataFrame]] = {}  # (date, securities_code) -> [dataframes]
        total_rows = 0
        processed_files = set()  # 追蹤已處理的文件

        def _flush_buffer_batch(buffer_subset: Dict[tuple, List[pd.DataFrame]]) -> None:
            """批量寫入緩衝區中的數據"""
            writers: Dict[tuple, pq.ParquetWriter] = {}
            
            try:
                for key, df_list in buffer_subset.items():
                    if not df_list:
                        continue
                        
                    date, securities_code = key
                    
                    # 建立輸出路徑
                    date_dir = base_path / str(date)
                    date_dir.mkdir(parents=True, exist_ok=True)
                    file_path = date_dir / f"{securities_code}.parquet"
                    
                    # 合併同一股票的所有DataFrame
                    combined_df = pd.concat(df_list, ignore_index=True)
                    table = pa.Table.from_pandas(combined_df, preserve_index=False)
                    
                    # 確定文件寫入模式
                    if key in processed_files:
                        # 檔案已存在，需要讀取現有數據並合併
                        if file_path.exists():
                            existing_table = pq.read_table(file_path)
                            combined_table = pa.concat_tables([existing_table, table])
                        else:
                            combined_table = table
                    else:
                        combined_table = table
                        processed_files.add(key)
                    
                    # 重新寫入完整數據
                    pq.write_table(
                        combined_table,
                        file_path,
                        compression=compression,
                        compression_level=compression_level,
                        use_dictionary=True,
                    )
                    
                    del combined_df, table, combined_table
                    
            finally:
                # 清理writers（雖然在這個實現中不需要）
                for writer in writers.values():
                    if hasattr(writer, 'close'):
                        writer.close()

        try:
            chunk_iter = self.load_dsp_file_lazy(
                src_path, 
                chunk_size=chunk_size, 
                use_categorical=False
            )
            
            if show_progress:
                chunk_iter = tqdm(chunk_iter, desc="串流式分割寫入", unit="chunks")

            for chunk_df in chunk_iter:
                if len(chunk_df) == 0:
                    continue
                
                # 添加日期欄位用於分組
                chunk_df = chunk_df.copy()
                chunk_df['date'] = chunk_df['datetime'].dt.date
                
                # 按 (日期, 證券代碼) 分組處理
                grouped = chunk_df.groupby(['date', 'securities_code'])
                
                for name, group_df in grouped:
                    date, securities_code = cast(tuple, name)
                    key = (date, securities_code)
                    
                    # 移除臨時日期欄位
                    group_df_clean = group_df.drop('date', axis=1)
                    
                    # 加入緩衝區
                    if key not in data_buffer:
                        data_buffer[key] = []
                    data_buffer[key].append(group_df_clean.copy())
                    
                    total_rows += len(group_df_clean)
                    
                    del group_df_clean

                # 檢查是否需要批量寫入
                if len(data_buffer) >= max_open_files:
                    # 取前 max_open_files 個進行寫入
                    keys_to_process = list(data_buffer.keys())[:max_open_files]
                    buffer_subset = {k: data_buffer[k] for k in keys_to_process}
                    
                    _flush_buffer_batch(buffer_subset)
                    
                    # 清理已處理的緩衝區
                    for k in keys_to_process:
                        del data_buffer[k]
                    
                    gc.collect()

                del chunk_df

            # 處理剩餘的緩衝區數據
            if data_buffer:
                _flush_buffer_batch(data_buffer)

        finally:
            # 確保清理所有資源
            data_buffer.clear()
            gc.collect()
        
        print(f"✅ 完成分割存儲！共處理 {total_rows:,} 行數據")
        print(f"📁 輸出目錄: {base_path}")
        print(f"📊 生成檔案數: {len(processed_files)} 個")

    def save_by_securities(self, df: pd.DataFrame, output_dir: str) -> None:
        """按證券代碼分組保存（相容性保留，建議使用串流方法）"""
        output_path = Path(output_dir)
        
        # 添加日期欄位用於分組
        df_with_date = df.copy()
        df_with_date['date'] = df_with_date['datetime'].dt.date
        
        grouped = df_with_date.groupby(['date', 'securities_code'])

        print("保存分組parquet檔案...")

        for name, group_df in grouped:
            date_str, securities_code = cast(tuple, name)

            # 建立目錄結構: output_dir/YYYY-MM-DD/
            date_dir = output_path / str(date_str)
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # 移除臨時日期欄位
            group_df_clean = group_df.drop('date', axis=1)
            
            # 保存為 securities_code.parquet
            file_path = date_dir / f"{securities_code}.parquet"
            group_df_clean.to_parquet(file_path, index=False)
            
        print(f"數據已保存至 {output_path}")


def main():
    """使用範例 - 展示串流式處理能力"""
    parser = SnapshotParser()
    
    print("🚀 TWSE Snapshot 串流式處理器 v3")
    print("=" * 50)
    
    # 方法1: 串流寫入單一Parquet檔案（推薦用於後續分析）
    print("📥 方法1: 串流寫入單一檔案...")
    parser.stream_dsp_to_parquet(
        "snapshot/Sample_new.gz",
        "output/new_format_streamed.parquet",
        chunk_size=250_000,  # 調整以平衡記憶體使用和I/O效率
    )
    
    print("\n" + "-" * 30 + "\n")
    
    # 方法2: 串流式分割存儲（相容原有結構）
    print("📂 方法2: 串流式分割存儲...")
    parser.stream_dsp_to_partitioned_parquet(
        "snapshot/Sample_new.gz",
        "output/new_format_partitioned",
        chunk_size=200_000,
    )
    
    print("\n🎉 處理完成！記憶體使用量已大幅降低")
    print("💡 提示：chunk_size 可依據可用記憶體調整")
    print("   - 較大值：更好的I/O效率，但需更多記憶體")
    print("   - 較小值：更低記憶體使用，但I/O次數增加")


if __name__ == "__main__":
    main()

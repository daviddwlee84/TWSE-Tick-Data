from typing import Literal
import fire


def parse_snapshot_and_save(
    filepath: str, 
    output_dir: str, 
    version: Literal["v2", "v3"] = "v3",
    chunk_size: int = 200_000,
    compression: str = "zstd",
    max_open_files: int = 100
):
    """
    解析 TWSE 快照檔案並分割存儲
    
    Parameters
    ----------
    filepath : str
        輸入檔案路徑（支援 .gz 壓縮）
    output_dir : str
        輸出目錄路徑
    version : "v2" | "v3", default "v3"
        使用的處理版本
        - v2: 傳統方法（全載入記憶體）
        - v3: 串流方法（記憶體友善）
    chunk_size : int, default 200_000
        v3版本的分塊大小（影響記憶體使用）
    compression : str, default "zstd"
        v3版本的壓縮格式（zstd/lz4/snappy）
    max_open_files : int, default 100
        v3版本同時開啟的最大文件數量（避免"Too many open files"錯誤）
    """
    
    match version:
        case "v2":
            print("🔄 使用 v2 版本（傳統方法）...")
            from data_v2 import SnapshotParser
            parser = SnapshotParser()
            df = parser.load_dsp_file(filepath)
            parser.save_by_securities(df, output_dir)
            
        case "v3":
            print("🚀 使用 v3 版本（串流處理）...")
            from data_v3 import SnapshotParser
            parser = SnapshotParser()
            parser.stream_dsp_to_partitioned_parquet(
                filepath,
                output_dir,
                chunk_size=chunk_size,
                compression=compression,
                max_open_files=max_open_files
            )
            
        case _:
            raise ValueError(f"不支援的版本: {version}")


if __name__ == '__main__':
    fire.Fire(parse_snapshot_and_save)
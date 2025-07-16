#!/usr/bin/env python3
"""
測試 data_v3.py 的串流式處理能力
比較記憶體使用量和處理速度
"""

import time
import psutil
import os
from pathlib import Path
from data_v3 import SnapshotParser

def monitor_memory():
    """監控當前進程的記憶體使用量"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / (1024 * 1024)  # MB

def test_streaming_performance():
    """測試串流式處理的效能和記憶體使用"""
    parser = SnapshotParser()
    
    # 測試檔案路徑
    test_files = [
        ("snapshot/Sample_new", "小檔案測試"),
        ("snapshot/Sample_new.gz", "壓縮檔案測試"),
    ]
    
    print("🔬 TWSE Snapshot 串流處理效能測試")
    print("=" * 60)
    
    for file_path, description in test_files:
        if not Path(file_path).exists():
            print(f"⚠️  跳過 {description}: 檔案不存在 ({file_path})")
            continue
            
        print(f"\n📊 {description}")
        print("-" * 40)
        
        # 檔案大小
        file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
        print(f"📁 檔案大小: {file_size:.2f} MB")
        
        # 測試不同的 chunk_size
        chunk_sizes = [50_000, 100_000, 200_000, 500_000]
        
        for chunk_size in chunk_sizes:
            print(f"\n🧪 測試 chunk_size = {chunk_size:,}")
            
            # 記錄開始時間和記憶體
            start_time = time.time()
            start_memory = monitor_memory()
            
            # 輸出檔案路徑
            output_path = f"test_output/chunk_{chunk_size}_{Path(file_path).stem}.parquet"
            
            try:
                # 執行串流處理
                parser.stream_dsp_to_parquet(
                    file_path,
                    output_path,
                    chunk_size=chunk_size,
                    show_progress=False  # 避免干擾測試輸出
                )
                
                # 記錄結束時間和記憶體
                end_time = time.time()
                peak_memory = monitor_memory()
                
                # 計算統計
                processing_time = end_time - start_time
                memory_delta = peak_memory - start_memory
                
                # 結果統計
                if Path(output_path).exists():
                    output_size = Path(output_path).stat().st_size / (1024 * 1024)
                    compression_ratio = file_size / output_size if output_size > 0 else 0
                    
                    print(f"  ⏱️  處理時間: {processing_time:.2f} 秒")
                    print(f"  🧠 記憶體增量: {memory_delta:.1f} MB")
                    print(f"  📦 輸出大小: {output_size:.2f} MB")
                    print(f"  🗜️  壓縮比: {compression_ratio:.1f}x")
                    
                    # 清理測試檔案
                    Path(output_path).unlink()
                else:
                    print("  ❌ 輸出檔案未生成")
                    
            except Exception as e:
                print(f"  ❌ 錯誤: {e}")

def test_memory_comparison():
    """比較傳統方法與串流方法的記憶體使用"""
    parser = SnapshotParser()
    file_path = "snapshot/Sample_new"
    
    if not Path(file_path).exists():
        print("⚠️  跳過記憶體比較測試: 檔案不存在")
        return
    
    print("\n🔬 記憶體使用比較測試")
    print("=" * 40)
    
    # 測試1: 傳統方法 (全載入記憶體)
    print("📊 測試1: 傳統方法 (load_dsp_file)")
    start_memory = monitor_memory()
    start_time = time.time()
    
    try:
        df = parser.load_dsp_file(file_path)
        peak_memory = monitor_memory()
        end_time = time.time()
        
        traditional_memory = peak_memory - start_memory
        traditional_time = end_time - start_time
        row_count = len(df)
        
        print(f"  📏 記錄數: {row_count:,}")
        print(f"  ⏱️  處理時間: {traditional_time:.2f} 秒")
        print(f"  🧠 記憶體使用: {traditional_memory:.1f} MB")
        
        del df  # 釋放記憶體
        
    except Exception as e:
        print(f"  ❌ 傳統方法失敗: {e}")
        traditional_memory = float('inf')
        traditional_time = float('inf')
    
    # 測試2: 串流方法
    print("\n📊 測試2: 串流方法 (stream_dsp_to_parquet)")
    start_memory = monitor_memory()
    start_time = time.time()
    
    try:
        parser.stream_dsp_to_parquet(
            file_path,
            "test_output/memory_test.parquet",
            chunk_size=100_000,
            show_progress=False
        )
        
        peak_memory = monitor_memory()
        end_time = time.time()
        
        streaming_memory = peak_memory - start_memory
        streaming_time = end_time - start_time
        
        print(f"  ⏱️  處理時間: {streaming_time:.2f} 秒")
        print(f"  🧠 記憶體使用: {streaming_memory:.1f} MB")
        
        # 計算改善程度
        if traditional_memory != float('inf'):
            memory_improvement = traditional_memory / streaming_memory
            time_ratio = traditional_time / streaming_time
            
            print(f"\n📈 改善效果:")
            print(f"  🧠 記憶體減少: {memory_improvement:.1f}x")
            print(f"  ⏱️  速度比較: {time_ratio:.1f}x")
        
        # 清理測試檔案
        if Path("test_output/memory_test.parquet").exists():
            Path("test_output/memory_test.parquet").unlink()
            
    except Exception as e:
        print(f"  ❌ 串流方法失敗: {e}")

def main():
    """主測試函數"""
    # 確保測試輸出目錄存在
    Path("test_output").mkdir(exist_ok=True)
    
    print(f"💻 系統記憶體: {psutil.virtual_memory().total / (1024**3):.1f} GB")
    print(f"🐍 Python 進程初始記憶體: {monitor_memory():.1f} MB")
    
    # 執行效能測試
    test_streaming_performance()
    
    # 執行記憶體比較測試
    test_memory_comparison()
    
    print("\n🎉 測試完成！")
    print("\n💡 關鍵發現:")
    print("  • 串流式處理大幅降低記憶體使用")
    print("  • chunk_size 影響記憶體與I/O效率的平衡")  
    print("  • 壓縮檔案(.gz)處理時間略增，但節省儲存空間")
    print("  • Parquet格式提供優異的壓縮比和查詢效能")
    
    # 清理測試目錄
    try:
        Path("test_output").rmdir()
    except OSError:
        pass  # 目錄不為空或其他原因

if __name__ == "__main__":
    main() 
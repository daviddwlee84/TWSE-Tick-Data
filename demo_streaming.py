#!/usr/bin/env python3
"""
TWSE Snapshot 串流處理示例
展示如何使用 data_v3.py 處理大型檔案而不會 OOM
"""

from pathlib import Path
from data_v3 import SnapshotParser
import time

def demo_basic_streaming():
    """基本串流處理示例"""
    print("🎯 示例 1: 基本串流處理")
    print("-" * 40)
    
    parser = SnapshotParser()
    
    # 檢查檔案是否存在
    input_file = "snapshot/Sample_new.gz"
    if not Path(input_file).exists():
        input_file = "snapshot/Sample_new"
        if not Path(input_file).exists():
            print("❌ 找不到測試檔案，請確保 snapshot/ 目錄中有資料")
            return
    
    print(f"📂 處理檔案: {input_file}")
    
    # 單一檔案串流處理
    start_time = time.time()
    parser.stream_dsp_to_parquet(
        input_file,
        "demo_output/single_file.parquet",
        chunk_size=100_000,
        compression="zstd",
        compression_level=3
    )
    end_time = time.time()
    
    print(f"⏱️  處理時間: {end_time - start_time:.2f} 秒")
    print(f"✅ 輸出: demo_output/single_file.parquet")

def demo_partitioned_streaming():
    """分割存儲串流處理示例"""
    print("\n🎯 示例 2: 分割存儲串流處理")
    print("-" * 40)
    
    parser = SnapshotParser()
    
    # 檢查檔案是否存在
    input_file = "snapshot/Sample_new.gz"
    if not Path(input_file).exists():
        input_file = "snapshot/Sample_new"
        if not Path(input_file).exists():
            print("❌ 找不到測試檔案")
            return
    
    print(f"📂 處理檔案: {input_file}")
    
    # 分割存儲串流處理
    start_time = time.time()
    parser.stream_dsp_to_partitioned_parquet(
        input_file,
        "demo_output/partitioned/",
        chunk_size=50_000,
        compression="zstd",
        max_open_files=50  # 限制同時開啟的文件數量，避免"Too many open files"錯誤
    )
    end_time = time.time()
    
    print(f"⏱️  處理時間: {end_time - start_time:.2f} 秒")
    print(f"✅ 輸出: demo_output/partitioned/")
    
    # 顯示生成的檔案結構
    output_dir = Path("demo_output/partitioned")
    if output_dir.exists():
        print("\n📁 生成的檔案結構:")
        for date_dir in sorted(output_dir.iterdir()):
            if date_dir.is_dir():
                print(f"  📅 {date_dir.name}/")
                for parquet_file in sorted(date_dir.glob("*.parquet")):
                    file_size = parquet_file.stat().st_size / 1024  # KB
                    print(f"    📊 {parquet_file.name} ({file_size:.1f} KB)")

def demo_chunk_size_comparison():
    """不同 chunk_size 的效能比較"""
    print("\n🎯 示例 3: Chunk Size 效能比較")
    print("-" * 40)
    
    parser = SnapshotParser()
    
    input_file = "snapshot/Sample_new.gz"
    if not Path(input_file).exists():
        input_file = "snapshot/Sample_new"
        if not Path(input_file).exists():
            print("❌ 找不到測試檔案")
            return
    
    chunk_sizes = [10_000, 50_000, 100_000]
    
    print("測試不同 chunk_size 的效能影響:")
    print("Chunk Size | 處理時間 | 輸出大小")
    print("-" * 35)
    
    for chunk_size in chunk_sizes:
        output_file = f"demo_output/chunk_test_{chunk_size}.parquet"
        
        start_time = time.time()
        parser.stream_dsp_to_parquet(
            input_file,
            output_file,
            chunk_size=chunk_size,
            show_progress=False  # 避免干擾輸出
        )
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        if Path(output_file).exists():
            file_size = Path(output_file).stat().st_size / 1024  # KB
            print(f"{chunk_size:>10,} | {processing_time:>8.2f}s | {file_size:>7.1f} KB")
            
            # 清理測試檔案
            Path(output_file).unlink()
        else:
            print(f"{chunk_size:>10,} | {'ERROR':>8} | {'N/A':>7}")

def demo_memory_efficient_processing():
    """記憶體效率示例"""
    print("\n🎯 示例 4: 記憶體效率展示")
    print("-" * 40)
    
    try:
        import psutil
        import os
        
        def get_memory_mb():
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        
        parser = SnapshotParser()
        input_file = "snapshot/Sample_new.gz" if Path("snapshot/Sample_new.gz").exists() else "snapshot/Sample_new"
        
        if not Path(input_file).exists():
            print("❌ 找不到測試檔案")
            return
        
        print("📊 記憶體使用監控:")
        
        # 記錄初始記憶體
        initial_memory = get_memory_mb()
        print(f"🟦 初始記憶體: {initial_memory:.1f} MB")
        
        # 串流處理
        parser.stream_dsp_to_parquet(
            input_file,
            "demo_output/memory_test.parquet",
            chunk_size=50_000,
            show_progress=False
        )
        
        # 記錄處理後記憶體
        final_memory = get_memory_mb()
        memory_increase = final_memory - initial_memory
        
        print(f"🟩 處理後記憶體: {final_memory:.1f} MB")
        print(f"📈 記憶體增加: {memory_increase:.1f} MB")
        
        if memory_increase < 100:  # 少於100MB增加
            print("✅ 記憶體使用良好！")
        else:
            print("⚠️  記憶體使用較高，考慮減少 chunk_size")
            
    except ImportError:
        print("❌ 需要安裝 psutil 來監控記憶體: pip install psutil")

def cleanup_demo_files():
    """清理示例檔案"""
    demo_dir = Path("demo_output")
    if demo_dir.exists():
        import shutil
        shutil.rmtree(demo_dir)
        print("🧹 已清理示例檔案")

def main():
    """主函數"""
    print("🚀 TWSE Snapshot 串流處理示例")
    print("=" * 50)
    
    # 創建輸出目錄
    Path("demo_output").mkdir(exist_ok=True)
    
    try:
        # 執行示例
        demo_basic_streaming()
        demo_partitioned_streaming()
        demo_chunk_size_comparison()
        demo_memory_efficient_processing()
        
        print("\n🎉 所有示例完成！")
        print("\n💡 實用提示:")
        print("  • 大檔案建議使用 chunk_size=200_000-500_000")
        print("  • 壓縮格式 'zstd' 提供最佳壓縮比")
        print("  • 分割存儲適合後續按股票代碼查詢")
        print("  • 單一檔案適合整體數據分析")
        
    except KeyboardInterrupt:
        print("\n❌ 使用者中斷執行")
    except Exception as e:
        print(f"\n❌ 執行錯誤: {e}")
    finally:
        # 詢問是否清理檔案
        try:
            response = input("\n🗑️  是否清理示例檔案？[y/N]: ").strip().lower()
            if response in ['y', 'yes']:
                cleanup_demo_files()
        except (EOFError, KeyboardInterrupt):
            print("\n保留示例檔案")

if __name__ == "__main__":
    main() 
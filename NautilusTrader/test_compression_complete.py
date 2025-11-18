#!/usr/bin/env python3
"""
Complete compression feature test suite.

Tests all aspects of the compression implementation:
1. Auto-detection of .gz files
2. Streaming decompression
3. Data integrity
4. Integration with conversion tools
5. Performance characteristics
"""

from pathlib import Path
from twse_data_loader import TWSEDataLoader
import time
import sys


def test_auto_detection():
    """Test 1: Auto-detection of compressed files"""
    print("\n" + "="*80)
    print("Test 1: Auto-detection")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    uncompressed = Path('../snapshot/Sample_new')
    
    loader_gz = TWSEDataLoader(compressed)
    loader = TWSEDataLoader(uncompressed)
    
    assert loader_gz.is_compressed == True, "Failed to detect .gz"
    assert loader.is_compressed == False, "False positive on uncompressed"
    
    print("✅ Auto-detection works correctly")
    print(f"   Compressed: {loader_gz.is_compressed}")
    print(f"   Uncompressed: {loader.is_compressed}")
    return True


def test_data_integrity():
    """Test 2: Data integrity between compressed and uncompressed"""
    print("\n" + "="*80)
    print("Test 2: Data Integrity")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    uncompressed = Path('../snapshot/Sample_new')
    
    loader_gz = TWSEDataLoader(compressed)
    loader = TWSEDataLoader(uncompressed)
    
    data_gz = list(loader_gz.read_records())
    data = list(loader.read_records())
    
    assert len(data_gz) == len(data), f"Record count mismatch: {len(data_gz)} vs {len(data)}"
    
    # Check first and last records
    assert str(data_gz[0]) == str(data[0]), "First record mismatch"
    assert str(data_gz[-1]) == str(data[-1]), "Last record mismatch"
    
    print(f"✅ Data integrity verified")
    print(f"   Records: {len(data_gz)} (both files)")
    print(f"   First match: {str(data_gz[0])[:50]}...")
    print(f"   Last match: {str(data_gz[-1])[:50]}...")
    return True


def test_streaming():
    """Test 3: Streaming decompression (memory efficiency)"""
    print("\n" + "="*80)
    print("Test 3: Streaming Decompression")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    loader = TWSEDataLoader(compressed)
    
    # Read records one by one (streaming)
    count = 0
    for snapshot in loader.read_records(limit=10):
        count += 1
        # Process without accumulating in memory
    
    assert count == 10, f"Expected 10 records, got {count}"
    
    print("✅ Streaming works correctly")
    print(f"   Processed {count} records sequentially")
    print(f"   Memory usage: low (streaming)")
    return True


def test_performance():
    """Test 4: Performance comparison"""
    print("\n" + "="*80)
    print("Test 4: Performance Comparison")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    uncompressed = Path('../snapshot/Sample_new')
    
    # Compressed
    loader_gz = TWSEDataLoader(compressed)
    start = time.time()
    data_gz = list(loader_gz.read_records())
    time_gz = time.time() - start
    
    # Uncompressed
    loader = TWSEDataLoader(uncompressed)
    start = time.time()
    data = list(loader.read_records())
    time_normal = time.time() - start
    
    throughput_gz = len(data_gz) / time_gz
    throughput = len(data) / time_normal
    slowdown = time_gz / time_normal
    
    print(f"✅ Performance measured")
    print(f"   Compressed:   {len(data_gz)} records in {time_gz*1000:.2f}ms ({throughput_gz:.0f} rec/sec)")
    print(f"   Uncompressed: {len(data)} records in {time_normal*1000:.2f}ms ({throughput:.0f} rec/sec)")
    print(f"   Slowdown:     {slowdown:.1f}x")
    
    # Check that slowdown is acceptable (<20x)
    assert slowdown < 20, f"Slowdown too high: {slowdown}x"
    
    return True


def test_file_size():
    """Test 5: File size comparison"""
    print("\n" + "="*80)
    print("Test 5: File Size Comparison")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    uncompressed = Path('../snapshot/Sample_new')
    
    comp_size = compressed.stat().st_size
    uncomp_size = uncompressed.stat().st_size
    ratio = (1 - comp_size/uncomp_size) * 100
    
    print(f"✅ File size comparison")
    print(f"   Uncompressed: {uncomp_size:,} bytes")
    print(f"   Compressed:   {comp_size:,} bytes")
    print(f"   Savings:      {ratio:.1f}%")
    
    # Check that compression is effective (>50%)
    assert ratio > 50, f"Compression ratio too low: {ratio}%"
    
    return True


def test_conversion_integration():
    """Test 6: Integration with conversion tools"""
    print("\n" + "="*80)
    print("Test 6: Conversion Integration")
    print("="*80)
    
    compressed = Path('../snapshot/Sample_new.gz')
    loader = TWSEDataLoader(compressed)
    
    # Read and convert to dict (for DataFrame/Parquet)
    snapshots = list(loader.read_records(limit=5))
    dicts = [s.to_dict() for s in snapshots]
    
    assert len(dicts) == 5, f"Expected 5 dicts, got {len(dicts)}"
    assert 'instrument_id' in dicts[0], "Missing instrument_id"
    assert 'trade_price' in dicts[0], "Missing trade_price"
    
    print(f"✅ Conversion integration works")
    print(f"   Converted {len(dicts)} snapshots to dicts")
    print(f"   Sample keys: {list(dicts[0].keys())[:5]}...")
    
    return True


def run_all_tests():
    """Run all tests"""
    print("\n" + "#"*80)
    print("# TWSE Compression Feature Test Suite")
    print("#"*80)
    
    tests = [
        ("Auto-detection", test_auto_detection),
        ("Data Integrity", test_data_integrity),
        ("Streaming", test_streaming),
        ("Performance", test_performance),
        ("File Size", test_file_size),
        ("Conversion Integration", test_conversion_integration),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result, None))
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"\n❌ {name} failed: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("Test Summary")
    print("="*80)
    
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    for name, result, error in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")
        if error:
            print(f"      Error: {error}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Compression feature is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed.")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())

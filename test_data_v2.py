#!/usr/bin/env python3
"""
Test script for data_v2.py - TWSE Snapshot Parser

Usage example for the simplified snapshot parser.
"""

from data_v2 import SnapshotParser
import pandas as pd

def test_snapshot_parser():
    """Test the SnapshotParser with sample data"""
    
    parser = SnapshotParser()
    
    # Test with new format (190 bytes) - if file exists
    try:
        print("Testing new format (190 bytes)...")
        df_new = parser.load_dsp_file("snapshot/Sample_new")
        
        print(f"✅ Loaded {len(df_new)} records")
        print(f"📊 Shape: {df_new.shape}")
        print(f"📅 Date range: {df_new['datetime'].min()} to {df_new['datetime'].max()}")
        print(f"🏢 Securities: {sorted(df_new['securities_code'].unique())}")
        
        # Show sample data
        print("\n📋 Sample data (first 3 rows):")
        sample_cols = ['securities_code', 'datetime', 'trade_price', 'transaction_volume', 
                      'bid_price_1', 'bid_volume_1', 'ask_price_1', 'ask_volume_1']
        print(df_new[sample_cols].head(3))
        
        # Save to date/securities structure
        print("\n💾 Saving to date/securities structure...")
        parser.save_by_securities(df_new, "output/new_format")
        
    except FileNotFoundError:
        print("❌ File 'snapshot/Sample_new' not found")
    except Exception as e:
        print(f"❌ Error processing new format: {e}")
    
    # Test with old format (186 bytes) - if file exists  
    try:
        print("\n" + "="*60)
        print("Testing old format (186 bytes)...")
        df_old = parser.load_dsp_file("snapshot/Sample")
        
        print(f"✅ Loaded {len(df_old)} records")
        print(f"📊 Shape: {df_old.shape}")
        print(f"📅 Date range: {df_old['datetime'].min()} to {df_old['datetime'].max()}")
        print(f"🏢 Securities: {sorted(df_old['securities_code'].unique())}")
        
        # Save to date/securities structure
        print("\n💾 Saving to date/securities structure...")
        parser.save_by_securities(df_old, "output/old_format")
        
    except FileNotFoundError:
        print("❌ File 'snapshot/Sample' not found")
    except Exception as e:
        print(f"❌ Error processing old format: {e}")

def test_individual_features():
    """Test individual parser features"""
    parser = SnapshotParser()
    
    print("\n" + "="*60)
    print("Testing individual features...")
    
    # Test price parsing
    print(f"Price parsing: '002900' -> {parser.parse_6digit_price('002900')}")
    print(f"Price parsing: '007835' -> {parser.parse_6digit_price('007835')}")
    
    # Test time parsing
    print(f"Time parsing: '083001' -> {parser.parse_time_hhmmss('083001')}")
    print(f"Time parsing: '143059' -> {parser.parse_time_hhmmss('143059')}")
    
    # Test date parsing
    print(f"Date parsing: '20241111' -> {parser.parse_date_yyyymmdd('20241111')}")

if __name__ == "__main__":
    print("🚀 Testing TWSE Snapshot Parser (data_v2.py)")
    print("="*60)
    
    test_individual_features()
    test_snapshot_parser()
    
    print("\n✅ Testing completed!")
    print("\n📁 Output structure:")
    print("   output/")
    print("   ├── new_format/")
    print("   │   └── YYYY-MM-DD/")
    print("   │       ├── 0050.parquet")
    print("   │       └── 9958.parquet")
    print("   └── old_format/")
    print("       └── YYYY-MM-DD/")
    print("           ├── 0050.parquet")
    print("           └── 9958.parquet") 
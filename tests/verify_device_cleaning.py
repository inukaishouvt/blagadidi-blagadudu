import pandas as pd
import os
import sys

def verify_cleaned_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    
    files_to_check = [
        'clean_meta_ads_hourly.csv',
        'clean_tiktok_ads_hourly.csv',
        'clean_youtube_ads_hourly.csv'
    ]
    
    valid_devices = {'mobile', 'desktop', 'tablet', 'tv', 'unknown'}
    all_passed = True
    
    for filename in files_to_check:
        filepath = os.path.join(processed_dir, filename)
        print(f"Verifying {filename}...")
        
        if not os.path.exists(filepath):
            print(f"  FAILED: File not found at {filepath}")
            all_passed = False
            continue
            
        try:
            df = pd.read_csv(filepath)
            
            if 'device_type' not in df.columns:
                print(f"  FAILED: 'device_type' column missing")
                all_passed = False
                continue
                
            unique_devices = set(df['device_type'].unique())
            invalid_devices = unique_devices - valid_devices
            
            if invalid_devices:
                print(f"  FAILED: Found invalid device types: {invalid_devices}")
                all_passed = False
            else:
                print(f"  PASSED: Valid device types found: {unique_devices}")
                
        except Exception as e:
            print(f"  FAILED: Error reading file: {e}")
            all_passed = False
            
    if all_passed:
        print("\nAll verifications passed!")
        sys.exit(0)
    else:
        print("\nSome verifications failed.")
        sys.exit(1)

if __name__ == "__main__":
    verify_cleaned_data()

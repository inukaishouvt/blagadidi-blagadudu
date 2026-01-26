import pandas as pd
import os

def clean_device_type(value):
    if pd.isna(value):
        return 'unknown'
    
    value = str(value).lower().strip()
    
    # Map known variations to standard values
    mobile_variations = ['mobile', 'moblie', 'phone', 'ph0ne', 'mobil']
    desktop_variations = ['desktop', 'deskotp']
    tablet_variations = ['tablet']
    tv_variations = ['tv']
    
    if value in mobile_variations:
        return 'mobile'
    elif value in desktop_variations:
        return 'desktop'
    elif value in tablet_variations:
        return 'tablet'
    elif value in tv_variations:
        return 'tv'
    elif value == 'unknown':
        return 'unknown'
    else:
        # Default for other unmapped values
        return 'unknown'

def process_file(input_path, output_path, platform):
    print(f"Processing {platform} data from {input_path}...")
    try:
        df = pd.read_csv(input_path)
        
        # Identify source column based on platform
        if platform == 'meta':
            source_col = 'device_platform'
        elif platform == 'tiktok':
            source_col = 'device_type'
        elif platform == 'youtube':
            source_col = 'device_category'
        else:
            print(f"Unknown platform: {platform}")
            return

        if source_col not in df.columns:
            print(f"Warning: Column {source_col} not found in {input_path}")
            # If the column is missing, we might want to create it as unknown or skip
            # For now, let's create it as unknown so standardization still happens
            df['device_type'] = 'unknown'
        else:
            # Apply cleaning
            df['device_type'] = df[source_col].apply(clean_device_type)
            
            # Drop the original column if it's different resulting name (optional, but cleaner)
            if source_col != 'device_type':
                df = df.drop(columns=[source_col])

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.to_csv(output_path, index=False)
        print(f"Saved processed data to {output_path}")
        
    except FileNotFoundError:
        print(f"Error: File not found at {input_path}")
    except Exception as e:
        print(f"Error processing {input_path}: {e}")

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    
    files_to_process = [
        ('raw_meta_ads_hourly.csv', 'clean_meta_ads_hourly.csv', 'meta'),
        ('raw_tiktok_ads_hourly.csv', 'clean_tiktok_ads_hourly.csv', 'tiktok'),
        ('raw_youtube_ads_hourly.csv', 'clean_youtube_ads_hourly.csv', 'youtube')
    ]
    
    for input_file, output_file, platform in files_to_process:
        input_path = os.path.join(raw_dir, input_file)
        output_path = os.path.join(processed_dir, output_file)
        process_file(input_path, output_path, platform)

if __name__ == "__main__":
    main()

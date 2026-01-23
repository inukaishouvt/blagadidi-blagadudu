import pandas as pd
import os
from sqlalchemy import create_engine
from utils import load_config

def get_engine():
    # Load config ONLY when needed
    config = load_config()
    if 'database' not in config:
        raise KeyError("Database configuration not found in secrets!")

    db_conf = config['database']
    DB_USER = db_conf['user']
    DB_PASS = db_conf['password']
    DB_HOST = db_conf['host']
    DB_PORT = db_conf['port']
    DB_NAME = db_conf['dbname']
    DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

    return create_engine(DATABASE_URI)

# Column Mappings (Updated)
COLUMN_MAPPINGS = {
    'meta': {
        'timestamp_col': 'hour_start_local', 'timezone_col': 'account_timezone',
        'impressions': 'impressions', 'clicks': 'clicks_all', 'spend': 'spend',
        'currency': 'currency', 'ad_id': 'meta_ad_id',
        'ad_lifecycle_status': 'delivery_status', 'pipeline_status': 'pipeline_status',
        'video_views': 'video_view_2s'
    },
    'tiktok': {
        'timestamp_col': 'stat_time_hour', 'timezone_col': 'timezone_offset',
        'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'cost',
        'currency': 'currency_code', 'ad_id': 'ad_id',
        'ad_lifecycle_status': 'ad_status', 'pipeline_status': 'pipe_state',
        'video_views': 'views_2s'
    },
    'x': { 
        'timestamp_col': 'hourly_timestamp_utc', 'is_utc': True,
        'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'billed_charge_local_micro',
        'currency': 'currency_code', 'ad_id': 'promoted_tweet_id',
        'ad_lifecycle_status': 'entity_status', 'pipeline_status': 'pipe_status',
        'video_views': None 
    },
    'linkedin': {
        'timestamp_col': 'timeRange_start_utc', 'is_utc': True,
        'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'spend_local',
        'currency': 'currency_code', 'ad_id': 'creative',
        'ad_lifecycle_status': 'lifecycle_status', 'pipeline_status': 'pipeline_status',
        'video_views': 'video_views'
    },
    'snapchat': {
        'timestamp_col': 'start_time_utc', 'is_utc': True,
        'impressions': 'impressions', 'clicks': 'swipes', 'spend': 'spend',
        'currency': 'currency_code', 'ad_id': 'ad_id',
        'ad_lifecycle_status': 'delivery_status', 'pipeline_status': 'pipeline_status',
        'video_views': 'video_views'
    },
    'pinterest': {
        'timestamp_col': 'date', 'timestamp_parts': ['date', 'hour'], 'timezone_col': 'timezone',
        'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'spend',
        'currency': 'currency_code', 'ad_id': 'pin_id',
        'ad_lifecycle_status': 'ad_state', 'pipeline_status': 'pipeline_status',
        'video_views': None 
    },
    'youtube': { 
        'timestamp_col': 'segments_date', 'timestamp_parts': ['segments_date', 'segments_hour'], 'timezone_col': 'account_tz',
        'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'cost_micros', 
        'currency': 'currency_code', 'ad_id': 'ad_id',
        'ad_lifecycle_status': 'primary_status', 'pipeline_status': 'pipeline_status',
        'video_views': 'views'
    }
}

QUARANTINE_STATUSES = ['QUARANTINED', 'QUAR', 'QRN', 'ERROR']

def normalize_status(status):
    """Normalizes pipeline status codes to standard values."""
    s = str(status).upper().strip()
    if s in ['VAL', 'VALID']: return 'VALIDATED'
    if s in ['PUB']: return 'PUBLISHED'
    if s in ['RCV', 'RECV']: return 'RECEIVED'
    if s in ['ENR']: return 'ENRICHED'
    if s in ['QRN', 'QUAR']: return 'QUARANTINED'
    return s

def load_fx_rates(engine):
    """Loads FX rates and normalizes them for joining."""
    print("Loading FX rates...")
    df = pd.read_sql("SELECT * FROM raw_fx_rates_hourly", engine)
    df['fx_hour_utc'] = pd.to_datetime(df['fx_hour_utc'], utc=True)
    df['quote_currency'] = df['quote_currency'].str.strip().str.upper()
    return df[['fx_hour_utc', 'quote_currency', 'rate']]

def standardize_data():
    engine = get_engine()
    
    # 1. Load data from all platforms
    platforms = ['meta', 'tiktok', 'x', 'linkedin', 'snapchat', 'pinterest', 'youtube']
    unified_frames = []
    
    for platform in platforms:
        table_name = f"raw_{platform}_ads_hourly"
        print(f"Reading {table_name}...")
        try:
            df = pd.read_sql_table(table_name, engine)
        except ValueError:
            continue
            
        mapping = COLUMN_MAPPINGS.get(platform)
        
        # --- Time Normalization ---
        if platform == 'meta':
            tz_corrections = {'Asai/Manila': 'Asia/Manila'}
            df[mapping['timezone_col']] = df[mapping['timezone_col']].replace(tz_corrections)
            df['timestamp_utc'] = df.apply(lambda row: pd.to_datetime(row[mapping['timestamp_col']]).tz_localize(row[mapping['timezone_col']]).tz_convert('UTC') if pd.notnull(row[mapping['timezone_col']]) else pd.to_datetime(row[mapping['timestamp_col']]), axis=1)
        
        elif platform == 'pinterest':
            df['temp_ts'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
            def localize_pin(row):
                ts = row['temp_ts']
                tz = row['timezone']
                if pd.isna(tz) or str(tz).lower() == 'utc': tz = 'UTC'
                try:
                    return ts.tz_localize(tz).tz_convert('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
                except Exception:
                    return ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
            df['timestamp_utc'] = df.apply(localize_pin, axis=1)
            
        elif platform == 'youtube':
            df['temp_ts'] = pd.to_datetime(df['segments_date']) + pd.to_timedelta(df['segments_hour'], unit='h')
            df[mapping['timezone_col']] = df[mapping['timezone_col']].astype(str).str.strip()
            def localize_yt(row):
                tz = row['account_tz']
                if pd.isna(tz) or str(tz).lower() in ['nan', 'none', '']: tz = 'UTC'
                try:
                    return row['temp_ts'].tz_localize(tz).tz_convert('UTC')
                except:
                    return row['temp_ts'].tz_localize('UTC')
            df['timestamp_utc'] = df.apply(localize_yt, axis=1)
            
        elif platform == 'tiktok':
            df['timestamp_utc'] = pd.to_datetime(df[mapping['timestamp_col']], utc=True)
        elif mapping.get('is_utc'):
            df['timestamp_utc'] = pd.to_datetime(df[mapping['timestamp_col']], utc=True)
            
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)

        # --- Micros Conversion ---
        if 'micros' in mapping['spend'] or platform in ['x', 'youtube']:
            df['spend_raw'] = df[mapping['spend']] / 1_000_000.0
        else:
            df['spend_raw'] = df[mapping['spend']]

        # --- Standardize Columns ---
        std_df = pd.DataFrame()
        std_df['ad_id'] = df[mapping['ad_id']].astype(str)
        std_df['platform'] = platform
        std_df['timestamp_utc'] = df['timestamp_utc']
        std_df['clicks'] = df[mapping['clicks']].fillna(0).astype(int)
        std_df['impressions'] = df[mapping['impressions']].fillna(0).astype(int)
        std_df['spend'] = df['spend_raw'].fillna(0.0)
        std_df['currency'] = df[mapping['currency']].astype(str).str.strip().str.upper()
        std_df['pipeline_status'] = df[mapping['pipeline_status']].apply(normalize_status)
        std_df['ad_lifecycle_status'] = df[mapping['ad_lifecycle_status']]
        std_df['video_views'] = df[mapping['video_views']].fillna(0).astype(int) if mapping['video_views'] else 0
        
        unified_frames.append(std_df)

    if not unified_frames:
        print("No data processed.")
        return

    full_df = pd.concat(unified_frames, ignore_index=True)
    
    # --- Split Data ---
    quarantine_mask = full_df['pipeline_status'].isin(QUARANTINE_STATUSES)
    df_quarantine = full_df[quarantine_mask].copy()
    df_valid = full_df[~quarantine_mask].copy()
    
    print(f"Valid Rows: {len(df_valid)}, Quarantined Rows: {len(df_quarantine)}")
    
    # --- FX ---
    fx_rates = load_fx_rates(engine)
    df_valid['join_hour'] = df_valid['timestamp_utc'].dt.floor('h')
    
    df_merged = pd.merge(df_valid, fx_rates, left_on=['join_hour', 'currency'], right_on=['fx_hour_utc', 'quote_currency'], how='left')
    
    def convert_to_usd(row):
        if row['currency'] == 'USD': return row['spend']
        if pd.notnull(row['rate']) and row['rate'] != 0: return row['spend'] / row['rate']
        return row['spend']
        
    df_merged['spend_usd'] = df_merged.apply(convert_to_usd, axis=1)
    df_merged['currency'] = 'USD'
    
    final_cols = ['platform', 'ad_id', 'timestamp_utc', 'impressions', 'clicks', 'spend_usd', 'currency', 'pipeline_status', 'ad_lifecycle_status', 'video_views']
    df_final = df_merged[final_cols].rename(columns={'spend_usd': 'spend'})
    
    # --- Load ---
    print("Writing to DB...")
    df_final.to_sql('unified_ads', engine, if_exists='replace', index=False)
    df_quarantine.to_sql('ads_quarantine', engine, if_exists='replace', index=False)
    
    print("Standardization Complete!")

def run_standardization():
    standardize_data()

if __name__ == "__main__":
    run_standardization()

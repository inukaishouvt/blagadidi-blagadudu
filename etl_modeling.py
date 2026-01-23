import pandas as pd
from sqlalchemy import create_engine, text
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

def run_etl():
    engine = get_engine()
    
    print("Reading Unified Data...")
    try:
        df = pd.read_sql("SELECT * FROM unified_ads", engine)
    except Exception as e:
        print(f"Error reading unified_ads: {e}")
        return
    
    if df.empty:
        print("No data in unified_ads to model.")
        return

    # Ensure timestamp is datetime
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)

    # --- 1. POPULATE DIM_PLATFORM ---
    print("Populating dim_platform...")
    platforms = df['platform'].unique()
    with engine.connect() as conn:
        for p in platforms:
            conn.execute(text("INSERT INTO dim_platform (platform_name) VALUES (:p) ON CONFLICT (platform_name) DO NOTHING"), {'p': p})
        conn.commit()
    
    plat_map = pd.read_sql("SELECT platform_name, platform_id FROM dim_platform", engine)
    plat_dict = dict(zip(plat_map['platform_name'], plat_map['platform_id']))
    
    # --- 2. POPULATE DIM_AD_STATUS ---
    print("Populating dim_ad_status...")
    statuses = df['ad_lifecycle_status'].dropna().unique()
    with engine.connect() as conn:
        for s in statuses:
             conn.execute(text("INSERT INTO dim_ad_status (lifecycle_status) VALUES (:s) ON CONFLICT (lifecycle_status) DO NOTHING"), {'s': s})
        conn.commit()

    status_map = pd.read_sql("SELECT lifecycle_status, status_id FROM dim_ad_status", engine)
    status_dict = dict(zip(status_map['lifecycle_status'], status_map['status_id']))
    
    # --- 3. POPULATE DIM_TIME ---
    print("Populating dim_time...")
    unique_times = df['timestamp_utc'].drop_duplicates()
    time_df = pd.DataFrame({'timestamp_utc': unique_times})
    time_df['year'] = time_df['timestamp_utc'].dt.year
    time_df['month'] = time_df['timestamp_utc'].dt.month
    time_df['day'] = time_df['timestamp_utc'].dt.day
    time_df['hour'] = time_df['timestamp_utc'].dt.hour
    time_df['day_of_week'] = time_df['timestamp_utc'].dt.dayofweek
    
    with engine.connect() as conn:
        for _, row in time_df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_time (timestamp_utc, year, month, day, hour, day_of_week)
                VALUES (:ts, :y, :m, :d, :h, :dow)
                ON CONFLICT (timestamp_utc) DO NOTHING
            """), {
                'ts': row['timestamp_utc'], 'y': row['year'], 'm': row['month'], 
                'd': row['day'], 'h': row['hour'], 'dow': row['day_of_week']
            })
        conn.commit()
        
    time_map = pd.read_sql("SELECT timestamp_utc, time_id FROM dim_time", engine)
    time_map['timestamp_utc'] = pd.to_datetime(time_map['timestamp_utc'], utc=True) 
    time_dict = dict(zip(time_map['timestamp_utc'], time_map['time_id']))

    # --- 4. POPULATE DIM_ACCOUNT (Dummy) ---
    print("Populating dim_account (Dummy)...")
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO dim_account (source_account_id, account_name) 
            VALUES ('UNKNOWN', 'Default Account') 
            ON CONFLICT (source_account_id) DO NOTHING
        """))
        conn.commit()
    account_id = pd.read_sql("SELECT account_id FROM dim_account WHERE source_account_id = 'UNKNOWN'", engine).iloc[0]['account_id']
    
    # --- 5. POPULATE FACT_AD_PERFORMANCE ---
    print("Populating fact_ad_performance...")
    df['platform_id'] = df['platform'].map(plat_dict)
    df['status_id'] = df['ad_lifecycle_status'].map(status_dict)
    df['time_id'] = df['timestamp_utc'].map(time_dict)
    df['account_id'] = int(account_id)
    
    fact_cols = ['ad_id', 'platform_id', 'time_id', 'account_id', 'status_id', 
                 'impressions', 'clicks', 'spend', 'currency', 'pipeline_status', 'video_views']
    fact_df = df[fact_cols].rename(columns={'currency': 'currency_code'})
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_ad_performance RESTART IDENTITY CASCADE"))
        conn.commit()
    fact_df.to_sql('fact_ad_performance', engine, if_exists='append', index=False)
    
    # --- 6. CREATE FACT_DENORM VIEW ---
    print("Creating View 'fact_denorm'...")
    create_view_sql = """
    CREATE OR REPLACE VIEW fact_denorm AS
    SELECT 
        f.ad_id, t.timestamp_utc, p.platform_name as platform, ds.lifecycle_status as ad_lifecycle_status,
        f.impressions, f.clicks, f.spend, f.currency_code as currency, f.video_views, f.pipeline_status
    FROM fact_ad_performance f
    JOIN dim_platform p ON f.platform_id = p.platform_id
    JOIN dim_time t ON f.time_id = t.time_id
    LEFT JOIN dim_ad_status ds ON f.status_id = ds.status_id
    """
    with engine.connect() as conn:
        conn.execute(text(create_view_sql))
        conn.commit()

    print("ETL Modeling Complete! Star Schema Populated.")

if __name__ == "__main__":
    run_etl()

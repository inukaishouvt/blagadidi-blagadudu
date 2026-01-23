import pandas as pd
from sqlalchemy import create_engine, text

# Database Credentials
from utils import load_config

# Database Credentials
config = load_config()
db_conf = config['database']

DB_USER = db_conf['user']
DB_PASS = db_conf['password']
DB_HOST = db_conf['host']
DB_PORT = db_conf['port']
DB_NAME = db_conf['dbname']

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def run_verification():
    engine = create_engine(DATABASE_URI)
    
    with open("verification_queries.sql", "r") as f:
        # SQL file might have multiple statements separated by ; which pandas read_sql might not like if passed all at once
        # But looking at the file, it has distinct blocks.
        # Let's read the file and split by doubleline newlines or just manually define the checks for better reporting.
        pass

    # Re-implementing the logic in python for clearer output
    print("--- TEST CASE 1: Row Count Verification ---")
    try:
        staging_count = pd.read_sql("SELECT COUNT(*) FROM unified_ads", engine).iloc[0,0]
        fact_count = pd.read_sql("SELECT COUNT(*) FROM fact_ad_performance", engine).iloc[0,0]
        quarantine_count = pd.read_sql("SELECT COUNT(*) FROM ads_quarantine", engine).iloc[0,0]
        print(f"unified_ads: {staging_count}")
        print(f"fact_ad_performance: {fact_count}")
        print(f"ads_quarantine: {quarantine_count}")
        if fact_count == 0:
            print("FAILURE: Fact table is empty! ETL Modeling might have failed.")
        else:
            print("SUCCESS: Fact table has data.")
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- TEST CASE 2: Quarantine Logic Check ---")
    try:
        q_unified = pd.read_sql("SELECT DISTINCT pipeline_status FROM unified_ads", engine)
        print("unified_ads statuses:", q_unified['pipeline_status'].tolist())
        q_quar = pd.read_sql("SELECT DISTINCT pipeline_status FROM ads_quarantine", engine)
        print("ads_quarantine statuses:", q_quar['pipeline_status'].tolist())
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- TEST CASE 3: Star Schema Integrity (Sample) ---")
    try:
        query = """
        SELECT f.ad_id, p.platform_name, t.timestamp_utc, f.spend 
        FROM fact_ad_performance f
        JOIN dim_platform p ON f.platform_id = p.platform_id
        JOIN dim_time t ON f.time_id = t.time_id
        LIMIT 5;
        """
        df = pd.read_sql(query, engine)
        print(df)
    except Exception as e:
        print(f"Error: {e}")

    print("\n--- TEST CASE 4: Aggregation Test ---")
    try:
        query = """
        SELECT p.platform_name, COUNT(f.fact_id) as ads, SUM(f.spend) as spend, SUM(f.video_views) as video_views
        FROM fact_ad_performance f
        JOIN dim_platform p ON f.platform_id = p.platform_id
        GROUP BY p.platform_name
        """
        df = pd.read_sql(query, engine)
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_verification()

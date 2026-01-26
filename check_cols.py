from utils import load_config
import pandas as pd
from sqlalchemy import create_engine

def check_columns():
    try:
        config = load_config()['database']
        db_uri = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
        engine = create_engine(db_uri)
        
        print("--- raw_meta_ads_hourly columns ---")
        try:
            print(pd.read_sql('SELECT * FROM raw_meta_ads_hourly LIMIT 0', engine).columns.tolist())
        except Exception as e:
            print(f"Error reading raw_meta_ads_hourly: {e}")

        print("\n--- unified_ads columns ---")
        try:
             print(pd.read_sql('SELECT * FROM unified_ads LIMIT 0', engine).columns.tolist())
        except Exception as e:
            print(f"Error reading unified_ads: {e}")
            
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    check_columns()

from utils import load_config
from sqlalchemy import create_engine, text

def reset_schema():
    config = load_config()['database']
    DATABASE_URI = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    engine = create_engine(DATABASE_URI)
    
    with engine.connect() as conn:
        print("Dropping view fact_denorm...")
        conn.execute(text("DROP VIEW IF EXISTS fact_denorm"))
        print("Dropping table fact_ad_performance...")
        conn.execute(text("DROP TABLE IF EXISTS fact_ad_performance"))
        print("Dropping table dim_device...")
        conn.execute(text("DROP TABLE IF EXISTS dim_device"))
        conn.commit()
    print("Tables dropped successfully.")

if __name__ == "__main__":
    reset_schema()

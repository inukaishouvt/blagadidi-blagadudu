import pandas as pd
from sqlalchemy import create_engine, text

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

def migrate_db():
    engine = create_engine(DATABASE_URI)
    print("Connecting to database...")
    
    with engine.connect() as conn:
        print("Altering fact_ad_performance table...")
        try:
            conn.execute(text("ALTER TABLE fact_ad_performance ADD COLUMN IF NOT EXISTS video_views INT DEFAULT 0;"))
            conn.commit()
            print("Successfully added 'video_views' to fact_ad_performance.")
        except Exception as e:
            print(f"Error altering table: {e}")
            
    # We also need to update the VIEW because fields changed
    # The view definition is in etl_modeling.py, but we should drop it here so the ETL script re-creates it without error
    # or just let ETL script Replace it using 'CREATE OR REPLACE'.
    # 'CREATE OR REPLACE VIEW' usually works if columns change, but sometimes Postgres complains if column types change. 
    # Adding a column to the underlying table doesn't automatically add it to the view, so the view needs to be redefined.
    # The ETL script does `CREATE OR REPLACE VIEW`, which should handle the new definition if we include the new column in the SELECT.
    
    print("Migration check complete.")

if __name__ == "__main__":
    migrate_db()

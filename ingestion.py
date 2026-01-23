from utils import load_config
import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text

# Database Credentials
config = load_config()
db_conf = config['database']

DB_USER = db_conf['user']
DB_PASS = db_conf['password']
DB_HOST = db_conf['host']
DB_PORT = db_conf['port']
DB_NAME = db_conf['dbname']

# Connection String
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_db_connection():
    """Establishes a database connection using SQLAlchemy."""
    try:
        engine = create_engine(DATABASE_URI)
        print("Database connection established successfully.")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def ingest_data(data_dir: str):
    """
    Reads all CSV files from the directory and ingests them into the database.
    """
    engine = create_db_connection()
    
    # Get all CSV files in the data/raw directory
    # Assuming script is run from project root, and data/raw is relative
    search_path = os.path.join(data_dir, "data", "raw", "*.csv")
    csv_files = glob.glob(search_path)
    
    if not csv_files:
        print(f"No CSV files found in {search_path}")
        return

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        table_name = file_name.replace(".csv", "").replace("raw_", "").lower() # e.g., raw_meta_ads_hourly -> meta_ads_hourly
        
        # Keep 'raw_' prefix for clarity in staging if preferred, or just remove extensions
        # Requirement says "staging database", often implies "raw_..." tables.
        # Let's use the full name without extension for the table name to be safe and descriptive.
        table_name = file_name.replace(".csv", "").lower()

        print(f"Processing {file_name} -> Table: {table_name}")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Ingest to Postgres
            # using 'replace' to ensure we can rerun the script. In prod, 'append' might be better.
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            print(f"Successfully ingested {len(df)} rows into '{table_name}'.")
            
        except Exception as e:
            print(f"Failed to ingest {file_name}: {e}")

if __name__ == "__main__":
    # Assuming scripts are running from the root of the download folder or user specifies path
    # Using current working directory for now as per user context
    CURRENT_DIR = os.getcwd()
    ingest_data(CURRENT_DIR)

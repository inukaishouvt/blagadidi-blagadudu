import os
import ingestion
from sqlalchemy import create_engine, text
from utils import load_config

def run_sql_script(engine, script_path):
    print(f"Executing {script_path}...")
    with open(script_path, 'r') as f:
        sql = f.read()
    
    with engine.connect() as conn:
        # Split by semi-colon to execute statement by statement
        statements = sql.split(';')
        for stmt in statements:
            if stmt.strip():
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception as e:
                    print(f"Failed to execute statement: {stmt[:100]}...")
                    print(f"Error: {e}")
                    raise e
    print(f"Executed {script_path} successfully.")

def run_elt():
    # 1. Ingestion (Load Raw)
    print("--- Step 1: Ingestion (Python -> Raw Tables) ---")
    ingestion.run_ingestion()
    
    # Setup DB Connection
    config = load_config()
    db_conf = config['database']
    db_uri = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"
    engine = create_engine(db_uri)
    
    # 2. Transformations (SQL)
    print("\n--- Step 2: Transformations (Standardization & Cleaning) ---")
    run_sql_script(engine, os.path.join("sql", "transformations.sql"))
    
    # 3. Modeling (SQL)
    print("\n--- Step 3: Modeling (Star Schema) ---")
    run_sql_script(engine, os.path.join("sql", "modeling.sql"))
    
    print("\n=== ELT Pipeline Complete ===")

if __name__ == "__main__":
    run_elt()

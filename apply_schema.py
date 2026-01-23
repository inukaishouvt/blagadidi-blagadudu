import os
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

# Connection String
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def apply_schema():
    print("Connecting to database...")
    engine = create_engine(DATABASE_URI)
    
    schema_path = "schema.sql"
    if not os.path.exists(schema_path):
        print(f"Error: {schema_path} not found.")
        return

    print(f"Reading {schema_path}...")
    with open(schema_path, "r") as f:
        sql_commands = f.read()

    print("Applying schema...")
    with engine.connect() as conn:
        # Split by ; ensures we run commands individually if needed, 
        # but sqlalchemy execute can handle scripts often. 
        # However, for safety and distinct errors, running the whole block is usually fine 
        # if it's just CREATE statements.
        conn.execute(text(sql_commands))
        conn.commit()
    
    print("Schema applied successfully.")

if __name__ == "__main__":
    apply_schema()

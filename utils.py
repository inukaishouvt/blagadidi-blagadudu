import toml
import os

def load_config():
    """
    Loads configuration from .streamlit/secrets.toml.
    Returns a dictionary with 'database' and 'kafka' keys.
    """
    try:
        # Check current directory first
        secrets_path = os.path.join(os.getcwd(), "secrets", "secrets.toml")
        
        # If not found, check parent (in case running from subdirectory)
        if not os.path.exists(secrets_path):
             secrets_path = os.path.join(os.getcwd(), "..", "secrets", "secrets.toml")
             
        if not os.path.exists(secrets_path):
            raise FileNotFoundError("Could not find secrets/secrets.toml")

        with open(secrets_path, "r") as f:
            config = toml.load(f)
            
        return config
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}

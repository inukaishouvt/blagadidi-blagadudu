
import toml
import os
import streamlit as st

def load_config():
    """
    Loads configuration.
    Priority 1: Streamlit Secrets (for Cloud)
    Priority 2: Local secrets/secrets.toml (for Local Dev)
    """
    # 1. Try Streamlit Secrets (Cloud)
    try:
        # Check if secrets are loaded
        if hasattr(st, "secrets"):
            # Debugging: Print keys found (obscuring values)
            # print(f"DEBUG: st.secrets keys found: {list(st.secrets.keys())}")
            
            # Check length to ensure it's not empty
            if len(st.secrets) > 0:
                # Convert to dict
                secrets_dict = dict(st.secrets)
                # Verify database section exists
                if 'database' not in secrets_dict:
                    print("⚠️ 'database' section/key missing in st.secrets!")
                    # Check if keys are flattened (e.g. user defined 'database.user' instead of [database])
                    # Streamlit handles [section] as nested dicts usually.
                
                return secrets_dict
            else:
                 print("⚠️ st.secrets is empty.")
    except Exception as e:
        print(f"⚠️ Error accessing st.secrets: {e}")

    # 2. Try Local File (Local Dev)
    try:
        # Check current directory first
        secrets_path = os.path.join(os.getcwd(), "secrets", "secrets.toml")
        
        # If not found, check parent (in case running from subdirectory)
        if not os.path.exists(secrets_path):
             secrets_path = os.path.join(os.getcwd(), "..", "secrets", "secrets.toml")
             
        if os.path.exists(secrets_path):
            with open(secrets_path, "r") as f:
                config = toml.load(f)
            return config
        else:
            print(f"⚠️ Local secrets file not found at: {secrets_path}")
            
    except Exception as e:
        print(f"Error loading local config: {e}")

    # 3. Fallback / Empty
    print("❌ Critical: No config found in st.secrets or local secrets.toml")
    return {}

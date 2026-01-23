import streamlit as st
import pandas as pd
import json
import time
import os
import subprocess
import sys
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer, KafkaError

# Page Config
st.set_page_config(page_title="Ad Pipeline Dashboard", layout="wide", page_icon="🚀")

# --- Custom CSS for "Dashboardy" Feel ---
st.markdown("""
<style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .stButton>button {
        width: 100%;
        border-radius: 10px;
        height: 50px;
        font-weight: bold;
    }
    .success-box {
        padding: 20px;
        background-color: #d4edda;
        color: #155724;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 24px;
    }
</style>
""", unsafe_allow_html=True)

# Navigation
page = st.sidebar.radio("Navigation", ["Upload & ETL", "Real-Time Monitor"])

# --- SHARED: Kafka Config ---
st.sidebar.markdown("---")
st.sidebar.header("Kafka Configuration")

# UI Inputs (Empty by default for security)
KAFKA_BOOTSTRAP_INPUT = st.sidebar.text_input("Bootstrap Server", value="")
KAFKA_KEY_INPUT = st.sidebar.text_input("API Key", value="", type="password")
KAFKA_SECRET_INPUT = st.sidebar.text_input("API Secret", value="", type="password")
TOPIC = "ad_pipeline_status"

# Logic: Use User Input -> Fallback to Secrets
if KAFKA_BOOTSTRAP_INPUT:
    KAFKA_BOOTSTRAP = KAFKA_BOOTSTRAP_INPUT
    KAFKA_KEY = KAFKA_KEY_INPUT
    KAFKA_SECRET = KAFKA_SECRET_INPUT
else:
    # Fallback to secrets (Hidden)
    from utils import load_config
    config = load_config()
    
    if "kafka" in config:
        st.sidebar.success("✅ Using credentials from Secrets")
        KAFKA_BOOTSTRAP = config["kafka"]["bootstrap_servers"]
        KAFKA_KEY = config["kafka"]["sasl_username"]
        KAFKA_SECRET = config["kafka"]["sasl_password"]
    else:
        KAFKA_BOOTSTRAP = ""
        KAFKA_KEY = ""
        KAFKA_SECRET = ""

# --- PAGE 1: UPLOAD & ETL ---
if page == "Upload & ETL":
    st.title("📂 Upload & ETL Pipeline")
    st.markdown("Upload raw ad data and trigger the processing pipeline manually.")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("1. Upload Raw CSV")
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'])
        
        if uploaded_file is not None:
            # Save file functionality
            save_path = os.path.join("data", "raw", uploaded_file.name)
            os.makedirs(os.path.join("data", "raw"), exist_ok=True)
            
            with open(save_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.success(f"Saved {uploaded_file.name} to Staging Area (data/raw)")
            
            # Preview
            st.markdown("### Preview")
            df_preview = pd.read_csv(save_path)
            st.dataframe(df_preview.head(), use_container_width=True)

    with col2:
        st.subheader("2. Run Processing")
        
        # ELT Pipeline
        st.markdown("**Step A: ETL Pipeline** (DB Update)")
        if st.button("🚀 Run ELT Pipeline", type="primary"):
            status_placeholder = st.empty()
            progress_bar = st.progress(0)
            
            try:
                # Step 1: Ingestion
                status_placeholder.info("Running Ingestion...")
                subprocess.run([sys.executable, "ingestion.py"], check=True)
                progress_bar.progress(33)
                
                # Step 2: Standardization
                status_placeholder.info("Running Standardization (Clean & Quarantine)...")
                subprocess.run([sys.executable, "standardization.py"], check=True)
                progress_bar.progress(66)
                
                # Step 3: Modeling
                status_placeholder.info("Running ETL Modeling (Star Schema)...")
                subprocess.run([sys.executable, "etl_modeling.py"], check=True)
                progress_bar.progress(100)
                
                status_placeholder.success("✅ Database Updated Successfully!")
                time.sleep(1)
                status_placeholder.empty()
                
            except subprocess.CalledProcessError as e:
                status_placeholder.error(f"Pipeline Failed! Error: {e}")

        st.markdown("---")

        # Kafka Producer
        st.markdown("**Step B: Streaming** (Push to Kafka)")
    # --- 3. Verify Data ---
    st.markdown("---")
    st.subheader("3. Verify Data (Database Preview)")
    st.markdown("Check if the **Star Schema** was populated correctly.")

    if st.button("🔍 Preview Star Schema Tables"):
        from sqlalchemy import create_engine, text
        
        # UI Credentials (or use hardcoded for demo)
        # Database Credentials from Secrets
        try:
            from utils import load_config
            config = load_config()
            if "database" in config:
                db_conf = config['database']
                DB_USER = db_conf['user']
                DB_PASS = db_conf['password']
                DB_HOST = db_conf['host']
                DB_PORT = db_conf['port']
                DB_NAME = db_conf['dbname']
            else:
                st.error("❌ Database secrets not found in secrets.toml!")
                st.stop()
        except Exception as e:
             st.error(f"Error loading secrets: {e}")
             st.stop()
            
        DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        try:
            engine = create_engine(DATABASE_URI)
            
            c1, c2 = st.columns(2)
            
            with c1:
                st.markdown("**Fact Table** (`fact_ad_performance`)")
                df_fact = pd.read_sql("SELECT * FROM fact_ad_performance ORDER BY ingested_at DESC LIMIT 5", engine)
                st.dataframe(df_fact, use_container_width=True)
                
            with c2:
                st.markdown("**Dimension** (`dim_platform`)")
                df_dim = pd.read_sql("SELECT * FROM dim_platform LIMIT 5", engine)
                st.dataframe(df_dim, use_container_width=True)
                
            st.success("✅ Connected to Database & Verified Data!")
            
        except Exception as e:
            st.error(f"Database Connection Failed: {e}")

# --- PAGE 2: REAL-TIME MONITOR ---
elif page == "Real-Time Monitor":
    st.title("📊 Real-Time Ad Pipeline Monitor")
    
    # State
    if 'data' not in st.session_state:
        st.session_state.data = []

    consumer = None

    # Connect to Kafka (Helper)
    def get_consumer(bootstrap, key, secret):
        try:
            conf = {
                'bootstrap.servers': bootstrap,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': key,
                'sasl.password': secret,
                'group.id': 'streamlit-dashboard-group-v1',
                'auto.offset.reset': 'earliest'
            }
            c = Consumer(conf)
            c.subscribe([TOPIC])
            return c
        except Exception:
            return None

    # Layout
    placeholder_metrics = st.empty()
    placeholder_charts = st.empty()
    st.markdown("### Latest Events")
    placeholder_table = st.empty()
    
    start_btn = st.sidebar.button("Start Streaming")
    
    if start_btn:
        if not KAFKA_BOOTSTRAP or not KAFKA_KEY or not KAFKA_SECRET:
             st.error("Please configure Kafka credentials in the sidebar.")
        else:
            consumer = get_consumer(KAFKA_BOOTSTRAP, KAFKA_KEY, KAFKA_SECRET)
            st.sidebar.success("🟢 Connected to Kafka")
            
            total_events = 0
            valid_count = 0
            quarantine_count = 0
            
            while True:
                msg = consumer.poll(0.1)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        st.error(msg.error())
                        break
                        
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    st.session_state.data.append(data)
                    
                    # Stats
                    status = data.get('status', 'UNKNOWN')
                    if status in ['QUARANTINED', 'QUAR', 'QRN', 'ERROR']:
                        quarantine_count += 1
                    else:
                        valid_count += 1
                    total_events += 1
                    
                    # Update Metrics
                    with placeholder_metrics.container():
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Total Events", total_events)
                        c2.metric("✅ Validated", valid_count)
                        c3.metric("🚨 Quarantined", quarantine_count)
                        
                    # Update Table & Charts (Batch update)
                    if total_events % 5 == 0:
                        df = pd.DataFrame(st.session_state.data[-20:])
                        if not df.empty:
                            placeholder_table.dataframe(
                                df[['timestamp', 'source_platform', 'status', 'ad_id']], 
                                use_container_width=True
                            )
                            
                            chart_data = pd.DataFrame({
                                'Status': ['Valid', 'Quarantined'],
                                'Count': [valid_count, quarantine_count]
                            })
                            placeholder_charts.bar_chart(chart_data.set_index('Status'))
                            
                except Exception as e:
                    pass

import streamlit as st
import pandas as pd
import json
import time
import os
import subprocess
import sys
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer, KafkaError
import uuid

import ingestion
import standardization
import etl_modeling

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
    from utils import load_config
    config = load_config()
    
    if not config:
        st.sidebar.error("❌ No Secrets Found!")
        st.sidebar.info("Please add your secrets in Streamlit Cloud Settings > Secrets.")
        st.sidebar.code("""[database]
user = "postgres"
password = "..."
host = "..."
port = "5432"
dbname = "postgres"

[kafka]
bootstrap_servers = "..."
sasl_username = "..."
sasl_password = "..."
""", language="toml")
    
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
            st.dataframe(df_preview.head(), width='stretch' if True else 'content') # Updated deprecation

    with col2:
        st.subheader("2. Run Processing")
        
        # ELT Pipeline
        st.markdown("**Step A: ETL Pipeline** (DB Update)")
        if st.button("🚀 Run ELT Pipeline", type="primary"):
            status_placeholder = st.empty()
            progress_bar = st.progress(0)
            
            try:
                # Step 0: Imports (Lazy import + Reload to ensure latest changes)
                import importlib
                import run_elt
                import kafka_producer
                importlib.reload(run_elt)
                importlib.reload(kafka_producer)
                
                print("--- STARTING PIPELINE FROM STREAMLIT ---")
                
                # Step 1: Run Full ELT (SQL-Based)
                status_placeholder.info("Running SQL-Based ELT Pipeline (Ingest -> Transform -> Model)...")
                # Redirect stdout to capture logs if needed, or just run
                run_elt.run_elt()
                progress_bar.progress(50)
                status_placeholder.success("✅ Database Updated Successfully!")
                
                # Step 2: Trigger Kafka Producer
                status_placeholder.info("Streaming updates to Kafka...")
                kafka_producer.produce_data()
                progress_bar.progress(100)
                
                status_placeholder.success("✅ ELT & Streaming Complete!")
                time.sleep(2)
                status_placeholder.empty()
                
            except Exception as e:
                status_placeholder.error(f"Pipeline Failed! Error: {e}")
                print(e)

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
                st.markdown("**Fact Table View** (`fact_denorm`)")
                # Use the view to get friendly names + device_type
                df_fact = pd.read_sql("SELECT ad_id, platform, timestamp_utc, spend, device_type, pipeline_status FROM fact_denorm LIMIT 10", engine)
                st.dataframe(df_fact, width='stretch')
                
            with c2:
                st.markdown("**Dimension** (`dim_platform`)")
                df_dim = pd.read_sql("SELECT * FROM dim_platform LIMIT 5", engine)
                st.dataframe(df_dim, width='stretch')
                
            st.success("✅ Connected to Database & Verified Data!")
            
        except Exception as e:
            st.error(f"Database Connection Failed: {e}")

    st.markdown("---")
    st.subheader("4. Analysis: Video Views by Platform & Device")
    if st.button("📊 Run Aggregation Query"):
         try:
            # Re-use config logic if possible or just try-catch separate
            # (Assuming engine relates to same block or re-init if needed, 
            #  but streamlit reruns script on interaction so we need to ensure engine is available)
            #  We'll just re-load config safely
            from utils import load_config
            from sqlalchemy import create_engine
            config = load_config()
            db_conf = config['database']
            db_uri = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"
            engine = create_engine(db_uri)

            query = """
            SELECT platform, device_type, SUM(video_views) as total_views 
            FROM fact_denorm 
            GROUP BY platform, device_type 
            ORDER BY platform, total_views DESC
            """
            df_analysis = pd.read_sql(query, engine)
            st.dataframe(df_analysis, width='stretch')
            st.bar_chart(df_analysis, x="platform", y="total_views", color="device_type")
            
         except Exception as e:
            st.error(f"Analysis Failed: {e}")

# --- PAGE 2: REAL-TIME MONITOR ---
elif page == "Real-Time Monitor":
    st.title("📊 Real-Time Ad Pipeline Monitor")
    
    # State
    if 'data' not in st.session_state:
        st.session_state.data = []

    consumer = None

    # Connect to Kafka (Helper)
    def get_consumer(bootstrap, key, secret):
        if 'consumer_group_id' not in st.session_state:
            st.session_state.consumer_group_id = f"streamlit-monitor-{uuid.uuid4().hex[:8]}"
            
        try:
            conf = {
                'bootstrap.servers': bootstrap,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': key,
                'sasl.password': secret,
                'group.id': st.session_state.consumer_group_id, # Unique group for fresh view
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
            st.write(f"Debug: Subscribed to {TOPIC} with group {st.session_state.consumer_group_id}")
            
            total_events = 0
            valid_count = 0
            quarantine_count = 0
            
            # Debug container
            debug_container = st.empty()
            
            while True:
                msg = consumer.poll(0.5) # Increased poll time slightly
                
                if msg is None:
                    debug_container.text("Polling... No message yet.")
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                         debug_container.text("Reached end of partition.")
                         continue
                    else:
                        st.error(msg.error())
                        break
                        
                debug_container.text(f"Message Received: {msg.value()[:50]}...") # Show first 50 chars
                        
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                        
                        # Flatten device_type from full_record if available/needed, 
                        # or just rely on what's in the top level if we updated producer (producer puts it in full_record)
                        # Pipeline msg: { ..., "full_record": { ..., "device_type": "mobile" } }
                        full_rec = data.get("full_record", {})
                        if isinstance(full_rec, dict):
                            data['device_type'] = full_rec.get('device_type', 'unknown')
                            # Extract video views (ensure it's an int)
                            try:
                                data['video_views'] = int(full_rec.get('video_views', 0))
                            except:
                                data['video_views'] = 0
                        else:
                            data['device_type'] = 'unknown'
                            data['video_views'] = 0

                        st.session_state.data.append(data)
                        
                        # Stats
                        status = data.get('status', 'UNKNOWN')
                        if status in ['QUARANTINED', 'QUAR', 'QRN', 'ERROR']:
                            quarantine_count += 1
                        else:
                            valid_count += 1
                        total_events += 1
                    
                        # Calculate Percentages
                        valid_pct = (valid_count / total_events) * 100 if total_events > 0 else 0
                        quar_pct = (quarantine_count / total_events) * 100 if total_events > 0 else 0
                        
                        # Update Metrics
                        with placeholder_metrics.container():
                            c1, c2, c3, c4 = st.columns(4)
                            c1.metric("Total Events", total_events)
                            c2.metric("✅ Validated", f"{valid_count} ({valid_pct:.1f}%)")
                            c3.metric("🚨 Quarantined", f"{quarantine_count} ({quar_pct:.1f}%)")
                            
                            # Total Views Metric
                            total_views = sum(d.get('video_views', 0) for d in st.session_state.data)
                            c4.metric("👀 Total Video Views", f"{total_views:,}")
                            
                        # Update Table & Charts (Batch update)
                        if total_events % 2 == 0: # More frequent updates
                            df = pd.DataFrame(st.session_state.data[-20:])
                            if not df.empty:
                                placeholder_table.dataframe(
                                    df[['timestamp', 'source_platform', 'device_type', 'video_views', 'status', 'ad_id']], 
                                    width='stretch'
                                )
                                
                                # Charts
                                chart1, chart2 = st.columns(2)
                                with chart1:
                                    st.markdown("##### Device Type Breakdown")
                                    if 'device_type' in df.columns:
                                        device_counts = df['device_type'].value_counts().reset_index()
                                        device_counts.columns = ['Device', 'Count']
                                        st.bar_chart(device_counts.set_index('Device'))
                                
                                with chart2:
                                    st.markdown("##### Video Views by Platform")
                                    if 'video_views' in df.columns and 'source_platform' in df.columns:
                                        # Group by platform and sum views
                                        views_by_plat = df.groupby('source_platform')['video_views'].sum().reset_index()
                                        views_by_plat.columns = ['Platform', 'Views']
                                        st.bar_chart(views_by_plat.set_index('Platform'))

                    except Exception as e:
                        pass

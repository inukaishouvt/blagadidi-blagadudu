import streamlit as st
import pandas as pd
import json
import time
from confluent_kafka import Consumer, KafkaError

# Page Config
st.set_page_config(page_title="Ad Pipeline Dashboard", layout="wide")
st.title("🚀 Real-Time Ad Pipeline Monitor")

# Sidebar - Config
st.sidebar.header("Kafka Configuration")

# Try to load from secrets, otherwise use defaults/empty
default_bootstrap = st.secrets["kafka"]["bootstrap_servers"] if "kafka" in st.secrets else ""
default_key = st.secrets["kafka"]["sasl_username"] if "kafka" in st.secrets else ""
default_secret = st.secrets["kafka"]["sasl_password"] if "kafka" in st.secrets else ""

KAFKA_BOOTSTRAP = st.sidebar.text_input("Bootstrap Server", value=default_bootstrap)
KAFKA_KEY = st.sidebar.text_input("API Key", value=default_key, type="password")
KAFKA_SECRET = st.sidebar.text_input("API Secret", value=default_secret, type="password")
TOPIC = "ad_pipeline_status"

# Connect to Kafka
@st.cache_resource
def get_consumer(bootstrap, key, secret):
    conf = {
        'bootstrap.servers': bootstrap,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': key,
        'sasl.password': secret,
        'group.id': 'streamlit-dashboard-group-v1',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    return consumer

consumer = get_consumer(KAFKA_BOOTSTRAP, KAFKA_KEY, KAFKA_SECRET)

# Layout
col1, col2, col3 = st.columns(3)
placeholder_metrics = st.empty()
placeholder_charts = st.empty()
placeholder_table = st.empty()

# State
if 'data' not in st.session_state:
    st.session_state.data = []

# run button
start_btn = st.sidebar.button("Start Streaming")

if start_btn:
    st.sidebar.success("Listening to Kafka...")
    
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
                
        # Parse Message
        try:
            data = json.loads(msg.value().decode('utf-8'))
            st.session_state.data.append(data)
            
            # Simple Stats logic
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
                
            # Update Table (Top 10 Latest)
            if total_events % 10 == 0: # Update UI every 10 events to save render time
                df = pd.DataFrame(st.session_state.data[-20:]) # Keep last 20 in view
                placeholder_table.dataframe(df[['timestamp', 'source_platform', 'status', 'ad_id']])
                
                # Chart
                if not df.empty:
                    chart_data = pd.DataFrame({
                        'Status': ['Valid', 'Quarantined'],
                        'Count': [valid_count, quarantine_count]
                    })
                    placeholder_charts.bar_chart(chart_data.set_index('Status'))

        except Exception as e:
            st.error(f"Error parsing json: {e}")
            
        # Stop button? Streamlit loops are tricky, usually just stop script

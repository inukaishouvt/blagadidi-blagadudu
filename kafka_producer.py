import json
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine
import glob

from utils import load_config

# Load common config
config = load_config()

# Database Credentials
db_conf = config['database']
DB_USER = db_conf['user']
DB_PASS = db_conf['password']
DB_HOST = db_conf['host']
DB_PORT = db_conf['port']
DB_NAME = db_conf['dbname']
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Kafka Config
kafka_conf = config['kafka']
KAFKA_CONF = {
    'bootstrap.servers': kafka_conf['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_conf['sasl_username'], 
    'sasl.password': kafka_conf['sasl_password']
}

TOPIC_PIPELINE = 'ad_pipeline_status'
TOPIC_LIFECYCLE = 'ad_lifecycle_events'

def create_topics():
    """ Creates topics if they do not exist. """
    admin_client = AdminClient(KAFKA_CONF)
    
    # Check what topics exist
    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
        existing_topics = cluster_metadata.topics
        
        topics_to_create = []
        if TOPIC_PIPELINE not in existing_topics:
            print(f"Topic {TOPIC_PIPELINE} missing. Scheduling creation...")
            topics_to_create.append(NewTopic(TOPIC_PIPELINE, num_partitions=3, replication_factor=3))
            
        if TOPIC_LIFECYCLE not in existing_topics:
            print(f"Topic {TOPIC_LIFECYCLE} missing. Scheduling creation...")
            topics_to_create.append(NewTopic(TOPIC_LIFECYCLE, num_partitions=3, replication_factor=3))
            
        if topics_to_create:
            futures = admin_client.create_topics(topics_to_create)
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    print(f"Topic {topic} created")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")
        else:
            print("All topics already exist.")
            
    except Exception as e:
        print(f"Failed to administer topics: {e}")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    # else:
    #     print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def get_producer():
    try:
        producer = Producer(KAFKA_CONF)
        print("Kafka Producer initialized (confluent-kafka).")
        return producer
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        return None

def produce_data():
    # step 0: create topics
    # Note: This might fail if keys are invalid placeholders
    # create_topics() 
    
    engine = create_engine(DATABASE_URI)
    
    # Read modeled data (Architecture Requirement: from Fact Final/Denorm)
    print("Reading data from 'fact_denorm'...")
    try:
        df = pd.read_sql("SELECT * FROM fact_denorm", engine)
    except Exception as e:
        print(f"Error reading DB: {e}")
        return

    producer = get_producer()
    if not producer:
        return

    print(f"Producing {len(df)} records to Kafka...")
    
    count = 0
    for _, row in df.iterrows():
        record = row.to_dict()
        
        # Handle datetime serialization (Pandas Timestamp -> str)
        for k, v in record.items():
            if isinstance(v, pd.Timestamp):
                record[k] = v.isoformat()
        
        # 1. Pipeline Status Topic
        pipeline_msg = {
            "ad_id": record.get("ad_id"),
            "timestamp": record.get("timestamp_utc"),
            "status": record.get("pipeline_status"),
            "source_platform": record.get("platform"),
            "full_record": record
        }
        
        producer.produce(
            TOPIC_PIPELINE, 
            key=str(record.get("ad_id")).encode('utf-8'),
            value=json.dumps(pipeline_msg).encode('utf-8'), 
            callback=delivery_report
        )
        
        # 2. Ad Lifecycle Topic
        lifecycle_msg = {
            "ad_id": record.get("ad_id"),
            "timestamp": record.get("timestamp_utc"),
            "lifecycle_status": record.get("ad_lifecycle_status"),
            "device_type": record.get("device_type"),
            "metrics": {
                "impressions": record.get("impressions"),
                "clicks": record.get("clicks"),
                "spend": record.get("spend"),
                "video_views": record.get("video_views")
            }
        }
        producer.produce(
            TOPIC_LIFECYCLE, 
            key=str(record.get("ad_id")).encode('utf-8'),
            value=json.dumps(lifecycle_msg).encode('utf-8'),
            callback=delivery_report
        )
        
        count += 1
        if count % 1000 == 0:
            producer.poll(0)
            print(f"Produced {count} records...")

    print("Flushing records...")
    producer.flush()
    print("Finished producing messages.")

if __name__ == "__main__":
    if "YOUR_API_KEY" in KAFKA_CONF['sasl.username']:
         print("⚠️  PLEASE UPDATE KAFKA CONFIGURATION WITH YOUR KEYS IN THE SCRIPT OR ENV VARS")
    else:
         produce_data()

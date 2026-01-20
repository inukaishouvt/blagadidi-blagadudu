import json
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine

# Database Credentials
DB_USER = "postgres"
DB_PASS = "wCb3Ww51PKCmO2wD"
DB_HOST = "db.yaknidhvchourohrjqfa.supabase.co"
DB_PORT = "5432"
DB_NAME = "postgres"
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Kafka Config
# NOTE: Set these or use a config file. DO NOT commit real keys to GitHub.
KAFKA_CONF = {
    'bootstrap.servers': 'pkc-312o0.ap-southeast-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'YOUR_API_KEY_HERE', 
    'sasl.password': 'YOUR_API_SECRET_HERE'
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
            "metrics": {
                "impressions": record.get("impressions"),
                "clicks": record.get("clicks"),
                "spend": record.get("spend")
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


import json
import pusher
from confluent_kafka import Consumer, KafkaError
from utils import load_config

# Load Config
config = load_config()

# Pusher Config
if 'pusher' not in config:
    print("❌ Pusher config not found in secrets/secrets.toml")
    exit(1)

pusher_conf = config['pusher']
pusher_client = pusher.Pusher(
    app_id=pusher_conf['app_id'],
    key=pusher_conf['key'],
    secret=pusher_conf['secret'],
    cluster=pusher_conf['cluster'],
    ssl=True
)

# Kafka Config
kafka_conf = config['kafka']
KAFKA_CONF = {
    'bootstrap.servers': kafka_conf['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_conf['sasl_username'],
    'sasl.password': kafka_conf['sasl_password'],
    'group.id': 'pusher-broadcaster-group',
    'auto.offset.reset': 'latest' # Start from new messages
}

TOPIC = 'ad_pipeline_status'

def main():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])
    print(f"📡  Listening to Kafka topic '{TOPIC}' and broadcasting to Pusher...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            try:
                # Parse Kafka Message
                data = json.loads(msg.value().decode('utf-8'))
                
                # Broadcast to Pusher
                # Channel: 'pipeline-updates'
                # Event: 'new-status'
                pusher_client.trigger('pipeline-updates', 'new-status', data)
                
                print(f"Broadcasting event for Ad ID: {data.get('ad_id', 'Unknown')}")
                
            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

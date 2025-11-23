import json
from confluent_kafka import Consumer, KafkaError, KafkaException

# 1. Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-analytics-group',  # Consumer group ID
    'auto.offset.reset': 'earliest'    # Start from beginning if no offset is stored
}

# 2. Create Consumer instance
consumer = Consumer(conf)

# 3. Subscribe to topic
topic = 'fumitran'
consumer.subscribe([topic])

print("Starting Consumer... Press Ctrl+C to stop.")

try:
    while True:
        # 4. Poll for new messages (timeout=1.0 second)
        msg = consumer.poll(1.0)

        if msg is None:
            continue # No message found, keep polling
            
        if msg.error():
            # Handle end of partition or actual errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'End of partition reached {msg.topic()}/{msg.partition()}')
            else:
                raise KafkaException(msg.error())
        else:
            # 5. Successfully received a message
            # Decode the bytes back to a string, then JSON
            data = json.loads(msg.value().decode('utf-8'))
            key = msg.key().decode('utf-8') if msg.key() else None
            
            print(f"Received | Key: {key} | Data: {data}")

except KeyboardInterrupt:
    print("Stopping Consumer...")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
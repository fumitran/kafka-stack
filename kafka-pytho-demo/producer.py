import json
import time
import random
from confluent_kafka import Producer

# configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": 'fumitran.client.1'
}

# create producer instance
producer = Producer(conf)

# callback function to check if msg was delivered
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Simulate data generation

topic = "fumitran"

print("Starting Producer...")
try:
    while True:
        # Create a dummy JSON message
        data = {
            'user_id': random.randint(1, 100),
            'action': random.choice(['login', 'logout', 'purchase', 'click']),
            'timestamp': time.time()
        }
        
        # Serialize data to bytes (Kafka requires bytes)
        value = json.dumps(data).encode('utf-8')
        
        # Asynchronously produce a message. 
        # The 'callback' is triggered when the broker acknowledges receipt.
        producer.produce(
            topic=topic, 
            key=str(data['user_id']), # Key ensures order for specific users
            value=value, 
            on_delivery=delivery_report
        )
        
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)
        
        time.sleep(30) # Send one message every second

except KeyboardInterrupt:
    print("Stopping Producer...")
finally:
    # Wait for all messages in the Producer queue to be delivered
    producer.flush()
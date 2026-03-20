# producer.py
from confluent_kafka import Producer
import json, time, uuid

p = Producer({'bootstrap.servers': 'localhost:9092'})

topic = "streaming.user.interactions"

for i in range(2000):
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": i % 100,
        "action": "click",
        "timestamp": time.time()
    }
    p.produce(topic, json.dumps(event))
    p.flush()
    time.sleep(0.01)

print("Done producing")
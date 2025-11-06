from confluent_kafka import Producer
import json, random, uuid
from datetime import datetime

p = Producer({"bootstrap.servers": "kafka:9092"})

EVENT_TYPES = ["view", "add_to_cart", "purchase"]
PRODUCTS = [f"product_{i}" for i in range(1, 501)]
USERS = [f"user_{i}" for i in range(1, 5001)]

def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "event_type": random.choices(EVENT_TYPES, [0.8, 0.15, 0.05])[0],
        "ts": datetime.utcnow().isoformat()
    }

def run_producer(num_events: int = 50_000):
    for _ in range(num_events):
        e = make_event()
        p.produce("events", json.dumps(e).encode("utf-8"))
    p.flush()

if __name__ == "__main__":
    run_producer()

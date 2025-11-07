from confluent_kafka import Producer
import json, random, uuid
from datetime import datetime
from pathlib import Path

EVENT_TYPES = ["view", "add_to_cart", "purchase"]
PRODUCTS = [f"product_{i}" for i in range(1, 501)]
USERS = [f"user_{i}" for i in range(1, 5001)]

DATA_DIR = Path("/opt/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_FILE = DATA_DIR / "events.jsonl"


def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "event_type": random.choices(EVENT_TYPES, [0.8, 0.15, 0.05])[0],
        "ts": datetime.utcnow().isoformat(),
    }


def run_producer(num_events: int = 5_000):
    # Пишемо у файл (гарантовано працює)
    with EVENTS_FILE.open("w", encoding="utf-8") as f:
        for _ in range(num_events):
            e = make_event()
            line = json.dumps(e)
            f.write(line + "\n")

    # Паралельно пробуємо штовхнути в Kafka, але без блокувань
    try:
        p = Producer(
            {
                "bootstrap.servers": "kafka:9092",
                "message.timeout.ms": 5000,  # щоб не висіти вічно
            }
        )
        with EVENTS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    p.produce("events", line.encode("utf-8"))
                except Exception as ex:
                    print("Kafka produce error, stop sending to kafka:", ex)
                    break
        p.flush(5)
    except Exception as ex:
        print("Kafka not available, continue without it:", ex)


if __name__ == "__main__":
    run_producer()

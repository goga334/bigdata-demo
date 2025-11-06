from confluent_kafka import Consumer
import polars as pl
import json
from pathlib import Path

DATA_DIR = Path("/opt/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_PATH = DATA_DIR / "bronze_events.parquet"

def run_consumer(max_messages: int = 50_000, batch_size: int = 10_000):
    c = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "bronze-writer",
        "auto.offset.reset": "earliest",
    })
    c.subscribe(["events"])

    batch = []
    read = 0

    while read < max_messages:
        msg = c.poll(1.0)
        if msg is None:
            break
        if msg.error():
            print("Error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        batch.append(event)
        read += 1

        if len(batch) >= batch_size:
            df = pl.DataFrame(batch)
            if BRONZE_PATH.exists():
                # append
                old = pl.read_parquet(str(BRONZE_PATH))
                df = pl.concat([old, df], rechunk=True)
            df.write_parquet(str(BRONZE_PATH))
            batch.clear()

    if batch:
        df = pl.DataFrame(batch)
        if BRONZE_PATH.exists():
            old = pl.read_parquet(str(BRONZE_PATH))
            df = pl.concat([old, df], rechunk=True)
        df.write_parquet(str(BRONZE_PATH))

    c.close()

if __name__ == "__main__":
    run_consumer()

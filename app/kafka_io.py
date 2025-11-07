import json, time
from confluent_kafka import Producer, Consumer, KafkaException

TOPIC = "shop.events"

def _common_conf(bootstrap):
    return {"bootstrap.servers": bootstrap}

def produce_events(bootstrap: str, n: int = 500):
    p = Producer(_common_conf(bootstrap))
    for i in range(n):
        evt = {"id": i, "ts": time.time(), "item": f"sku-{i%37}", "qty": 1 + (i % 5), "price": round(10 + i*0.1, 2)}
        p.produce(TOPIC, json.dumps(evt).encode("utf-8"))
    p.flush(30)

def consume_to_bronze(bootstrap: str, out_path: str, max_msgs: int = 10000, timeout_s: int = 10):
    from pathlib import Path
    import polars as pl
    c = Consumer({
        **_common_conf(bootstrap),
        "group.id": "shop-pipeline",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    rows = []
    seen = 0
    start = time.time()
    try:
        while seen < max_msgs and time.time() - start < timeout_s:
            msg = c.poll(0.2)
            if msg is None: 
                continue
            if msg.error():
                raise KafkaException(msg.error())
            rows.append(json.loads(msg.value().decode("utf-8")))
            seen += 1
    finally:
        c.close()

    if not rows:
        raise RuntimeError("Kafka returned no messages for bronze step")

    df = pl.DataFrame(rows)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)

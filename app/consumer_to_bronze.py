import polars as pl
import json
from pathlib import Path

DATA_DIR = Path("/opt/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_FILE = DATA_DIR / "events.jsonl"
BRONZE_PATH = DATA_DIR / "bronze_events.parquet"


def run_consumer():
    if not EVENTS_FILE.exists():
        print("No events file, nothing to consume")
        return

    rows = []
    with EVENTS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    if not rows:
        print("No events in file")
        return

    df = pl.DataFrame(rows)
    df.write_parquet(str(BRONZE_PATH))
    print(f"Wrote {len(rows)} events to {BRONZE_PATH}")


if __name__ == "__main__":
    run_consumer()

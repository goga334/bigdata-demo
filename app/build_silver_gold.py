from pathlib import Path
import polars as pl

DATA_DIR = Path("/opt/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BRONZE_PATH = DATA_DIR / "bronze_events.parquet"
SILVER_PATH = DATA_DIR / "silver_purchases.parquet"
GOLD_PATH = DATA_DIR / "gold_top10.parquet"


def build_silver():
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(f"Bronze data not found at {BRONZE_PATH}")

    df = pl.read_parquet(str(BRONZE_PATH))

    print("Bronze columns:", df.columns)

    if "event_type" not in df.columns:
        raise ValueError("Column 'event_type' not found in bronze dataset")

    # беремо тільки purchase-ів
    purchases = df.filter(pl.col("event_type") == "purchase")

    if purchases.is_empty():
        print("No purchase events found, writing empty silver table")
        purchases = pl.DataFrame(
            {"user_id": [], "product_id": [], "purchases": []}
        )
    else:
        purchases = (
            purchases
            .group_by(["user_id", "product_id"])
            .agg(purchases=pl.len())
        )

    purchases.write_parquet(str(SILVER_PATH))
    print(f"Wrote silver layer to {SILVER_PATH} with {purchases.height} rows.")


def build_gold():
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Silver data not found at {SILVER_PATH}")

    df = pl.read_parquet(str(SILVER_PATH))

    if df.is_empty():
        print("Silver is empty, writing empty gold")
        df.write_parquet(str(GOLD_PATH))
        return

    gold = (
        df.group_by("product_id")
        .agg(total_purchases=pl.col("purchases").sum())
        .sort("total_purchases", descending=True)
        .head(10)
    )

    gold.write_parquet(str(GOLD_PATH))
    print("Gold top-10:")
    print(gold)

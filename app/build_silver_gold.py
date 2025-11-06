import polars as pl
from pathlib import Path

DATA_DIR = Path("/opt/app/data")
BRONZE = DATA_DIR / "bronze_events.parquet"
SILVER = DATA_DIR / "silver_purchases.parquet"
GOLD_TOP = DATA_DIR / "gold_top10.parquet"

def build_silver():
    df = pl.read_parquet(str(BRONZE))

    silver = (
        df
        .filter(pl.col("event_type") == "purchase")
        .groupby("product_id")
        .agg(pl.count().alias("purchases"))
    )

    silver.write_parquet(str(SILVER))

def build_gold():
    silver = pl.read_parquet(str(SILVER))
    top10 = silver.sort("purchases", descending=True).head(10)
    top10.write_parquet(str(GOLD_TOP))
    print(top10)

if __name__ == "__main__":
    build_silver()
    build_gold()

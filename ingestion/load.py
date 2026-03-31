"""
Ingestion script — loads Olist CSV files into PostgreSQL raw schema.
Usage: python ingestion/load.py --data-dir ./data
"""
import os
import argparse
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Map: CSV filename stem → raw table name
TABLES = {
    "olist_orders_dataset":                    "orders",
    "olist_order_items_dataset":               "order_items",
    "olist_order_payments_dataset":            "order_payments",
    "olist_order_reviews_dataset":             "order_reviews",
    "olist_customers_dataset":                 "customers",
    "olist_sellers_dataset":                   "sellers",
    "olist_products_dataset":                  "products",
    "product_category_name_translation":       "product_category_translation",
    "olist_geolocation_dataset":               "geolocation",
}

COLUMN_RENAMES = {
    "orders": {
        "order_purchase_timestamp":        "order_purchase_ts",
        "order_approved_at":               "order_approved_ts",
        "order_delivered_carrier_date":    "order_delivered_carrier_ts",
        "order_delivered_customer_date":   "order_delivered_customer_ts",
        "order_estimated_delivery_date":   "order_estimated_delivery_ts",
    },
    "order_reviews": {
        "review_answer_timestamp": "review_answer_ts",
    },
}


def get_engine() -> object:
    user     = os.getenv("POSTGRES_USER",     "analytics")
    password = os.getenv("POSTGRES_PASSWORD", "analytics")
    host     = os.getenv("POSTGRES_HOST",     "localhost")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "ecommerce")
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load_table(engine, csv_path: Path, table: str) -> None:
    log.info(f"Loading {csv_path.name} → raw.{table}")
    df = pd.read_csv(csv_path, low_memory=False)

    if table in COLUMN_RENAMES:
        df.rename(columns=COLUMN_RENAMES[table], inplace=True)

    df.columns = [c.lower() for c in df.columns]

    df.to_sql(
        table,
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=10_000,
        method="multi",
    )
    log.info(f"  {len(df):,} rows loaded into raw.{table}")


def main(data_dir: str) -> None:
    data_path = Path(data_dir)
    engine    = get_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()

    loaded, skipped = 0, 0
    for stem, table in TABLES.items():
        candidates = list(data_path.glob(f"{stem}*.csv"))
        if not candidates:
            log.warning(f"  File not found: {stem}.csv — skipping")
            skipped += 1
            continue
        load_table(engine, candidates[0], table)
        loaded += 1

    log.info(f"\nDone. {loaded} tables loaded, {skipped} skipped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Olist CSVs into PostgreSQL")
    parser.add_argument("--data-dir", default="./data", help="Directory with Olist CSV files")
    args = parser.parse_args()
    main(args.data_dir)

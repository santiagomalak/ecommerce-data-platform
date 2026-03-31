-- Raw schema: one table per Olist CSV
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id               TEXT PRIMARY KEY,
    customer_id            TEXT,
    order_status           TEXT,
    order_purchase_ts      TIMESTAMP,
    order_approved_ts      TIMESTAMP,
    order_delivered_carrier_ts  TIMESTAMP,
    order_delivered_customer_ts TIMESTAMP,
    order_estimated_delivery_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id               TEXT,
    order_item_id          INTEGER,
    product_id             TEXT,
    seller_id              TEXT,
    shipping_limit_date    TIMESTAMP,
    price                  NUMERIC(10,2),
    freight_value          NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id               TEXT,
    payment_sequential     INTEGER,
    payment_type           TEXT,
    payment_installments   INTEGER,
    payment_value          NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id              TEXT,
    order_id               TEXT,
    review_score           INTEGER,
    review_comment_title   TEXT,
    review_comment_message TEXT,
    review_creation_date   TIMESTAMP,
    review_answer_ts       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id            TEXT PRIMARY KEY,
    customer_unique_id     TEXT,
    customer_zip_code      TEXT,
    customer_city          TEXT,
    customer_state         TEXT
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id              TEXT PRIMARY KEY,
    seller_zip_code        TEXT,
    seller_city            TEXT,
    seller_state           TEXT
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id             TEXT PRIMARY KEY,
    product_category_name  TEXT,
    product_name_length    INTEGER,
    product_description_length INTEGER,
    product_photos_qty     INTEGER,
    product_weight_g       INTEGER,
    product_length_cm      INTEGER,
    product_height_cm      INTEGER,
    product_width_cm       INTEGER
);

CREATE TABLE IF NOT EXISTS raw.product_category_translation (
    product_category_name             TEXT PRIMARY KEY,
    product_category_name_english     TEXT
);

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code   TEXT,
    geolocation_lat        NUMERIC(10,6),
    geolocation_lng        NUMERIC(10,6),
    geolocation_city       TEXT,
    geolocation_state      TEXT
);

-- ============================================================================
-- snowflake_setup.sql — Full Snowflake setup for the E-Commerce Data Lakehouse
--
-- This file is executed by GitHub Actions CI/CD pipeline
-- AWS credentials are sourced from GitHub Secrets (not hardcoded here)
-- ============================================================================

-- ── Warehouse + Database ─────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS CAPSTONE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND   = 60
         AUTO_RESUME    = TRUE
         INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
USE DATABASE ECOMMERCE_DW;
USE SCHEMA PUBLIC;
USE WAREHOUSE CAPSTONE_WH;


-- ── Dimension tables (star schema) ───────────────────────────────────────────

CREATE OR REPLACE TABLE dim_customers (
    customer_id               VARCHAR,
    customer_unique_id        VARCHAR,
    customer_zip_code_prefix  VARCHAR,
    customer_city             VARCHAR,
    customer_state            VARCHAR
);

CREATE OR REPLACE TABLE dim_sellers (
    seller_id                VARCHAR,
    seller_zip_code_prefix   VARCHAR,
    seller_city              VARCHAR,
    seller_state             VARCHAR,
    seller_lat               FLOAT,
    seller_lng               FLOAT
);

CREATE OR REPLACE TABLE dim_products (
    product_id                       VARCHAR,
    product_category_name            VARCHAR,
    product_category_name_english    VARCHAR,
    product_name_length              INTEGER,
    product_description_length       INTEGER,
    product_photos_qty               INTEGER,
    product_weight_g                 FLOAT,
    product_length_cm                FLOAT,
    product_height_cm                FLOAT,
    product_width_cm                 FLOAT
);

CREATE OR REPLACE TABLE dim_geolocation (
    geolocation_zip_code_prefix  VARCHAR,
    geolocation_lat              FLOAT,
    geolocation_lng              FLOAT,
    geolocation_city             VARCHAR,
    geolocation_state            VARCHAR
);


-- ── Fact table ───────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE fact_order_items (
    order_id                     VARCHAR,
    order_item_id                INTEGER,
    customer_id                  VARCHAR,
    product_id                   VARCHAR,
    seller_id                    VARCHAR,
    price                        FLOAT,
    freight_value                FLOAT,
    total_amount                 FLOAT,
    payment_value                FLOAT,
    payment_type                 VARCHAR,
    avg_installments             FLOAT,
    order_status                 VARCHAR,
    purchase_timestamp           TIMESTAMP,
    delivered_date               TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    review_score                 FLOAT,
    delivery_days                INTEGER,
    delay_flag                   INTEGER,
    on_time_flag                 INTEGER
);


-- ── Business aggregate tables ────────────────────────────────────────────────

CREATE OR REPLACE TABLE order_summary (
    order_id        VARCHAR,
    total_items     INTEGER,
    total_price     FLOAT,
    total_freight   FLOAT,
    total_amount    FLOAT,
    order_status    VARCHAR,
    purchase_date   TIMESTAMP,
    delivered_date  TIMESTAMP,
    delivery_days   INTEGER
);

CREATE OR REPLACE TABLE customer_metrics (
    customer_id       VARCHAR,
    total_orders      INTEGER,
    total_spent       FLOAT,
    avg_order_value   FLOAT,
    last_order_date   TIMESTAMP,
    avg_review_score  FLOAT
);

CREATE OR REPLACE TABLE seller_performance (
    seller_id              VARCHAR,
    total_orders           INTEGER,
    total_revenue          FLOAT,
    avg_review_score       FLOAT,
    on_time_delivery_rate  FLOAT,
    avg_delivery_days      FLOAT
);

CREATE OR REPLACE TABLE product_performance (
    product_id        VARCHAR,
    total_quantity    INTEGER,
    total_sales       FLOAT,
    avg_price         FLOAT,
    avg_review_score  FLOAT
);

-- Verify all 9 tables were created
SHOW TABLES;
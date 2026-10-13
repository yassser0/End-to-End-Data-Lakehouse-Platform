-- =============================================================================
-- Database initialisation script
-- Run once to set up the PostgreSQL schema for the data lakehouse platform.
-- Usage: psql -U postgres -f sql/init.sql
-- =============================================================================

-- Create application database and user (run as superuser if needed)
-- CREATE DATABASE lakehouse;
-- CREATE USER lakehouse_user WITH ENCRYPTED PASSWORD 'change_me';
-- GRANT ALL PRIVILEGES ON DATABASE lakehouse TO lakehouse_user;

-- Connect to the lakehouse database before running the rest:
-- \c lakehouse

-- =============================================================================
-- Gold layer — Analytical tables
-- These are fully replaced on each pipeline run (TRUNCATE + INSERT).
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold_daily_sales (
    order_date          DATE        NOT NULL,
    total_revenue       NUMERIC(14,2),
    paid_order_count    INTEGER,
    avg_order_value     NUMERIC(14,2),
    CONSTRAINT pk_daily_sales PRIMARY KEY (order_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON gold_daily_sales (order_date DESC);

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold_product_performance (
    product_id          VARCHAR(20) NOT NULL,
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    total_quantity      BIGINT,
    total_revenue       NUMERIC(14,2),
    order_count         BIGINT,
    CONSTRAINT pk_product_performance PRIMARY KEY (product_id)
);

CREATE INDEX IF NOT EXISTS idx_product_revenue
    ON gold_product_performance (total_revenue DESC);

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold_customer_analytics (
    customer_id         VARCHAR(20)  NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    segment             VARCHAR(50),
    total_orders        BIGINT,
    paid_orders         BIGINT,
    total_spent         NUMERIC(14,2),
    avg_order_value     NUMERIC(14,2),
    CONSTRAINT pk_customer_analytics PRIMARY KEY (customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_spent
    ON gold_customer_analytics (total_spent DESC);

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold_order_status_summary (
    order_status        VARCHAR(50)  NOT NULL,
    order_count         BIGINT,
    total_amount        NUMERIC(14,2),
    CONSTRAINT pk_order_status PRIMARY KEY (order_status)
);

-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold_data_quality_metrics (
    batch_id            VARCHAR(50)  NOT NULL,
    entity              VARCHAR(50)  NOT NULL,
    ingested_count      INTEGER,
    valid_count         INTEGER,
    rejected_count      INTEGER,
    quality_pct         NUMERIC(5,2),
    run_timestamp       TIMESTAMPTZ,
    CONSTRAINT pk_dq_metrics PRIMARY KEY (batch_id, entity)
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_batch
    ON gold_data_quality_metrics (batch_id, run_timestamp DESC);

-- =============================================================================
-- Airflow metadata database (separate from lakehouse)
-- Airflow manages its own schema via alembic migrations.
-- Reference only — not executed here.
-- =============================================================================
-- CREATE DATABASE airflow_db;
-- GRANT ALL PRIVILEGES ON DATABASE airflow_db TO lakehouse_user;

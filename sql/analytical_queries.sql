-- =============================================================================
-- Analytical SQL queries for the e-commerce Data Lakehouse
-- Compatible with the Gold layer schema defined in init.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. Daily revenue (last 30 days)
-- -----------------------------------------------------------------------------
SELECT
    order_date,
    total_revenue,
    paid_order_count,
    avg_order_value
FROM gold_daily_sales
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY order_date DESC;

-- -----------------------------------------------------------------------------
-- Q2. Monthly revenue summary
-- -----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', order_date)          AS month,
    SUM(total_revenue)                       AS monthly_revenue,
    SUM(paid_order_count)                    AS monthly_paid_orders,
    ROUND(SUM(total_revenue) /
          NULLIF(SUM(paid_order_count), 0), 2) AS monthly_aov
FROM gold_daily_sales
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;

-- -----------------------------------------------------------------------------
-- Q3. Top 10 revenue-generating products
-- -----------------------------------------------------------------------------
SELECT
    product_id,
    product_name,
    category,
    total_quantity,
    total_revenue,
    order_count,
    ROUND(total_revenue / NULLIF(order_count, 0), 2) AS avg_revenue_per_order
FROM gold_product_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Q4. Revenue by product category
-- -----------------------------------------------------------------------------
SELECT
    COALESCE(category, 'Unknown')  AS category,
    SUM(total_revenue)             AS category_revenue,
    SUM(total_quantity)            AS units_sold,
    COUNT(*)                       AS product_count
FROM gold_product_performance
GROUP BY category
ORDER BY category_revenue DESC;

-- -----------------------------------------------------------------------------
-- Q5. Average order value for paid orders (platform-wide)
-- -----------------------------------------------------------------------------
SELECT
    SUM(total_revenue)                                AS total_platform_revenue,
    SUM(paid_order_count)                             AS total_paid_orders,
    ROUND(SUM(total_revenue) /
          NULLIF(SUM(paid_order_count), 0), 2)        AS platform_aov
FROM gold_daily_sales;

-- -----------------------------------------------------------------------------
-- Q6. Order status distribution
-- -----------------------------------------------------------------------------
SELECT
    order_status,
    order_count,
    total_amount,
    ROUND(order_count * 100.0 /
          SUM(order_count) OVER (), 2)  AS pct_of_total
FROM gold_order_status_summary
ORDER BY order_count DESC;

-- -----------------------------------------------------------------------------
-- Q7. Top 10 customers by spending
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name)   AS full_name,
    segment,
    total_orders,
    paid_orders,
    total_spent,
    avg_order_value
FROM gold_customer_analytics
ORDER BY total_spent DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Q8. Customer spending by segment
-- -----------------------------------------------------------------------------
SELECT
    COALESCE(segment, 'Unknown')   AS segment,
    COUNT(*)                       AS customer_count,
    SUM(total_spent)               AS segment_revenue,
    ROUND(AVG(total_spent), 2)     AS avg_spent_per_customer
FROM gold_customer_analytics
GROUP BY segment
ORDER BY segment_revenue DESC;

-- -----------------------------------------------------------------------------
-- Q9. Data quality — rejection rate per entity per batch
-- -----------------------------------------------------------------------------
SELECT
    batch_id,
    entity,
    ingested_count,
    valid_count,
    rejected_count,
    quality_pct,
    run_timestamp
FROM gold_data_quality_metrics
ORDER BY run_timestamp DESC, entity;

-- -----------------------------------------------------------------------------
-- Q10. Percentage of total records rejected across all entities (latest batch)
-- -----------------------------------------------------------------------------
WITH latest_batch AS (
    SELECT batch_id
    FROM gold_data_quality_metrics
    ORDER BY run_timestamp DESC
    LIMIT 1
)
SELECT
    dq.entity,
    dq.ingested_count,
    dq.rejected_count,
    dq.quality_pct,
    ROUND(dq.rejected_count * 100.0 /
          NULLIF(dq.ingested_count, 0), 2)  AS rejection_pct
FROM gold_data_quality_metrics dq
JOIN latest_batch lb USING (batch_id)
ORDER BY rejection_pct DESC;

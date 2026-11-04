-- ============================================================
-- E-Commerce ETL Pipeline - SQL Analysis Queries
-- ============================================================
-- Run these queries after the ETL pipeline has loaded data
-- into MySQL. Each query answers a key business question.
-- ============================================================

USE ecommerce_etl;

-- ============================================================
-- 1. Total Revenue
-- ============================================================
SELECT
    'Total Revenue' AS metric,
    CONCAT('$', FORMAT(SUM(total_price), 2)) AS value
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status != 'Cancelled';

-- ============================================================
-- 2. Best-Selling Products (Top 10 by Quantity)
-- ============================================================
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(oi.quantity)    AS total_qty_sold,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS total_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN orders o   ON oi.order_id   = o.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_qty_sold DESC
LIMIT 10;

-- ============================================================
-- 3. Top Customers by Spending
-- ============================================================
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.city,
    c.state,
    COUNT(DISTINCT o.order_id)              AS total_orders,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS total_spending
FROM customers c
JOIN orders o       ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id    = oi.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state
ORDER BY SUM(oi.total_price) DESC
LIMIT 10;

-- ============================================================
-- 4. Monthly Sales Performance
-- ============================================================
SELECT
    DATE_FORMAT(o.order_date, '%Y-%m')      AS sales_month,
    COUNT(DISTINCT o.order_id)              AS num_orders,
    SUM(oi.quantity)                        AS items_sold,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS monthly_revenue,
    CONCAT('$', FORMAT(AVG(oi.total_price), 2)) AS avg_item_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY DATE_FORMAT(o.order_date, '%Y-%m')
ORDER BY sales_month;

-- ============================================================
-- 5. Category-Wise Revenue
-- ============================================================
SELECT
    p.category,
    COUNT(DISTINCT oi.item_id)              AS items_sold,
    SUM(oi.quantity)                        AS total_qty,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS category_revenue,
    CONCAT('$', FORMAT(AVG(oi.total_price), 2)) AS avg_item_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN orders o   ON oi.order_id   = o.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY p.category
ORDER BY SUM(oi.total_price) DESC;

-- ============================================================
-- 6. Total Number of Orders (excluding cancelled)
-- ============================================================
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_id END)  AS delivered,
    COUNT(DISTINCT CASE WHEN order_status = 'Shipped'   THEN order_id END)  AS shipped,
    COUNT(DISTINCT CASE WHEN order_status = 'Cancelled' THEN order_id END)  AS cancelled
FROM orders;

-- ============================================================
-- 7. Average Order Value (AOV)
-- ============================================================
SELECT
    CONCAT('$', FORMAT(AVG(order_total), 2)) AS average_order_value
FROM (
    SELECT
        o.order_id,
        SUM(oi.total_price) AS order_total
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY o.order_id
) AS order_totals;

-- ============================================================
-- 8. Products with Highest Revenue
-- ============================================================
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    SUM(oi.quantity)                             AS qty_sold,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS revenue,
    RANK() OVER (ORDER BY SUM(oi.total_price) DESC) AS revenue_rank
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN orders o   ON oi.order_id   = o.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY p.product_id, p.product_name, p.category, p.brand
ORDER BY revenue_rank
LIMIT 10;

-- ============================================================
-- 9. Customers with Above-Average Spending
-- (uses subquery + HAVING)
-- ============================================================
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2)) AS total_spending
FROM customers c
JOIN orders o       ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id    = oi.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING SUM(oi.total_price) > (
    SELECT AVG(customer_total)
    FROM (
        SELECT SUM(oi2.total_price) AS customer_total
        FROM orders o2
        JOIN order_items oi2 ON o2.order_id = oi2.order_id
        WHERE o2.order_status != 'Cancelled'
        GROUP BY o2.customer_id
    ) AS avgs
)
ORDER BY SUM(oi.total_price) DESC;

-- ============================================================
-- 10. Revenue by Payment Method
-- ============================================================
SELECT
    o.payment_method,
    COUNT(DISTINCT o.order_id)                   AS order_count,
    CONCAT('$', FORMAT(SUM(oi.total_price), 2))  AS revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY o.payment_method
ORDER BY SUM(oi.total_price) DESC;

-- ============================================================
-- 11. Cross-Sell Analysis: Products Frequently Bought Together
-- ============================================================
SELECT
    p1.product_name AS product_a,
    p2.product_name AS product_b,
    COUNT(*)        AS times_bought_together
FROM order_items oi1
JOIN order_items oi2 ON oi1.order_id = oi2.order_id
                     AND oi1.product_id < oi2.product_id
JOIN products p1     ON oi1.product_id = p1.product_id
JOIN products p2     ON oi2.product_id = p2.product_id
GROUP BY p1.product_name, p2.product_name
HAVING COUNT(*) >= 2
ORDER BY times_bought_together DESC;

-- ============================================================
-- 12. Sales Summary Report (from pre-aggregated table)
-- ============================================================
SELECT
    summary_type,
    dimension_key,
    dimension_label,
    total_quantity_sold,
    CONCAT('$', FORMAT(total_revenue, 2))    AS total_revenue,
    total_orders,
    CONCAT('$', FORMAT(avg_order_value, 2))  AS avg_order_value
FROM sales_summary
ORDER BY summary_type, total_revenue DESC;


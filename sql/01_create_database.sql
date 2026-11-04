-- ============================================================
-- E-Commerce ETL Pipeline - Database Creation & Table Setup
-- ============================================================
-- Execute this script to set up the MySQL database and tables
-- before running the ETL pipeline.
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce_etl
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ecommerce_etl;

-- ============================================================
-- Drop tables if they exist (in dependency order)
-- ============================================================
DROP TABLE IF EXISTS sales_summary;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

-- ============================================================
-- Table: customers
-- Stores customer profile information
-- ============================================================
CREATE TABLE customers (
    customer_id     VARCHAR(10)   PRIMARY KEY,
    first_name      VARCHAR(50)   NOT NULL,
    last_name       VARCHAR(50)   NOT NULL,
    email           VARCHAR(100),
    phone           VARCHAR(20),
    city            VARCHAR(50),
    state           VARCHAR(10),
    country         VARCHAR(50)   DEFAULT 'USA',
    registration_date DATE,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_customer_email (email),
    INDEX idx_customer_city (city),
    INDEX idx_customer_state (state)
) ENGINE=InnoDB;

-- ============================================================
-- Table: products
-- Stores product catalog information
-- ============================================================
CREATE TABLE products (
    product_id      VARCHAR(10)   PRIMARY KEY,
    product_name    VARCHAR(100)  NOT NULL,
    category        VARCHAR(50)   NOT NULL,
    brand           VARCHAR(50),
    unit_price      DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    stock_quantity  INT           DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_product_category (category),
    INDEX idx_product_brand (brand)
) ENGINE=InnoDB;

-- ============================================================
-- Table: orders
-- Stores order header information
-- ============================================================
CREATE TABLE orders (
    order_id        VARCHAR(10)   PRIMARY KEY,
    customer_id     VARCHAR(10)   NOT NULL,
    order_date      DATE          NOT NULL,
    shipping_address VARCHAR(200),
    order_status    VARCHAR(20)   NOT NULL DEFAULT 'Pending',
    payment_method  VARCHAR(30),
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,

    INDEX idx_order_customer (customer_id),
    INDEX idx_order_date (order_date),
    INDEX idx_order_status (order_status)
) ENGINE=InnoDB;

-- ============================================================
-- Table: order_items
-- Stores individual line items within each order
-- ============================================================
CREATE TABLE order_items (
    item_id         VARCHAR(10)   PRIMARY KEY,
    order_id        VARCHAR(10)   NOT NULL,
    product_id      VARCHAR(10)   NOT NULL,
    quantity        INT           NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    discount_percent DECIMAL(5,2) DEFAULT 0.00,
    total_price     DECIMAL(12,2) GENERATED ALWAYS AS
                        (quantity * unit_price * (1 - discount_percent / 100)) STORED,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (order_id)   REFERENCES orders(order_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,

    INDEX idx_item_order (order_id),
    INDEX idx_item_product (product_id)
) ENGINE=InnoDB;

-- ============================================================
-- Table: sales_summary
-- Pre-aggregated sales metrics for fast reporting
-- ============================================================
CREATE TABLE sales_summary (
    summary_id          INT AUTO_INCREMENT PRIMARY KEY,
    summary_type        VARCHAR(30)   NOT NULL COMMENT 'by_product | by_category | by_customer | by_month',
    dimension_key       VARCHAR(100)  NOT NULL COMMENT 'The group-by key value',
    dimension_label     VARCHAR(200)  COMMENT 'Human-readable label',
    total_quantity_sold BIGINT        DEFAULT 0,
    total_revenue       DECIMAL(14,2) DEFAULT 0.00,
    total_orders        BIGINT        DEFAULT 0,
    avg_order_value     DECIMAL(12,2) DEFAULT 0.00,
    created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_summary_type (summary_type),
    INDEX idx_summary_key (dimension_key)
) ENGINE=InnoDB;

-- ============================================================
-- Verify table creation
-- ============================================================
SHOW TABLES;


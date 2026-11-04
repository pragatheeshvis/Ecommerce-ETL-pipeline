# E-Commerce Data ETL Pipeline

A production-grade data engineering application that **extracts** raw e-commerce sales data from CSV files, **transforms** and cleanses it using Apache Spark, and **loads** the processed data into MySQL for reporting and analysis.

```
┌─────────┐     ┌──────────────┐     ┌──────────┐     ┌─────────┐
│  CSV    │────▶│ Apache Spark │────▶│  MySQL   │────▶│ Reports │
│  Files  │     │  (Java ETL)  │     │ Database │     │  & SQL  │
└─────────┘     └──────────────┘     └──────────┘     └─────────┘
  EXTRACT         TRANSFORM             LOAD           ANALYZE
```

---

## Technology Stack

| Technology    | Purpose                              |
|---------------|--------------------------------------|
| Java 11+      | Core programming language            |
| Apache Spark  | Large-scale data processing          |
| MySQL 8.x     | Relational database storage          |
| JDBC          | Database connectivity                |
| Maven         | Build and dependency management      |
| CSV           | Raw input data format                |

---

## Project Structure

```
ecommerce-etl-pipeline/
├── pom.xml                              # Maven build configuration
├── README.md                            # This file
├── data/                                # Sample CSV datasets
│   ├── customers.csv                    #   20 customers (+ 2 duplicates)
│   ├── products.csv                     #   20 products  (+ 2 dupes, 1 invalid)
│   ├── orders.csv                       #   40 orders    (+ 1 dupe, 1 bad date)
│   └── order_items.csv                  #   80 items     (+ 1 dupe, 2 invalid)
├── sql/
│   ├── 01_create_database.sql           # Database & table creation DDL
│   └── 02_analysis_queries.sql          # 12 analytical SQL queries
└── src/main/java/com/ecommerce/etl/
    ├── ECommerceETLPipeline.java         # Main entry point & orchestrator
    ├── config/
    │   └── DatabaseConfig.java          # MySQL connection configuration
    ├── extract/
    │   └── DataExtractor.java           # CSV → Spark DataFrame reader
    ├── transform/
    │   └── DataTransformer.java         # Data cleaning & aggregation
    ├── load/
    │   └── DataLoader.java              # Spark DataFrame → MySQL writer
    ├── model/
    │   └── SalesSummary.java            # POJO for sales_summary records
    └── util/
        └── SparkSessionUtil.java        # SparkSession singleton manager
```

---

## Prerequisites

1. **Java 11** or higher — [Download](https://adoptium.net/)
2. **Apache Maven 3.6+** — [Download](https://maven.apache.org/download.cgi)
3. **MySQL 8.x** — [Download](https://dev.mysql.com/downloads/mysql/)

Verify installations:
```bash
java -version
mvn -version
mysql --version
```

---

## Setup & Execution

### Step 1: Configure MySQL

1. Start your MySQL server.
2. Update the database credentials in [`DatabaseConfig.java`](src/main/java/com/ecommerce/etl/config/DatabaseConfig.java):

```java
private static final String HOST     = "localhost";
private static final int    PORT     = 3306;
private static final String DATABASE = "ecommerce_etl";
private static final String USER     = "root";
private static final String PASSWORD = "root";  // ← Change this
```

3. *(Optional)* You can manually run the SQL script to pre-create the database:
```bash
mysql -u root -p < sql/01_create_database.sql
```
> The ETL pipeline creates the database and tables automatically, so this step is optional.

### Step 2: Build the Project

```bash
cd ecommerce-etl-pipeline
mvn clean package -DskipTests
```

This produces a fat JAR at `target/ecommerce-etl-pipeline-1.0.0.jar`.

### Step 3: Run the ETL Pipeline

```bash
# Option A: Run via Maven
mvn exec:java

# Option B: Run the fat JAR directly
java -jar target/ecommerce-etl-pipeline-1.0.0.jar data

# Option C: Run with a custom data directory
java -jar target/ecommerce-etl-pipeline-1.0.0.jar /path/to/csv/files
```

### Step 4: Run Analysis Queries

After the pipeline loads data into MySQL:
```bash
mysql -u root -p ecommerce_etl < sql/02_analysis_queries.sql
```

---

## ETL Pipeline Details

### Phase 1: Extract

The `DataExtractor` reads four CSV files with **explicit schemas** for type safety:

| File              | Records | Columns |
|-------------------|---------|---------|
| `customers.csv`   | 22*     | 9       |
| `products.csv`    | 23*     | 6       |
| `orders.csv`      | 41*     | 6       |
| `order_items.csv` | 83*     | 6       |

*\* Includes intentional duplicates and invalid records for testing transformations.*

### Phase 2: Transform

The `DataTransformer` applies these operations:

| Operation                        | Description                                              |
|----------------------------------|----------------------------------------------------------|
| **Deduplication**                | Removes exact duplicate rows by primary key              |
| **Null handling**                | Fills missing emails/phones with `"N/A"`, defaults discounts to 0 |
| **Invalid record removal**       | Drops rows with negative prices, zero/negative quantities, unknown categories |
| **Date parsing**                 | Handles `yyyy-MM-dd` and `dd-MM-yyyy` formats; drops unparseable dates |
| **Total price calculation**      | `quantity × unit_price × (1 - discount_percent / 100)`   |
| **Sales aggregation**            | Groups by product, category, customer, and month         |

**Sample Transform Output:**

```
order_items: 83 → 79 rows  (4 removed)
  - 1 duplicate (I001 repeated)
  - 1 invalid quantity (0)
  - 1 negative quantity (-2)
  - 1 zero unit_price ($0.00)
```

### Phase 3: Load

The `DataLoader` writes cleaned DataFrames to MySQL via JDBC:

1. Creates the `ecommerce_etl` database if it doesn't exist.
2. Drops and recreates all five tables for idempotent runs.
3. Writes data using Spark's JDBC writer with batch inserts.
4. Verifies row counts post-load.

---

## Database Schema

```
┌─────────────┐       ┌─────────────┐
│  customers  │       │  products   │
├─────────────┤       ├─────────────┤
│ customer_id │ PK    │ product_id  │ PK
│ first_name  │       │ product_name│
│ last_name   │       │ category    │
│ email       │       │ brand       │
│ phone       │       │ unit_price  │
│ city        │       │ stock_qty   │
│ state       │       └──────┬──────┘
│ country     │              │
│ reg_date    │              │
└──────┬──────┘              │
       │                     │
       │ FK                  │ FK
       ▼                     ▼
┌─────────────┐       ┌─────────────┐
│   orders    │       │ order_items │
├─────────────┤       ├─────────────┤
│ order_id    │ PK ◀──│ order_id    │ FK
│ customer_id │ FK    │ item_id     │ PK
│ order_date  │       │ product_id  │ FK
│ ship_addr   │       │ quantity    │
│ status      │       │ unit_price  │
│ payment     │       │ discount_%  │
└─────────────┘       │ total_price │
                      └─────────────┘

       ┌───────────────┐
       │ sales_summary │  (Pre-aggregated)
       ├───────────────┤
       │ summary_id    │ PK (AUTO)
       │ summary_type  │  by_product | by_category | by_customer | by_month
       │ dimension_key │
       │ dimension_lbl │
       │ total_qty_sold│
       │ total_revenue │
       │ total_orders  │
       │ avg_order_val │
       └───────────────┘
```

---

## SQL Analysis Queries

The file `sql/02_analysis_queries.sql` contains 12 analytical queries:

| #  | Query                                    | SQL Concepts Used                       |
|----|------------------------------------------|-----------------------------------------|
| 1  | Total Revenue                            | `SUM`, `JOIN`                           |
| 2  | Best-Selling Products (Top 10)           | `GROUP BY`, `ORDER BY`, `LIMIT`         |
| 3  | Top Customers by Spending                | `JOIN`, `GROUP BY`, `CONCAT`            |
| 4  | Monthly Sales Performance                | `DATE_FORMAT`, `GROUP BY`, `AVG`        |
| 5  | Category-Wise Revenue                    | `GROUP BY`, `SUM`, `COUNT`              |
| 6  | Total Orders by Status                   | `CASE WHEN`, `COUNT DISTINCT`           |
| 7  | Average Order Value (AOV)                | **Subquery**, `AVG`                     |
| 8  | Products with Highest Revenue            | `RANK() OVER`, Window Functions         |
| 9  | Customers with Above-Average Spending    | `HAVING`, **Nested Subquery**           |
| 10 | Revenue by Payment Method                | `GROUP BY`, `SUM`                       |
| 11 | Cross-Sell Analysis (Bought Together)     | **Self-JOIN**, `HAVING`                 |
| 12 | Sales Summary Report                     | Pre-aggregated table query              |

---

## Sample Input & Output

### Sample Input (`order_items.csv`)
```csv
item_id,order_id,product_id,quantity,unit_price,discount_percent
I001,O001,P001,2,29.99,0
I002,O001,P003,1,89.99,10
I003,O002,P002,1,49.99,0
```

### Sample Transform Output
```
+-------+--------+----------+--------+----------+----------------+-----------+
|item_id|order_id|product_id|quantity|unit_price|discount_percent|total_price|
+-------+--------+----------+--------+----------+----------------+-----------+
|I001   |O001    |P001      |2       |29.99     |0.0             |59.98      |
|I002   |O001    |P003      |1       |89.99     |10.0            |80.99      |
|I003   |O002    |P002      |1       |49.99     |0.0             |49.99      |
+-------+--------+----------+--------+----------+----------------+-----------+
```

### Sample Sales Summary (by_category)
```
+------------+-------------+---------------+------------------+-------------+------------+---------------+
|summary_type|dimension_key|dimension_label|total_quantity_sold|total_revenue|total_orders|avg_order_value|
+------------+-------------+---------------+------------------+-------------+------------+---------------+
|by_category |Electronics  |Electronics    |35                |4250.50      |25          |121.44         |
|by_category |Sports       |Sports         |18                |1580.75      |12          |87.82          |
|by_category |Health       |Health         |16                |1120.30      |10          |70.02          |
+------------+-------------+---------------+------------------+-------------+------------+---------------+
```

---

## Error Handling

The pipeline handles the following edge cases:

- **Missing CSV files** → Descriptive error with the expected path.
- **Duplicate records** → Deduplicated by primary key before loading.
- **Null / missing values** → Filled with defaults or marked `"N/A"`.
- **Invalid records** → Removed (negative prices, zero quantities, unknown categories).
- **Unparseable dates** → Multi-format parser; drops if no format matches.
- **MySQL connection failures** → Wrapped in `RuntimeException` with context.
- **JDBC write failures** → Per-table error reporting with stack traces.

---

## Troubleshooting

| Issue                              | Solution                                                   |
|------------------------------------|------------------------------------------------------------|
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | Ensure MySQL connector is in `pom.xml` dependencies |
| `Access denied for user 'root'`    | Update credentials in `DatabaseConfig.java`                |
| `Communications link failure`      | Verify MySQL is running on the configured host/port        |
| `OutOfMemoryError`                 | Increase driver memory: `--driver-memory 4g`               |
| `Table doesn't exist`             | The pipeline auto-creates tables; ensure DB user has DDL privileges |

---

## License

This project is provided for educational and portfolio purposes.


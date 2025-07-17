# 🧊 SQL Data Warehouse Project

A fully modular, multi-layered **SQL Data Warehouse** solution designed to ingest, clean, transform, and present structured business data using the Bronze–Silver–Gold architecture. This warehouse is optimized for scalable analytics and supports dimensional modeling with efficient OLAP-ready views.

---

## 📚 Table of Contents

* [🚀 Project Overview](#-project-overview)
* [🎯 Objectives](#-objectives)
* [📐 Architecture](#-architecture)
* [🧰 Tech Stack](#-tech-stack)
* [🧩 Data Model](#-data-model)
* [🔁 ETL Process](#-etl-process)
* [📦 How to Use](#-how-to-use)
* [🌟 Key Features](#-key-features)
* [📊 Sample Queries](#-sample-queries)
* [🔮 Future Improvements](#-future-improvements)
* [📄 License](#-license)

---

## 🚀 Project Overview

This project demonstrates the end-to-end design and implementation of a SQL-based Data Warehouse using a multi-layered approach. It simulates a real-world BI solution integrating CRM and ERP datasets from CSV sources and organizing them for optimized reporting and dashboarding.

---

## 🎯 Objectives

* ✅ Build a multi-schema warehouse (`bronze`, `silver`, `gold`) to separate raw, cleansed, and curated data.
* ✅ Automate ETL using stored procedures for each layer.
* ✅ Design dimensional models (star schema) with surrogate keys and clean semantics.
* ✅ Enable complex analytical queries and aggregations.
* ✅ Ensure data accuracy, consistency, and interpretability.

---

## 📐 Architecture

```
                     +--------------------------+
                     |      CSV Data Files      |
                     +-----------+--------------+
                                 |
                          [Bulk Insert]
                                 |
                      +----------v----------+
                      |     Bronze Layer     |
                      | Raw staging tables   |
                      +----------+-----------+
                                 |
                         [Transformations]
                                 |
                      +----------v----------+
                      |     Silver Layer     |
                      | Cleaned business data|
                      +----------+-----------+
                                 |
                           [Data Marts]
                                 |
                      +----------v----------+
                      |      Gold Layer      |
                      | Star schema views    |
                      +----------------------+
```

---

## 🧰 Tech Stack

* **SQL Server** (T-SQL)
* **CSV Data Files** as raw sources
* **BULK INSERT** for data ingestion
* **Stored Procedures** for automation
* **Partitioning/Views/Indexes** (planned for optimization)

---

## 🧩 Data Model

### Bronze Layer

Stores raw data loaded directly from source files.

**CRM Tables:**

* `crm_cust_info`
* `crm_prd_info`
* `crm_sales_details`

**ERP Tables:**

* `erp_cust_az12`
* `erp_loc_a101`
* `erp_px_cat_g1v2`

### Silver Layer

Applies data cleansing, normalization, deduplication, and validation logic.

### Gold Layer

Business-ready views for analytics and reporting:

* `dim_customers`
* `dim_products`
* `fact_sales`

---

## 🔁 ETL Process

### 1. Bronze Layer: Raw Load

**Procedure:** `bronze.load_bronze`

* Truncates all bronze tables
* Uses `BULK INSERT` to load CSV files

### 2. Silver Layer: Data Cleaning & Enrichment

**Procedure:** `silver.load_silver`

* Normalizes values (e.g., gender, marital status)
* Filters duplicates and invalid entries
* Applies derivations (e.g., inferred sales and prices)

### 3. Gold Layer: Star Schema Views

**DDL File:** `ddl_gold.sql`

* Surrogate key generation
* Combines ERP and CRM dimensions
* Joins fact tables to dimension views

---

## 📦 How to Use

### Setup

1. Run `init_database.sql` to create the database and schemas.
2. Run `ddl_bronze.sql`, `ddl_silver.sql`, and `ddl_gold.sql` to create all required tables and views.
3. Execute the ETL steps:

   ```sql
   EXEC bronze.load_bronze;
   EXEC silver.load_silver;
   ```

> ⚠️ **Warning:** Running `init_database.sql` will delete and recreate the entire database.

---

## 🌟 Key Features

* 🔄 End-to-end ETL automation using stored procedures
* 🧽 Cleansing logic for invalid/missing values
* 🧠 Business logic to derive fields like `sales`, `price`, `prd_end_dt`
* 🧩 Star schema with surrogate keys and conforming dimensions
* 📈 Ready for BI tools and dashboard integration

---

## 📊 Sample Queries

```sql
-- Top 5 customers by total sales
SELECT TOP 5
    cu.first_name + ' ' + cu.last_name AS customer,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers cu ON f.customer_key = cu.customer_key
GROUP BY cu.first_name, cu.last_name
ORDER BY total_sales DESC;

-- Monthly sales trend
SELECT
    FORMAT(order_date, 'yyyy-MM') AS sales_month,
    SUM(sales_amount) AS monthly_sales
FROM gold.fact_sales
GROUP BY FORMAT(order_date, 'yyyy-MM')
ORDER BY sales_month;
```

---

## 🔮 Future Improvements

* Add incremental loading and change tracking
* Implement indexes and partitions on fact tables
* Connect to Power BI/Tableau for visualization
* Add audit logging and error handling enhancements
* Add CDC or streaming source integration

---

## 📄 License

This project is released under the [MIT License](LICENSE).

---


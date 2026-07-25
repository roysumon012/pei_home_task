-- Databricks notebook source
-- MAGIC %md
-- MAGIC 07_sql_queries
-- MAGIC
-- MAGIC Reporting queries for the PEI Lead Data Engineer Assessment

-- COMMAND ----------

-- USE CATALOG AND SCHEMA
USE CATALOG <catalog_name>;
USE SCHEMA <schema_name>;

-- COMMAND ----------

-- 1. Profit by year
SELECT
    YEAR(order_date) AS year,
    ROUND(SUM(profit), 2) AS total_profit
FROM silver_order
GROUP BY year
ORDER BY year;

-- COMMAND ----------

-- 2. Profit by year, category
SELECT
    YEAR(order_date) AS year,
    category,
    ROUND(SUM(profit), 2) AS total_profit
FROM silver_order
GROUP BY YEAR(order_date), category
ORDER BY year, category;

-- COMMAND ----------

-- 3. Profit by customer
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(profit),2) AS total_profit
FROM silver_order
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;

-- COMMAND ----------

-- 4. Profit by customer, year
SELECT
    customer_id,
    customer_name,
    YEAR(order_date) AS year,
    ROUND(SUM(profit),2) AS total_profit
FROM silver_order
GROUP BY customer_id, customer_name, YEAR(order_date)
ORDER BY customer_id, customer_name, year, total_profit DESC;
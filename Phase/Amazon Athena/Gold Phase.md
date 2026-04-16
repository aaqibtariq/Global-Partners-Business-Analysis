
# create database

```sql

CREATE DATABASE IF NOT EXISTS globalpartners_gold;
```

# Create Tables

```sql

DROP TABLE IF EXISTS globalpartners_gold.sales;

CREATE EXTERNAL TABLE globalpartners_gold.sales
LOCATION 's3://globalpartner-datalake/gold/sales/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_gold.loyalty
LOCATION 's3://globalpartner-datalake/gold/loyalty/'
TBLPROPERTIES ('table_type' = 'DELTA');



```

# Check Tables

```sql


SHOW TABLES IN globalpartners_gold;

```
## Check Counts

```sql

SELECT COUNT(*) FROM globalpartners_gold.sales;
SELECT COUNT(*) FROM globalpartners_gold.loyalty;

```
## gold.sales

```sql
# Check First 10 rows

SELECT * 
FROM globalpartners_gold.sales
ORDER BY order_date
LIMIT 10;

# Min and Max
SELECT MIN(order_date), MAX(order_date)
FROM globalpartners_gold.sales;

# compare that revenue total with Silver:

SELECT SUM(total_revenue)
FROM globalpartners_gold.sales;

SELECT SUM(line_total)
FROM globalpartners_silver.orders;


# Total revenue 

SELECT restaurant_id, item_category, total_orders, total_revenue
FROM globalpartners_gold.sales
ORDER BY total_revenue DESC
LIMIT 10;

```
## gold.loyalty

```sql

# View full table
SELECT * FROM globalpartners_gold.loyalty;

# Revenue check with silver and gold

SELECT SUM(total_revenue) FROM globalpartners_gold.loyalty;
SELECT SUM(line_total) FROM globalpartners_silver.orders WHERE user_id != 'GUEST';

# Order check with silver and gold

SELECT SUM(total_orders) FROM globalpartners_gold.loyalty;
SELECT COUNT(DISTINCT order_id) FROM globalpartners_silver.orders WHERE user_id != 'GUEST';

# Sanity check (distribution)

SELECT 
    cohort_label,
    unique_customers,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_clv_per_customer
FROM globalpartners_gold.loyalty;

```


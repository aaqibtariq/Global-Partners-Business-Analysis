
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

```

# Check Tables

```sql


SHOW TABLES IN globalpartners_gold;

```
## Check Counts

```sql

SELECT COUNT(*) FROM globalpartners_gold.sales;

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

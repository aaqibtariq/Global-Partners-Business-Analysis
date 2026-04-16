
# create database

```sql

CREATE DATABASE IF NOT EXISTS globalpartners_gold;
```

# Create Tables

```sql


CREATE EXTERNAL TABLE globalpartners_gold.sales
LOCATION 's3://globalpartner-datalake/gold/sales/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_gold.loyalty
LOCATION 's3://globalpartner-datalake/gold/loyalty/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_gold.daily_clv
LOCATION 's3://globalpartner-datalake/gold/daily_clv/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_gold.rfm
LOCATION 's3://globalpartner-datalake/gold/rfm/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_gold.churn
LOCATION 's3://globalpartner-datalake/gold/churn/'
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
SELECT COUNT(*) FROM globalpartners_gold.daily_clv;
SELECT COUNT(*) FROM globalpartners_gold.rfm;
SELECT COUNT(*) FROM globalpartners_gold.churn;

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
# daily_clv

```sql

# Check total count

SELECT clv_segment, COUNT(*)
FROM globalpartners_gold.daily_clv
GROUP BY clv_segment;

# last 10 rows

SELECT *
FROM globalpartners_gold.daily_clv
ORDER BY total_spent DESC
LIMIT 10;

```

# gold.rfm

```sql

# Segment distribution

SELECT rfm_segment, COUNT(*)
FROM globalpartners_gold.rfm
GROUP BY rfm_segment
ORDER BY COUNT(*) DESC;


# Top VIPs

SELECT *
FROM globalpartners_gold.rfm
WHERE rfm_segment = 'VIP'
ORDER BY monetary_value DESC
LIMIT 10;

# Churn Risk check

SELECT *
FROM globalpartners_gold.rfm
WHERE rfm_segment = 'Churn Risk'
ORDER BY recency_days DESC
LIMIT 10;

```

# gold_churn

```sql

# Status distribution

SELECT churn_status, COUNT(*)
FROM globalpartners_gold.churn
GROUP BY churn_status;

# Top at-risk users

SELECT *
FROM globalpartners_gold.churn
WHERE churn_status = 'At Risk'
ORDER BY days_since_last_order DESC
LIMIT 10;

# Spend trend check

SELECT *
FROM globalpartners_gold.churn
ORDER BY spend_change_pct ASC
LIMIT 10;

```

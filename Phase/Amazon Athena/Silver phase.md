# Create Database

```sql
CREATE DATABASE globalpartners_silver;
```

# Create table

```sql


CREATE EXTERNAL TABLE globalpartners_silver.date_dim
LOCATION 's3://globalpartner-datalake/silver/date_dim/'
TBLPROPERTIES ('table_type' = 'DELTA');


CREATE EXTERNAL TABLE globalpartners_silver.orders
LOCATION 's3://globalpartner-datalake/silver/orders/'
TBLPROPERTIES ('table_type' = 'DELTA');


CREATE EXTERNAL TABLE IF NOT EXISTS globalpartners_silver.orders_quarantine
LOCATION 's3://globalpartner-datalake/silver/quarantine/orders/'
TBLPROPERTIES ('table_type'='DELTA');

```

# Show Table 

```sql

SHOW TABLES IN globalpartners_silver;

```

# Check count 

```sql

SELECT COUNT(*) FROM globalpartners_silver.date_dim;
SELECT COUNT(*) FROM globalpartners_silver.orders;
SELECT COUNT(*) FROM globalpartners_silver.orders_quarantine;
```

# Check First 10 and last 10 Rows

```sql

SELECT * FROM globalpartners_silver.date_dim LIMIT 10;

SELECT * FROM  globalpartners_silver.date_dim
ORDER BY DATE_KEY DESC
LIMIT 10;


SELECT * FROM globalpartners_silver.orders LIMIT 10;

SELECT *
FROM globalpartners_silver.orders
ORDER BY order_ts DESC
LIMIT 10;
```

# Null timestamp check 

```

SELECT COUNT(*) AS bad_ts
FROM globalpartners_silver.orders
WHERE order_ts IS NULL;

```
# Revenue validation

```
SELECT
    order_id,
    item_price,
    item_quantity,
    option_price,
    option_quantity,
    line_total
FROM globalpartners_silver.orders
LIMIT 10;


```

# Negative / bad values check

```
SELECT COUNT(*) AS bad_prices
FROM globalpartners_silver.orders
WHERE item_price < 0;

```

# Distribution check

```

SELECT item_category, COUNT(*) AS cnt
FROM globalpartners_silver.orders
GROUP BY item_category
ORDER BY cnt DESC;

```

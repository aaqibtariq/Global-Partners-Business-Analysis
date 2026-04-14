
# Create Database

```sql

CREATE DATABASE globalpartners_bronze;

```


# Create tables 

```sql

CREATE EXTERNAL TABLE globalpartners_bronze.order_items
LOCATION 's3://globalpartner-datalake/bronze/order_items/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_bronze.order_item_options
LOCATION 's3://globalpartner-datalake/bronze/order_item_options/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE globalpartners_bronze.date_dim
LOCATION 's3://globalpartner-datalake/bronze/date_dim/'
TBLPROPERTIES ('table_type' = 'DELTA');

```

# Show Tables


```sql

SHOW TABLES IN globalpartners_bronze;

```

###  Target Data Preview (Athena)

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Amazon%20Athena/Bronze%20phase%20result/table%20show.png" width="700"/>
</p>


# Count all tables row

```sql

SELECT COUNT(*) FROM globalpartners_bronze.order_items
union all
SELECT COUNT(*) FROM globalpartners_bronze.order_item_options
union all
SELECT COUNT(*) FROM globalpartners_bronze.date_dim;

```
###  Source Validation (SQL Server – SSMS)

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Amazon%20Athena/Bronze%20phase%20result/SSMS%20table%20count.jpg" width="700"/>
</p>

---

###  Target Validation (Athena – Table Count)

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Amazon%20Athena/Bronze%20phase%20result/table%20count.png" width="700"/>
</p>


# Check first 10 rows

``` sql

SELECT *
FROM globalpartners_bronze.order_items
LIMIT 10;

SELECT *
FROM globalpartners_bronze.order_item_options
LIMIT 10;

SELECT *
FROM globalpartners_bronze.date_dim
LIMIT 10;

```

# Check last 10 rows

```sql

SELECT *
FROM globalpartners_bronze.order_items
ORDER BY ORDER_ID DESC, LINEITEM_ID DESC
LIMIT 10;


SELECT *
FROM globalpartners_bronze.order_item_options
ORDER BY ORDER_ID DESC, LINEITEM_ID DESC
LIMIT 10;

SELECT *
FROM globalpartners_bronze.date_dim
ORDER BY DATE_KEY DESC
LIMIT 10;

```


# Check Nulls

```sql

SELECT *
FROM dbo.order_item_options
WHERE OPTION_PRICE IS NULL
   OR OPTION_QUANTITY IS NULL;

```

# check Duplicates 

``` sql

SELECT ORDER_ID, LINEITEM_ID, COUNT(*)
FROM dbo.order_items
GROUP BY ORDER_ID, LINEITEM_ID
HAVING COUNT(*) > 1;

SELECT ORDER_ID, LINEITEM_ID, COUNT(*)
FROM dbo.order_item_options
GROUP BY ORDER_ID, LINEITEM_ID
HAVING COUNT(*) > 1;

```

# check join coverage 

```sql

SELECT COUNT(*)
FROM dbo.order_items oi
LEFT JOIN dbo.order_item_options oio
  ON oi.ORDER_ID = oio.ORDER_ID
 AND oi.LINEITEM_ID = oio.LINEITEM_ID
WHERE oio.ORDER_ID IS NULL;

```

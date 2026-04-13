# Create Database

```sql
CREATE DATABASE globalpartners_silver;
```

# Create table

```sql


CREATE EXTERNAL TABLE globalpartners_silver.date_dim
LOCATION 's3://globalpartner-datalake/silver/date_dim/'
TBLPROPERTIES ('table_type' = 'DELTA');

```

# Show Table 

```sql

SHOW TABLES IN globalpartners_silver;

```

# Check count 

```sql

SELECT COUNT(*) FROM globalpartners_silver.date_dim;

```

# Check Rows

```sql

SELECT * FROM globalpartners_silver.date_dim LIMIT 10;

SELECT * FROM  globalpartners_silver.date_dim
ORDER BY DATE_KEY DESC
LIMIT 10;

```


# glue_gold_sales.py

```sql

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    to_date,
    countDistinct,
    sum as spark_sum,
    round as spark_round,
    current_timestamp
)
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Paths ─────────────────────────────────────────────────
SILVER_ORDERS = "s3://globalpartner-datalake/silver/orders/"
SILVER_DATE_DIM = "s3://globalpartner-datalake/silver/date_dim/"
GOLD_PATH = "s3://globalpartner-datalake/gold/sales/"

print("=== STEP 1: Reading Silver tables ===")
orders = spark.read.format("delta").load(SILVER_ORDERS)
date_dim = spark.read.format("delta").load(SILVER_DATE_DIM)

print(f"Silver orders rows:   {orders.count()}")
print(f"Silver date_dim rows: {date_dim.count()}")

# ── STEP 2: Derive order_date from timestamp ──────────────
print("=== STEP 2: Deriving order_date from order_ts ===")
orders = orders.withColumn("order_date", to_date(col("order_ts")))

# ── STEP 3: Join to date_dim for year/month/week ──────────
print("=== STEP 3: Joining orders to date_dim ===")
joined = orders.join(
    date_dim.select(
        col("date_key_parsed").alias("dim_date"),
        "year",
        "month",
        "week"
    ),
    orders["order_date"] == col("dim_date"),
    "left"
).drop("dim_date")

# ── STEP 4: Aggregate gold_sales at daily grain ───────────
print("=== STEP 4: Aggregating gold_sales ===")
gold_sales = (
    joined.groupBy(
        "order_date",
        "year",
        "month",
        "week",
        "restaurant_id",
        "item_category"
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum("line_total").alias("total_revenue"),
        spark_sum("item_quantity").alias("total_items_sold")
    )
)

gold_sales = gold_sales.withColumn(
    "avg_order_value",
    spark_round(col("total_revenue") / col("total_orders"), 2)
)

gold_sales = gold_sales.withColumn("gold_load_ts", current_timestamp())

output_count = gold_sales.count()
print(f"Gold sales rows: {output_count}")

# ── STEP 5: Write Gold Delta ──────────────────────────────
print("=== STEP 5: Writing Gold sales Delta ===")
if DeltaTable.isDeltaTable(spark, GOLD_PATH):
    dt = DeltaTable.forPath(spark, GOLD_PATH)
    (
        dt.alias("old")
        .merge(
            gold_sales.alias("new"),
            """
            old.order_date = new.order_date
            AND old.restaurant_id = new.restaurant_id
            AND old.item_category = new.item_category
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Delta MERGE complete")
else:
    gold_sales.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print("First run — Delta overwrite complete")

print(f"=== SUCCESS: gold_sales complete — {output_count} rows at {GOLD_PATH} ===")
job.commit()

```
# glue_gold_loyalty.py

```sql

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct,
    round as spark_round, when, current_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_LOYALTY = "s3://globalpartner-datalake/gold/loyalty/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Total rows: {silver.count()}")

# Exclude guest users from loyalty analysis
silver_members = silver.filter(col("user_id") != "GUEST")

# Aggregate loyalty vs non-loyalty cohorts
loyalty = (
    silver_members
    .groupBy("is_loyalty")
    .agg(
        countDistinct("user_id").alias("unique_customers"),
        countDistinct("order_id").alias("total_orders"),
        spark_round(spark_sum("line_total"), 2).alias("total_revenue")
    )
    .withColumn(
        "avg_order_value",
        spark_round(col("total_revenue") / col("total_orders"), 2)
    )
    .withColumn(
        "avg_clv_per_customer",
        spark_round(col("total_revenue") / col("unique_customers"), 2)
    )
    .withColumn(
        "cohort_label",
        when(col("is_loyalty") == True, "Loyalty Member")
        .otherwise("Non-Member")
    )
    .withColumn("gold_load_ts", current_timestamp())
)

print("Loyalty comparison:")
loyalty.show(truncate=False)

row_count = loyalty.count()
print(f"Loyalty table rows: {row_count} (expected 2)")

loyalty.write.format("delta").mode("overwrite").save(GOLD_LOYALTY)
print(f"=== SUCCESS: Gold Loyalty at {GOLD_LOYALTY} ===")
job.commit()


```

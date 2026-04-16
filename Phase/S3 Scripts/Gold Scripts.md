
# glue_gold_sales.py

```python

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

```python

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

# glue_gold_daily_clv.py

```python

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    current_timestamp,
    when
)
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_CLV = "s3://globalpartner-datalake/gold/daily_clv/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Total Silver rows: {silver.count()}")

# Exclude guest users
customers = silver.filter(col("user_id") != "GUEST")

# Aggregate customer lifetime value metrics
print("=== Aggregating customer CLV ===")
clv = (
    customers.groupBy("user_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_round(spark_sum("line_total"), 2).alias("total_spent"),
        spark_min("order_ts").alias("first_order_ts"),
        spark_max("order_ts").alias("last_order_ts")
    )
)

clv = clv.withColumn(
    "avg_order_value",
    spark_round(col("total_spent") / col("total_orders"), 2)
)

# 20/60/20 segmentation using ntile(5)
print("=== Assigning CLV segments ===")
w = Window.orderBy(col("total_spent").desc())

clv = clv.withColumn("clv_bucket", ntile(5).over(w))

clv = clv.withColumn(
    "clv_segment",
    when(col("clv_bucket") == 1, "High CLV")
    .when(col("clv_bucket") == 5, "Low CLV")
    .otherwise("Medium CLV")
)

clv = clv.drop("clv_bucket").withColumn("gold_load_ts", current_timestamp())

print(f"Gold CLV rows: {clv.count()}")
clv.show(10, truncate=False)

# Write Gold output
clv.write.format("delta").mode("overwrite").save(GOLD_CLV)

print(f"=== SUCCESS: Gold daily_clv at {GOLD_CLV} ===")
job.commit()

```

# glue_gold_rfm

```sql

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    datediff,
    countDistinct,
    max as spark_max,
    sum as spark_sum,
    when,
    col,
    to_date,
    current_timestamp,
    lit
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_RFM = "s3://globalpartner-datalake/gold/rfm/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)

# Exclude guest users first
silver = silver.filter(col("user_id") != "GUEST")

# Convert order timestamp to date
silver = silver.withColumn("order_date", to_date(col("order_ts")))

# Get max date in dataset so recency is relative to data, not today
max_order_date = silver.agg(spark_max("order_date").alias("max_date")).collect()[0]["max_date"]
print(f"Dataset max order date: {max_order_date}")

# Aggregate to order level first
order_totals = (
    silver.groupBy("user_id", "order_id", "order_date")
    .agg(
        spark_sum("line_total").alias("order_value"),
        spark_max("order_date").alias("last_order_date")
    )
)

# Customer-level RFM metrics
rfm = (
    order_totals.groupBy("user_id")
    .agg(
        datediff(lit(max_order_date), spark_max("last_order_date")).alias("recency_days"),
        countDistinct("order_id").alias("frequency_orders"),
        spark_sum("order_value").alias("monetary_value")
    )
)

# Improved segmentation logic
rfm = rfm.withColumn(
    "rfm_segment",
    when(
        (col("recency_days") <= 30) & (col("frequency_orders") >= 5),
        "VIP"
    )
    .when(
        (col("recency_days") <= 30) & (col("frequency_orders") <= 2),
        "New Customer"
    )
    .when(
        col("recency_days") > 90,
        "Churn Risk"
    )
    .otherwise("Regular")
)

rfm = rfm.withColumn("gold_load_ts", current_timestamp())

print(f"RFM customers: {rfm.count()}")
print("Segment distribution:")
rfm.groupBy("rfm_segment").count().orderBy("count", ascending=False).show()

rfm.write.format("delta").mode("overwrite").save(GOLD_RFM)
print(f"=== SUCCESS: Gold RFM at {GOLD_RFM} ===")
job.commit()
```

# glue_gold_churn.py

```python

import sys
from datetime import timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    lag,
    avg,
    datediff,
    col,
    when,
    max as spark_max,
    sum as spark_sum,
    to_date,
    round as spark_round,
    lit,
    current_timestamp
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_CHURN = "s3://globalpartner-datalake/gold/churn/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)

# Exclude guest users first
silver = silver.filter(col("user_id") != "GUEST")

# Convert timestamp to date
silver = silver.withColumn("order_date", to_date(col("order_ts")))

# Use dataset max date, not current_date()
max_order_date = silver.agg(
    spark_max("order_date").alias("max_date")
).collect()[0]["max_date"]

print(f"Dataset max order date: {max_order_date}")

# Order-level aggregation
order_rev = (
    silver.groupBy("user_id", "order_id", "order_date")
    .agg(
        spark_round(spark_sum("line_total"), 2).alias("order_value")
    )
)

# ── STEP 1: Average gap between orders ────────────────────
w = Window.partitionBy("user_id").orderBy("order_date")

churn = (
    order_rev
    .withColumn("prev_order_date", lag("order_date", 1).over(w))
    .withColumn("gap_days", datediff(col("order_date"), col("prev_order_date")))
)

# ── STEP 2: Customer-level base churn metrics ─────────────
churn_summary = (
    churn.groupBy("user_id")
    .agg(
        spark_max("order_date").alias("last_order_date"),
        spark_round(avg("gap_days"), 2).alias("avg_gap_days"),
        spark_round(spark_sum("order_value"), 2).alias("total_spend")
    )
    .withColumn(
        "days_since_last_order",
        datediff(lit(max_order_date), col("last_order_date"))
    )
)

# ── STEP 3: Spend trend windows (recent 90 days vs prior 90 days) ──
recent_start = max_order_date - timedelta(days=90)
prior_start = max_order_date - timedelta(days=180)
prior_end = max_order_date - timedelta(days=91)

print(f"Recent window start: {recent_start}")
print(f"Prior window: {prior_start} to {prior_end}")

recent_spend = (
    order_rev
    .filter(col("order_date") >= lit(recent_start))
    .groupBy("user_id")
    .agg(
        spark_round(spark_sum("order_value"), 2).alias("recent_90d_spend")
    )
)

prior_spend = (
    order_rev
    .filter((col("order_date") >= lit(prior_start)) & (col("order_date") <= lit(prior_end)))
    .groupBy("user_id")
    .agg(
        spark_round(spark_sum("order_value"), 2).alias("prior_90d_spend")
    )
)

# ── STEP 4: Join spend trend metrics ──────────────────────
churn_summary = (
    churn_summary
    .join(recent_spend, ["user_id"], "left")
    .join(prior_spend, ["user_id"], "left")
    .fillna({
        "avg_gap_days": 0,
        "recent_90d_spend": 0,
        "prior_90d_spend": 0
    })
)

# Spend change %
churn_summary = churn_summary.withColumn(
    "spend_change_pct",
    when(
        col("prior_90d_spend") > 0,
        spark_round(
            ((col("recent_90d_spend") - col("prior_90d_spend")) / col("prior_90d_spend")) * 100,
            2
        )
    ).otherwise(None)
)

# ── STEP 5: Churn status ──────────────────────────────────
# Using requirement-aligned threshold:
# >45 days = At Risk
# <=7 days = Active
# otherwise = Stable
churn_summary = churn_summary.withColumn(
    "churn_status",
    when(col("days_since_last_order") > 45, "At Risk")
    .when(col("days_since_last_order") <= 7, "Active")
    .otherwise("Stable")
)

churn_summary = churn_summary.withColumn("gold_load_ts", current_timestamp())

print(f"Churn customers: {churn_summary.count()}")
print("Churn status distribution:")
churn_summary.groupBy("churn_status").count().show()

# Write Gold output
churn_summary.write.format("delta").mode("overwrite").save(GOLD_CHURN)
print(f"=== SUCCESS: Gold Churn at {GOLD_CHURN} ===")
job.commit()

```


# glue_gold_discounts.py


```python

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    to_date,
    when,
    lit,
    coalesce,
    countDistinct,
    sum as spark_sum,
    round as spark_round,
    current_timestamp,
    max as spark_max
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_DISCOUNTS = "s3://globalpartner-datalake/gold/discounts/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver rows: {silver.count()}")

# Exclude guests only if you want customer-focused metrics.
# For sales comparison, keep all orders.
# Convert timestamp to date for trend reporting.
silver = silver.withColumn("order_date", to_date(col("order_ts")))

# ── STEP 1: Create line-level discount and revenue fields ─────────────
# Gross revenue = item revenue before discount/options
# Discount amount = only negative option_price impact, stored as positive number
# Net revenue = already represented by line_total in Silver
print("=== STEP 1: Calculating gross, discount, and net revenue at line level ===")

line_discount_amount = when(
    col("option_price") < 0,
    (coalesce(col("option_price"), lit(0)) * coalesce(col("option_quantity"), lit(1)) * lit(-1))
).otherwise(lit(0))

silver = (
    silver
    .withColumn(
        "gross_line_revenue",
        spark_round(col("item_price") * col("item_quantity"), 2)
    )
    .withColumn(
        "discount_amount_line",
        spark_round(line_discount_amount, 2)
    )
    .withColumn(
        "net_line_revenue",
        spark_round(col("line_total"), 2)
    )
    .withColumn(
        "has_discount_line",
        when(col("option_price") < 0, lit(1)).otherwise(lit(0))
    )
)

# ── STEP 2: Aggregate to order level first ────────────────────────────
# Important so one order is counted once, even if multiple line items exist
print("=== STEP 2: Aggregating to order level ===")

order_level = (
    silver.groupBy("order_date", "order_id")
    .agg(
        spark_max("user_id").alias("user_id"),
        spark_max("is_loyalty").alias("is_loyalty"),
        spark_sum("gross_line_revenue").alias("gross_revenue_order"),
        spark_sum("discount_amount_line").alias("discount_amount_order"),
        spark_sum("net_line_revenue").alias("net_revenue_order"),
        spark_max("has_discount_line").alias("has_discount_order")
    )
)

order_level = order_level.withColumn(
    "discount_status",
    when(col("has_discount_order") == 1, "Discounted")
    .otherwise("Full Price")
)

# ── STEP 3: Aggregate to Gold daily discount cohorts ──────────────────
print("=== STEP 3: Building Gold discount summary ===")

gold_discounts = (
    order_level.groupBy("order_date", "discount_status")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_customers"),
        spark_round(spark_sum("gross_revenue_order"), 2).alias("gross_revenue"),
        spark_round(spark_sum("discount_amount_order"), 2).alias("discount_amount"),
        spark_round(spark_sum("net_revenue_order"), 2).alias("net_revenue")
    )
)

gold_discounts = gold_discounts.withColumn(
    "avg_order_value",
    spark_round(col("net_revenue") / col("total_orders"), 2)
)

gold_discounts = gold_discounts.withColumn("gold_load_ts", current_timestamp())

print(f"Gold discount rows: {gold_discounts.count()}")
print("Discount status distribution:")
gold_discounts.groupBy("discount_status").count().show()

# ── STEP 4: Write Gold Delta ───────────────────────────────────────────
gold_discounts.write.format("delta").mode("overwrite").save(GOLD_DISCOUNTS)

print(f"=== SUCCESS: Gold discounts at {GOLD_DISCOUNTS} ===")
job.commit()


```


# glue_gold_location_performance.py


```python

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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_PATH = "s3://globalpartner-datalake/silver/orders/"
GOLD_LOCATION = "s3://globalpartner-datalake/gold/location_performance/"

print("=== Reading Silver orders ===")
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver rows: {silver.count()}")

# Ensure date column exists
silver = silver.withColumn("order_date", to_date(col("order_ts")))

# Keep only rows with restaurant_id
silver = silver.filter(col("restaurant_id").isNotNull())

# Aggregate at location + day grain
location_perf = (
    silver.groupBy("restaurant_id", "order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_customers"),
        spark_round(spark_sum("line_total"), 2).alias("total_revenue")
    )
)

location_perf = location_perf.withColumn(
    "avg_order_value",
    spark_round(col("total_revenue") / col("total_orders"), 2)
)

location_perf = location_perf.withColumn("gold_load_ts", current_timestamp())

print(f"Gold location rows: {location_perf.count()}")
location_perf.show(10, truncate=False)

location_perf.write.format("delta").mode("overwrite").save(GOLD_LOCATION)

print(f"=== SUCCESS: Gold location_performance at {GOLD_LOCATION} ===")
job.commit()


```

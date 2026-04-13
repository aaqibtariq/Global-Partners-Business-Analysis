# glue_silver_clean_dates.py

```python

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, current_timestamp
)
from pyspark.sql.types import DateType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_DATE = "s3://globalpartner-datalake/bronze/date_dim/"
SILVER_DATE = "s3://globalpartner-datalake/silver/date_dim/"

print("=== STEP 1: Reading Bronze date_dim ===")
dates = spark.read.format("delta").load(BRONZE_DATE)
print(f"Bronze date_dim rows: {dates.count()}")

# ── CONFIRMED from data analysis ──────────────────────────
# Bronze columns (all lowercase):
#   date_key         = "01-01-2023" string (DD-MM-YYYY format)
#   date_key_parsed  = "2023-01-01" string — already parsed during SSMS import
#   year, month, week = integers
#   day_of_week      = string "Sunday", "Monday" etc.
#   is_weekend       = boolean already
#   is_holiday       = boolean already
#   holiday_name     = string, nullable
#   bronze_load_ts, bronze_load_dt, source_table = Bronze metadata

# ── STEP 2: Drop Bronze metadata columns ──────────────────
print("=== STEP 2: Dropping Bronze metadata columns ===")
dates = dates.drop("bronze_load_ts", "bronze_load_dt", "source_table")

# ── STEP 3: Cast date_key_parsed to proper DateType ───────
# Bronze stores it as string "2023-01-01"
# Silver needs proper DateType for joining with order timestamps
print("=== STEP 3: Casting date_key_parsed string → DateType ===")
dates = dates.withColumn(
    "date_key_parsed",
    to_date(col("date_key_parsed"), "yyyy-MM-dd")
)

# Validate: zero nulls after cast
nulls = dates.filter(col("date_key_parsed").isNull()).count()
print(f"date_key_parsed null count after cast: {nulls}")
if nulls > 0:
    # Fallback: try parsing from original date_key DD-MM-YYYY
    print("Attempting fallback parse from date_key column...")
    dates = dates.withColumn(
        "date_key_parsed",
        to_date(col("date_key"), "dd-MM-yyyy")
    )
    nulls2 = dates.filter(col("date_key_parsed").isNull()).count()
    if nulls2 > 0:
        raise Exception(
            f"ABORT: {nulls2} date rows failed to parse. "
            "Check date_key format in Bronze."
        )
    print(f"Fallback parse succeeded — {nulls2} nulls remaining")

# ── STEP 4: Confirm row count ─────────────────────────────
# 2023 is not a leap year so 365 rows is correct
# (366 would be for a leap year)
total = dates.count()
print(f"Total date rows: {total}")
if total < 365:
    raise Exception(
        f"ABORT: Only {total} date rows — expected 365. "
        "Check Bronze date_dim."
    )

# ── STEP 5: Show sample for verification ──────────────────
print("Sample date_dim rows:")
dates.select(
    "date_key", "date_key_parsed", "day_of_week",
    "is_weekend", "is_holiday", "holiday_name"
).show(5, truncate=False)

# ── STEP 6: Add Silver metadata ───────────────────────────
dates = dates.withColumn("silver_load_ts", current_timestamp())

# ── STEP 7: Write to Silver as Delta overwrite ────────────
# date_dim is a reference table — always safe to fully overwrite
# Only 365 rows so overwrite is near-instant
print("=== STEP 7: Writing to Silver as Delta overwrite ===")
dates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_DATE)

print(f"=== SUCCESS: Silver date_dim complete — {total} rows at {SILVER_DATE} ===")
job.commit()



```

# glue_silver_clean_orders.py

```python
import sys, boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, coalesce,
    sum as spark_sum, trim, length, regexp_extract,
    current_timestamp
)
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── Paths ─────────────────────────────────────────────────
BRONZE_ITEMS   = "s3://globalpartner-datalake/bronze/order_items/"
BRONZE_OPTIONS = "s3://globalpartner-datalake/bronze/order_item_options/"
SILVER_PATH    = "s3://globalpartner-datalake/silver/orders/"
QUARANTINE     = "s3://globalpartner-datalake/silver/quarantine/orders/"

print("=== STEP 1: Reading Bronze Delta tables ===")
items   = spark.read.format("delta").load(BRONZE_ITEMS)
options = spark.read.format("delta").load(BRONZE_OPTIONS)

print(f"Bronze order_items rows:   {items.count()}")
print(f"Bronze order_options rows: {options.count()}")

# ── CONFIRMED from data analysis ──────────────────────────
# All columns are lowercase in Bronze Delta
# creation_time_utc = "2023-07-14T15:43:14.394Z" (string)
# creation_time_parsed = "2023-07-14 15:43:14.394 UTC" (string, pre-parsed in SSMS)
# is_loyalty = boolean already
# is_loyalty_bit = boolean already (duplicate — we drop it)
# item_price = float64 — needs cast to decimal(10,2)
# option_price = float64, min=0 max=1, no negatives in this dataset

# ── STEP 2: Drop duplicate/redundant Bronze columns ────────
print("=== STEP 2: Dropping redundant Bronze columns ===")
# is_loyalty_bit is a duplicate of is_loyalty — created during SSMS import
# creation_time_parsed was created during SSMS import — we re-derive from UTC
# bronze_load_ts, bronze_load_dt, source_table are Bronze metadata — not needed in Silver
items = items.drop(
    "is_loyalty_bit",        # duplicate of is_loyalty
    "creation_time_parsed",  # we re-derive from creation_time_utc cleanly
    "bronze_load_ts",
    "bronze_load_dt",
    "source_table"
)
options = options.drop("bronze_load_ts", "bronze_load_dt", "source_table")

# ── STEP 3: Null handling ──────────────────────────────────
print("=== STEP 3: Null handling ===")
# order_id and lineitem_id are join keys — rows without them are unusable
items = items.dropna(subset=["order_id", "lineitem_id"])

# user_id is null for guest orders — fill with GUEST flag
items = items.fillna({"user_id": "GUEST", "printed_card_number": "NONE"})

# ── STEP 4: Deduplication ──────────────────────────────────
print("=== STEP 4: Deduplication ===")
items   = items.dropDuplicates(["order_id", "lineitem_id"])
options = options.dropDuplicates(["order_id", "lineitem_id"])

# ── STEP 5: Parse ISO 8601 timestamp ──────────────────────
# Bronze has: "2023-07-14T15:43:14.394Z"
# Spark format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# Creates a proper Spark TimestampType column
print("=== STEP 5: Parsing creation_time_utc to proper timestamp ===")
items = items.withColumn(
    "order_ts",
    to_timestamp(col("creation_time_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# Validate parse worked
ts_nulls = items.filter(col("order_ts").isNull()).count()
print(f"Timestamp parse failures: {ts_nulls}")
if ts_nulls > 100:
    raise Exception(f"Too many timestamp parse failures: {ts_nulls}")

# ── STEP 6: Fix item_price type (float → decimal) ──────────
# Float causes rounding errors: 5.99 becomes 5.9900000001
# DecimalType(10,2) stores exact currency values
print("=== STEP 6: Casting item_price float → decimal(10,2) ===")
items = items.withColumn(
    "item_price",
    col("item_price").cast(DecimalType(10, 2))
)
options = options.withColumn(
    "option_price",
    col("option_price").cast(DecimalType(10, 2))
)

# ── STEP 7: Data quality flagging ─────────────────────────
# Known valid categories confirmed from data analysis
VALID_CATEGORIES = [
    "BBQ Plates", "Breakfast", "Sandwiches", "Salads", "Smoothies",
    "Bowls", "Drinks", "Chips", "Specialty Coffee Beverages",
    "Drip Coffee", "Snacks", "Alltown Fresh Hot Coffee"
]

print("=== STEP 7: Data quality flagging ===")
items_flagged = items.withColumn("dq_flag",
    when(col("item_category").like("%http%"),  "URL_IN_CATEGORY")
    .when(col("item_category").like("%www.%"), "URL_IN_CATEGORY")
    .when(length(col("item_category")) > 100,  "CATEGORY_TOO_LONG")
    .when(col("item_name").like("%http%"),      "URL_IN_NAME")
    .when(col("item_price").isNull(),           "NULL_PRICE")
    .when(col("item_price") < 0,               "NEGATIVE_PRICE")
    .otherwise("CLEAN")
)

# Auto-fix: extract category name before URL contamination
fixable = (items_flagged
    .filter(col("dq_flag") == "URL_IN_CATEGORY")
    .withColumn("item_category",
        trim(regexp_extract(col("item_category"),
             r"^([A-Za-z &]+?)(?:https?://|www\.)", 1)))
    .withColumn("dq_flag", lit("FIXED")))

# Check which fixed rows now have valid categories
fixed_clean = (fixable
    .filter(col("item_category").isin(VALID_CATEGORIES))
    .drop("dq_flag"))

# Rows we could not fix — send to quarantine
still_dirty = fixable.filter(~col("item_category").isin(VALID_CATEGORIES))
other_dirty = items_flagged.filter(
    (col("dq_flag") != "CLEAN") & (col("dq_flag") != "URL_IN_CATEGORY"))
quarantine_rows = still_dirty.union(other_dirty)

qcount = quarantine_rows.count()
clean_items = items_flagged.filter(col("dq_flag") == "CLEAN").drop("dq_flag")
fixed_count = fixed_clean.count()

print(f"Clean rows:      {clean_items.count()}")
print(f"Auto-fixed rows: {fixed_count}")
print(f"Quarantined rows:{qcount}")

if qcount > 0:
    (quarantine_rows.write
        .format("delta")
        .mode("overwrite")
        .save(QUARANTINE))
    print(f"Quarantine written to {QUARANTINE}")

# Combine clean and auto-fixed
all_clean = clean_items.union(fixed_clean)

# ── STEP 8: Distributed hash join ─────────────────────────
# Joins order_items (~203K) with order_item_options (~193K)
# on composite key: order_id + lineitem_id
# LEFT join: keep all order_items even if they have no options
print("=== STEP 8: Distributed hash join order_items + order_item_options ===")
joined = all_clean.join(options, ["order_id", "lineitem_id"], "left")

# ── STEP 9: Revenue calculation ───────────────────────────
# LINE_TOTAL = (item_price × item_quantity) + (option_price × option_quantity)
# option_price is always >= 0 in this dataset (confirmed in data analysis)
# coalesce handles NULLs from left join (rows with no matching options)
print("=== STEP 9: Calculating LINE_TOTAL revenue ===")
joined = joined.withColumn(
    "line_total",
    (col("item_price") * col("item_quantity")) +
    coalesce(col("option_price") * col("option_quantity"), lit(0.0))
)

# ── STEP 10: Add Silver metadata ───────────────────────────
joined = joined.withColumn("silver_load_ts", current_timestamp())

output_count = joined.count()
print(f"=== STEP 10: Silver output rows: {output_count} ===")

# ── STEP 11: Data quality gate ────────────────────────────
ssm = boto3.client("ssm", region_name="us-east-1")
try:
    prev = int(ssm.get_parameter(
        Name="/gp/silver/orders/last_row_count")["Parameter"]["Value"])
    drop_pct = (prev - output_count) / prev * 100
    print(f"Row count: prev={prev}, current={output_count}, drop={drop_pct:.1f}%")
    if output_count < prev * 0.9:
        raise Exception(
            f"QUALITY GATE FAILED: {output_count} rows vs {prev} previous "
            f"(drop of {drop_pct:.1f}% exceeds 10% threshold)"
        )
except ssm.exceptions.ParameterNotFound:
    print("First run — no baseline. Storing current count.")

ssm.put_parameter(
    Name="/gp/silver/orders/last_row_count",
    Value=str(output_count), Type="String", Overwrite=True
)

# ── STEP 12: Delta MERGE to Silver ────────────────────────
# MERGE = upsert: update existing rows, insert new rows
# Safe to rerun — no duplicates created
print("=== STEP 12: Writing to Silver via Delta MERGE ===")
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    dt = DeltaTable.forPath(spark, SILVER_PATH)
    (dt.alias("old")
       .merge(joined.alias("new"),
              "old.order_id = new.order_id AND old.lineitem_id = new.lineitem_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())
    print("Delta MERGE complete")
else:
    joined.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print("First run — Delta overwrite complete")

print(f"=== SUCCESS: Silver orders complete — {output_count} rows at {SILVER_PATH} ===")
job.commit()


```

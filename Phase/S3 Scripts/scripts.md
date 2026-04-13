# glue_ingest_order_items.py

```python

import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, current_date, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Config
BRONZE_PATH = "s3://globalpartner-datalake/bronze/order_items/"
SECRET_NAME = "globalparnter_secret"
DATABASE_NAME = "GlobalPartners"

print("Starting Glue job...")

# Get credentials from Secrets Manager
sm = boto3.client("secretsmanager", region_name="us-east-1")
creds = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"])

jdbc_url = (
    f"jdbc:sqlserver://{creds['host']}:{creds['port']};"
    f"databaseName={DATABASE_NAME};"
    "encrypt=true;trustServerCertificate=true"
)

print(f"Connecting to: {creds['host']}")
print(f"Database: {DATABASE_NAME}")

# Read full snapshot from SQL Server
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.order_items")
    .option("user", creds["username"])
    .option("password", creds["password"])
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("numPartitions", "4")
    .load()
)

row_count = df.count()
print(f"Rows read from SQL Server: {row_count}")

# Optional Bronze audit columns
df = (
    df.withColumn("bronze_load_ts", current_timestamp())
      .withColumn("bronze_load_dt", current_date())
      .withColumn("source_table", lit("order_items"))
)

# Write full snapshot to Bronze as Delta
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — full snapshot written to {BRONZE_PATH}")

job.commit()

```

# glue_ingest_order_item_options.py

```python

import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, current_date, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Config
BRONZE_PATH = "s3://globalpartner-datalake/bronze/order_item_options/"
SECRET_NAME = "globalparnter_secret"
DATABASE_NAME = "GlobalPartners"

print("Starting Glue job for order_item_options...")

# Get credentials
sm = boto3.client("secretsmanager", region_name="us-east-1")
creds = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"])

jdbc_url = (
    f"jdbc:sqlserver://{creds['host']}:{creds['port']};"
    f"databaseName={DATABASE_NAME};"
    "encrypt=true;trustServerCertificate=true"
)

print(f"Connecting to: {creds['host']}")
print(f"Database: {DATABASE_NAME}")

# Read full snapshot
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.order_item_options")
    .option("user", creds["username"])
    .option("password", creds["password"])
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("numPartitions", "4")
    .load()
)

row_count = df.count()
print(f"Rows read from SQL Server: {row_count}")

# Optional Bronze audit columns
df = (
    df.withColumn("bronze_load_ts", current_timestamp())
      .withColumn("bronze_load_dt", current_date())
      .withColumn("source_table", lit("order_item_options"))
)

# Overwrite Bronze with full snapshot
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — full snapshot written to {BRONZE_PATH}")

job.commit()

```


# glue_ingest_date_dim.py

```python

import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, current_date, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Config
BRONZE_PATH = "s3://globalpartner-datalake/bronze/date_dim/"
SECRET_NAME = "globalparnter_secret"
DATABASE_NAME = "GlobalPartners"

print("Starting Glue job for date_dim...")

# Get credentials
sm = boto3.client("secretsmanager", region_name="us-east-1")
creds = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"])

jdbc_url = (
    f"jdbc:sqlserver://{creds['host']}:{creds['port']};"
    f"databaseName={DATABASE_NAME};"
    "encrypt=true;trustServerCertificate=true"
)

print(f"Connecting to: {creds['host']}")
print(f"Database: {DATABASE_NAME}")

# Read full snapshot
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.date_dim")
    .option("user", creds["username"])
    .option("password", creds["password"])
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("numPartitions", "4")
    .load()
)

row_count = df.count()
print(f"Rows read from SQL Server: {row_count}")

# Optional audit columns
df = (
    df.withColumn("bronze_load_ts", current_timestamp())
      .withColumn("bronze_load_dt", current_date())
      .withColumn("source_table", lit("date_dim"))
)

# Overwrite Bronze snapshot
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — full snapshot written to {BRONZE_PATH}")

job.commit()

```

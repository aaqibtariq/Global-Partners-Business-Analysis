
# glue_ingest_date_dim.py

```python
import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Read from SQL Server
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

# Write to Bronze as Delta
(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — written to {BRONZE_PATH}")

job.commit()
```
# glue_ingest_order_item_options.py

```python
import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Read from SQL Server
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

# Write to Bronze as Delta
(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — written to {BRONZE_PATH}")

job.commit()
```
# glue_ingest_order_items.py

```python
import sys, boto3, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Read from SQL Server
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

# Write to Bronze as Delta
(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Done — written to {BRONZE_PATH}")

job.commit()
```

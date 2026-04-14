
# Step 1 Create Folder in S3

- open s3 -> select globalpartner-datalake Buckets
- Create new Folder silver


# Step 2 - upload Scripts 

Upload scripts  to Amazon S3 Buckets -> globalpartner-glue-scripts -> silver/

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/silver%20scripts.png" width="750"/> </p>

# Step 3 Add new permissions to IAM Role

Incline new policy GlueReadWriteGpSsmParameters

```json

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "GlueReadWriteGpSsmParameters",
			"Effect": "Allow",
			"Action": [
				"ssm:GetParameter",
				"ssm:PutParameter"
			],
			"Resource": "arn:aws:ssm:us-east-1:****:parameter/gp/*"
		}
	]
}

```
# step 4 Create endpoint VPC


Create Endpoint

- Name tag - SSM endpoint
- Type -> AWS service
- Service -> com.amazonaws.us-east-1.ssm Interface
- Network settings -> VPC -> Select same the one you used in RDS default or the one you created
- Private DNS name -> Enabled
- DNS record IP type -> IPv4
- Subnets -> Select the subnet you selected for route table or select multiple
- Security groups -> Select same the one you used in RDS default or the one you created
- Policy -> Full access
- Create endpoint

### SSM / Endpoint Configuration

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/silve%20SSM%20endpoint.png" width="750"/> </p>

# Step 5 Glue Job setup

We have to create 2 jobs

- glue_silver_clean_dates

- glue_silver_clean_orders

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/Silver%20glue%20%20jobs.png" width="750"/> </p>

## glue_silver_clean_orders

- Click Create job

- Open Job details

- Name - > glue_silver_clean_orders

- Description - optional -> BSilver cleanup — reads Bronze order_items and order_item_options Delta, cleans, joins, calculates line_total, writes Silver Delta to S3

- IAM Role -> The one you use in Glue Connection

- Type -> Spark

- Glue version -> Glue 4.0 - Support spark 3.3, Scala 2, Python 3

- Language -> Python 3

- Worker type -> G 1X

- Automatically scale the number of workers -> Uncheck

- Requested number of workers -> 2

- Generate job insights -> Check -> AWS Glue will analyze your job runs and provide insights on how to optimize your jobs and the reasons for job failures.

- Job bookmark -> Disable

- Job Run Queuing -> Uncheck

- Flex execution -> Uncheck

- Number of retries -> 2

- Job timeout (minutes) -> 60

- Advanced Propeties

- Script filename -> Important Use same as the one you have in S3 the name of script -> glue_silver_clean_orders.py

- Script path -> Script path -> s3://globalpartner-glue-scripts/silver/

- Enabled Job metrics, Job observability metrics, Continuous logging

- Spark UI logs path -> s3://globalpartner-glue-scripts/sparkHistoryLogs/

- Spark UI logging and monitoring configuration -> Standard - default

- Maximum concurrency -> 1

- Temporary path -> s3://globalpartner-glue-scripts/temporary/

- Delay notification threshold (minutes) -> none

- Security configuration -> None

- Server-side encryption -> uncheck

- Use Glue data catalog as the Hive metastore -> check

- Connections -> Dont need as we wil use S3 Bronze layer

- Libraries

	-	Python library path -> none
	-	Dependent JARs path -> None
	-	Referenced files path -> none
	-	Additional Python modules path -> none

- Job parameters
	-	Key -> --datalake-formats Value -> delta
	-	Key -> --conf  Value -> spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
	-	Key -> --conf Value ->  spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
	-	Key -> --conf Value ->  spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore


- Save
- Before running please check your scripts and then run

### Glue Job 

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_orders%201.png" width="750"/> </p>

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_orders%202.png" width="750"/> </p>


<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_orders%203.png" width="750"/> </p>


<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_orders%204.png" width="750"/> </p>


<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_orders%205.png" width="750"/> </p>

### Glue job result

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/glue_silver_clean_dates%20run%20results.png" width="750"/> </p>

### Result in S3

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Silver/Silver%20S3%20results.png" width="750"/> </p>


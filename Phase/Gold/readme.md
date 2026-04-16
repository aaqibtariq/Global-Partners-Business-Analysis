


# Step 1 Create Folder in S3

- open s3 -> select globalpartner-datalake Buckets
- Create new Folder gold


# Step 2 - upload Scripts 

Upload scripts  to Amazon S3 Buckets -> globalpartner-glue-scripts -> gold/

# Step 3 - Glue job

## glue_gold_sales

- Click Create job

- Open Job details

- Name - > glue_gold_sales

- Description - optional -> 

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

- Script filename -> Important Use same as the one you have in S3 the name of script -> glue_gold_sales.py

- Script path -> Script path -> s3://globalpartner-glue-scripts/gold/

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

**Repeat same for all other glue job, clone this and change name and script name and run**


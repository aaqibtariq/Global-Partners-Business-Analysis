# AWS Glue Connection Setup (SQL Server – Global Partners)

## In case you need to uplaod drivers 

-  Download from Microsoft website 
https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server?view=sql-server-ver17

- Extract and upload  mssql-jdbc-12.4.2.jre11.jar fiel to S3 globalpartner-glue-scripts

# Create Glue Connection

- **Before setting up this make sure your VPC configrations are correct**
- **Glue Security Group, Subnet, Endpoints should be linked properly same as RDS**
- **Make sure your role has permissions EC2 Full and GlueServiceRole**
  

# Glue connection Setup


- Open AWS Glue -> Click connections -> Create Connection
- Choose data source -> Microsoft SQL Server -> next
- Database instances -> Your RDS database select that
- Database name -> The one you created in SSMS
- Credential type -> AWS Sewcret Manager and select your secret name
- IAM service role -> The one which has all permissions as mentioned above
- Network options
    -  VPC -> Same as RDS
    -  Subnet -> in Subnets Configuration we had 6 subnets available, select the one which you select in the route as Explicit subnet associations
    -  Security groups -> same as RDS
- Click next
- Name -> Sqlserver connection
- Click next and revew and create connection
- Once created, select and click action and test connection

##  AWS Glue Connection – RDS Integration

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20connection.png" width="750"/>
</p>



# Glue Jobs

We have to create 3 jobs
- glue_ingest_date_dim
- glue_ingest_order_items
- glue_ingest_order_item_options

  ##  AWS Glue Jobs – ETL Pipeline Overview

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/Glue%20jobs.png" width="750"/>
</p>

### glue_ingest_date_dim

- Click Create job
- Open Job details
- Name - > glue_ingest_date_dim
- Description - optional -> Bronze ingestion — reads date_dim from SQL Server, writes Delta to S3
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
- Number of retries -> 3
- Job timeout (minutes) -> 60
- Advanced Propeties
- Script filename -> **Important** Use same as the one you have in S3 the name of script -> glue_ingest_date_dim.py
- Script path -> Script path -> s3://globalpartner-glue-scripts/bronze/
- Enabled Job metrics, Job observability metrics, Continuous logging
- Spark UI logs path -> s3://globalpartner-glue-scripts/sparkHistoryLogs/
- Spark UI logging and monitoring configuration -> Standard - default
- Maximum concurrency -> 1
- Temporary path -> s3://globalpartner-glue-scripts/temporary/
- Delay notification threshold (minutes) -> none
- Security configuration -> None
- Server-side encryption -> uncheck
- Use Glue data catalog as the Hive metastore -> check
- Connections -> **Important** MNake sure to select proper connection otherwise Glue job will not get data if nothing there no worries We will manually setup
- Libraries
    - Python library path -> none
    - Dependent JARs path -> s3://globalpartner-glue-scripts/sqljdbc_13.4/enu/jars/mssql-jdbc-13.4.0.jre11.jar
    - Referenced files path -> none
    - Additional Python modules path -> none
- Job parameters
    - Key -> --conf    Value -> spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.eventLog.rolling.enabled=true --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
    - Key ->   --datalake-formats  Value -> delta
    - Key ->  --datalake_bucket    Value -> globalpartner-datalake
    - Key ->  --secret_name        Value -> globalparnter_secret

- Save ( Don't Run Yet)

###  Glue Job Configuration 

 <p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue_ingest_date_dim%201.png" width="750"/>
</p>


###  Data Source Configuration (RDS → Glue)

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue_ingest_date_dim%202.png" width="750"/>
</p>


###  Transformation Logic

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue_ingest_date_dim%203.png" width="750"/>
</p>
 
###  Target Configuration (Glue → S3)

<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue_ingest_date_dim%204.png" width="750"/>
</p>

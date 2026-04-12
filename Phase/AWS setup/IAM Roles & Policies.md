
# Step 1 Create the Glue role

- Go to:
- IAM → Roles → Create role → AWS service → Glue
- Use this name:
    - GlueExecutionRole-GlobalPartners
- Attached policy AWSGlueServiceRole, EC2FullAccess
- Save
- Add below incline policy - GlueExecutionPolicy
- Check Trust relationships


```josn

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "S3DatalakeAccess",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:ListBucket"
			],
			"Resource": [
				"arn:aws:s3:::globalpartner-datalake",
				"arn:aws:s3:::globalpartner-datalake/*",
				"arn:aws:s3:::globalpartner-glue-scripts",
				"arn:aws:s3:::globalpartner-glue-scripts/*",
				"arn:aws:s3:::globalpartner-athena-results",
				"arn:aws:s3:::globalpartner-athena-results/*"
			]
		},
		{
			"Sid": "GlueAndLogs",
			"Effect": "Allow",
			"Action": [
				"glue:*",
				"logs:*",
				"cloudwatch:PutMetricData",
				"ssm:GetParameter",
				"ssm:PutParameter"
			],
			"Resource": "*"
		},
		{
			"Sid": "SecretsManager",
			"Effect": "Allow",
			"Action": [
				"secretsmanager:GetSecretValue"
			],
			"Resource": "arn:aws:secretsmanager:*:*:secret:globalpartner_secret*"
		},
		{
			"Sid": "KMS",
			"Effect": "Allow",
			"Action": [
				"kms:Decrypt",
				"kms:GenerateDataKey",
				"kms:DescribeKey"
			],
			"Resource": "*"
		}
	]
}

```
## Trust relationships

```json

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

```


# Step 2 Setup S3 buckets 

**Create S3 buckets to support:**

- Data lake storage
- Glue scripts
- Athena query results

**Buckets Created**

- globalpartner-datalake	-> Stores raw and processed data
- globalpartner-glue-scripts	-> Stores AWS Glue ETL scripts
- globalpartner-athena-results	-> Stores Athena query outputs

**Steps to Create S3 Buckets**

- Navigate to S3
- Go to AWS Console
- Search for S3
- Click Create bucket
- Enter name
- Select: US East (N. Virginia) – us-east-1
- Keep “Block all public access” ENABLED
- Enable Server-side encryption (SSE-S3)
- Click Create bucket


# Step 3 After creating RDS setup AWS Secrets Manager

- Open AWS Secrets Manager
- Click Store a new secret
- Select Credentials for Amazon RDS Database
- Under Credentials -> Enter your RDS admin username and password
- Select DB instance the one you created in RDS
- Give proper name Secret name and next
- Nothing to do in Configure rotation - optional
- Review and click store
- Open your secret and Retrieve secret value
- Click Edit
- Add Engine key and value sqlserver
- Add host key and value your rds Endpoint link globalpartners-db.xxxxxxxxx.us-east-1.rds.amazonaws.com
- Add port key and value 1433
- Add dbInstanceIdentifier and value your rds DB instance name
- Click Save

##  AWS Secrets Manager – Secure Credential Storage


<p align="center">
  <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/aws%20secret.png" width="750"/>
</p>



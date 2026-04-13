
# Step 1 Create Folder in S3

- open s3 -> select globalpartner-datalake Buckets
- Create new Folder silver


# Step 2 - upload Scripts 

Upload scripts  to Amazon S3 Buckets -> globalpartner-glue-scripts -> silver/

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

# Step 4

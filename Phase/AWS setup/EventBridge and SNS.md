# STEP 1 — Create EventBridge Rule

- Go to:  AWS Console → EventBridge
- Click:  Rules → Create rule
- STEP 2 — Configure Rule
- Name: globalpartner-daily-pipeline
- Rule type: Schedule
-  STEP 3 — Set Schedule
- Choose: Cron expression
- Example (run daily at midnight UTC):
- cron(0 0 * * ? *) If you want EST 8 PM (example):
- cron(0 1 * * ? *)
- (UTC conversion)
-  STEP 4 — Select Target
- Target type: AWS service
- Then choose:
-  Glue → Glue Workflow
- Select: Your workflow name
- (e.g., globalpartner_workflow)
- STEP 5 — Permissions
- EventBridge will ask for role:
-  Choose: Create new role automatically 
- OR existing role with:
- glue:StartWorkflowRun
- STEP 6 — Create Rule
- Click: Create

 # Create Role

- Role name: EventBridgeInvokeGlueWorkflowRole
- Trust Policy EventBridge rule

 ```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

```

- Permissions policy

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "glue:StartWorkflowRun",
      "Resource": "arn:aws:glue:us-east-1:440222523928:workflow/globalpartner_workflow"
    }
  ]
}

```


# SNS

- Create an SNS topic like: globalpartner-workflow-alerts
- Protocol: Email
- Endpoint: your email
- After that, confirm the email from your inbox.
- SNS topic policy should allow EventBridge to do: sns:Publish
```

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEventBridgePublish",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:us-east-1:440222523928:globalpartner-workflow-alerts"
    }
  ]
}


```


# SNS

##  STEP 1 — Create SNS Topic
- Go to AWS Console → SNS
- Click Create topic
- Choose:
- Type: Standard

- Name: globalpartner-workflow-alerts
- Click Create topic

 ## STEP 2 — Add Email Subscription

- Open your topic
- Click Create subscription
- Fill:
  - Protocol: Email
  - Endpoint: your email
- Click Create subscription
-  Go to your email → click Confirm subscription

## STEP 3 — Add SNS Topic Policy 
- Inside your SNS topic
- Go to Access policy
- Click Edit
- Replace with this:
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEventBridgePublish",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:us-east-1:440222523928:globalpartner-workflow-alerts"
    }
  ]
}

```
- Click Save changes

## STEP 4 — Create EventBridge Rule for Glue Failure Alerts

- Now we connect SNS to failures.
- Go to EventBridge
- Click Create rule
- Basic config:
- Name:
- glue-workflow-failure-alert
- Rule type: Rule with event pattern

## STEP 5 — Event Pattern (Glue Failure)

- Select:
- Event source: AWS services
- Service: Glue
- Event type: Glue Job State Change
- Then choose:
- State:
  - FAILED
  - TIMEOUT
  - STOPPED
- or
```
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"],
  "detail": {
    "state": ["FAILED", "TIMEOUT", "STOPPED"]
  }
}

```

## STEP 6 — Add Target (SNS)

- Click Next
- Target type: AWS service
- Target: SNS topic
- Select: globalpartner-workflow-alerts

## STEP 7 — Configure Input (Message)

- Choose:  Input transformer (recommended)
- Then:
```
Input path:

{
  "jobName": "$.detail.jobName",
  "state": "$.detail.state",
  "time": "$.time"
}

```
- Template:

```

Glue Job Failed 

Job Name: <jobName>
Status: <state>
Time: <time>

```

## STEP 8 — Create Rule

- Click Create rule

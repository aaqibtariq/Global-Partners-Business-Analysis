# Setting up Glue Workflow using bash

## Create workflow

```

aws glue create-workflow \
  --name globalpartner_workflow \
  --description "Global Partners Bronze Silver Gold workflow"

```

## Create Bronze start trigger
This is the root trigger

```
aws glue create-trigger \
  --name bronze_trigger \
  --workflow-name globalpartner_workflow \
  --type ON_DEMAND \
  --actions '[{"JobName":"glue_ingest_order_items"},{"JobName":"glue_ingest_order_item_options"},{"JobName":"glue_ingest_date_dim"}]'

```

## Create Silver trigger

Each condition needs LogicalOperator:"EQUALS".

```
aws glue create-trigger \
  --name silver_trigger \
  --workflow-name globalpartner_workflow \
  --type CONDITIONAL \
  --predicate '{"Logical":"AND","Conditions":[{"LogicalOperator":"EQUALS","JobName":"glue_ingest_order_items","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_ingest_order_item_options","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_ingest_date_dim","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"glue_silver_clean_orders"},{"JobName":"glue_silver_clean_dates"}]' \
  --start-on-creation

```

## Create Gold trigger

```
aws glue create-trigger \
  --name gold_trigger \
  --workflow-name globalpartner_workflow \
  --type CONDITIONAL \
  --predicate '{"Logical":"AND","Conditions":[{"LogicalOperator":"EQUALS","JobName":"glue_silver_clean_orders","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_silver_clean_dates","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"glue_gold_sales"},{"JobName":"glue_gold_daily_clv"},{"JobName":"glue_gold_rfm"},{"JobName":"glue_gold_churn"},{"JobName":"glue_gold_loyalty"},{"JobName":"glue_gold_discounts"},{"JobName":"glue_gold_location_performance"}]' \
  --start-on-creation

```

## Get crawler name

```
aws glue get-crawlers --query 'Crawlers[].Name'

```
## Create Crawler trigger
Replace YOUR_CRAWLER_NAME:

```

aws glue create-trigger \
  --name crawler_trigger \
  --workflow-name globalpartner_workflow \
  --type CONDITIONAL \
  --predicate '{"Logical":"AND","Conditions":[{"LogicalOperator":"EQUALS","JobName":"glue_gold_sales","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_daily_clv","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_rfm","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_churn","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_loyalty","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_discounts","State":"SUCCEEDED"},{"LogicalOperator":"EQUALS","JobName":"glue_gold_location_performance","State":"SUCCEEDED"}]}' \
  --actions '[{"CrawlerName":"YOUR_CRAWLER_NAME"}]' \
  --start-on-creation

```

## Verify triggers


```
aws glue get-triggers --query 'Triggers[?WorkflowName==`globalpartner_workflow`].[Name,Type,State]'
```

## Start the workflow

```
aws glue start-workflow-run --name globalpartner_workflow
```


## Check run status

```
aws glue get-workflow-run --name globalpartner_workflow --run-id YOUR_RUN_ID
```


## AWS Glue Workflow – Orchestration 

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20workflow%201.png" width="750"/> </p>

## AWS Glue Workflow – Orchestration 

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20workflow%202.png" width="750"/> </p>

## AWS Glue Workflow – Orchestration 

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20workflow%203.png" width="750"/> </p>

## AWS Glue Workflow – Orchestration 

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/Phase/Reference%20Files/glue%20workflow%204.png" width="750"/> </p>

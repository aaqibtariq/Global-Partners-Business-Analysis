
**Source (SQL Server):**

Data is pulled daily from three core tables (orders, order options, and date dimension) via JDBC.

**Bronze Layer (Raw Ingestion):**

Data is ingested into S3 as-is using AWS Glue and stored in Parquet format. This serves as an immutable raw copy of the source.

**Silver Layer (Data Cleaning):**

Data is cleaned and standardized (handling nulls, deduplication, timestamp fixes, schema enforcement). A data quality check ensures no major data loss before proceeding.

**Gold Layer (Business Metrics):**

Processed into 7 analytics tables (e.g., CLV, RFM, churn, sales trends, loyalty, location performance, discounts) to support business insights.

**Serving Layer:**

Amazon Athena is used for querying the data, and a Streamlit dashboard visualizes insights. Data is queried via Athena (not directly from S3).

**Orchestration & Monitoring:**

EventBridge triggers the pipeline daily, Step Functions manages workflow and retries, CloudWatch handles logging/metrics, and SNS sends alerts on failures.

**CI/CD:**

GitHub Actions automates testing and deployment of pipeline updates.

**End Output:**

A Streamlit dashboard provides interactive insights across key business metrics.

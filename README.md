# Global Partners Data Engineering Project


# Objective 

This project demonstrates an end-to-end modern data engineering pipeline on AWS, designed to ingest, transform, and serve business data using a medallion architecture (Bronze → Silver → Gold).

The pipeline processes transactional data from a SQL Server database and builds analytical datasets for reporting and business insights using AWS-native services.


# Core Objective

The main objective of this project is to:

- Build a scalable and cost-efficient data pipeline
- Implement data lake architecture using AWS S3 + Delta Lake
- Enable analytical querying using AWS Athena
- Apply best practices in orchestration, monitoring, and automation

# Project Goal

- Automate ingestion from SQL Server → AWS S3
- Clean and transform raw data into analytics-ready datasets
- Design a production-grade workflow using AWS Glue & EventBridge
- Enable business-level insights through Gold layer datasets

**This project is really solving three business problems at once.**

- Problem 1: Customer value visibility
  -  The company wants to know how much each customer is worth and how that changes daily. That is the CLV/LTV requirement.
- Problem 2: Customer lifecycle management
  - The company wants to segment customers and detect churn risk using RFM and inactivity behavior.
- Problem 3: Store and promotion performance
  - The company wants to compare stores, loyalty behavior, time patterns, and promotion effects.

- So the project is both:
  - customer analytics
  - business performance analytics
 
  #  Abstract

This project simulates a real-world enterprise data pipeline where raw operational data is ingested, transformed, and served for analytics.
The system leverages AWS services such as:

- AWS Glue for ETL
- Amazon S3 for storage
- AWS Athena for querying
- EventBridge for orchestration
- SNS for notifications
- Ec2 for Compute
- Streamlit for Visualization

The architecture ensures data reliability, scalability, and performance, following industry-standard design patterns.

# Technical Architecture

```
SQL Server (RDS)
        ↓
AWS Glue (Bronze Ingestion)
        ↓
S3 (Bronze - Delta)
        ↓
AWS Glue (Silver Transformation)
        ↓
S3 (Silver - Cleaned Data)
        ↓
AWS Glue (Gold Aggregation)
        ↓
S3 (Gold - Business Metrics)
        ↓
AWS Athena
        ↓
Analytics / Reporting in streamlit

```
# **End-to-End System Design – Data Pipeline Architecture**

<p align="center"> <img src="https://raw.githubusercontent.com/aaqibtariq/Global-Partners-Business-Analysis/main/System%20Design/SD.png" width="800"/> </p>


# Architecture Components

- Source: SQL Server (AWS RDS)
- Ingestion: AWS Glue (JDBC connection)
- Storage: Amazon S3 (Delta format)
- Processing: AWS Glue (PySpark)
- Orchestration: AWS Glue Workflow + EventBridge
- Monitoring: CloudWatch + SNS
- Query Layer: AWS Athena
- Compute / App Layer: EC2
- Visualization / BI Layer: Streamlit Dashboard


# Key Metrics

This pipeline enables analysis such as:

- Total Orders
- Revenue Calculation
- Item-Level Performance
- Option-Level Revenue Impact
- Daily Trends via Date Dimension

**From Dashboard**

- Total Revenue: ~$10M
- Total Orders: 171K
- Customers: 20K
- At-Risk Customers: 17K
- VIP Customers: 1,119

# Project Phases

## Phase 1: Data Ingestion (Bronze Layer)

-  Setup AWS Permissions
    - [IAM Roles & Policies Setup](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/IAM%20Roles%20%26%20Policies.md)
-  Created SQL Server database in AWS RDS
    - [RDS and SSMS Setup](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/RDS%20and%20SSMS%20setup.md)
-  Loaded raw CSV data into SQL Server
-  Configured AWS Glue connection (JDBC)
    - [Bronze Layer – AWS Glue Setup](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/Bronze%20Glue%20Setup.md)
-  Built Glue jobs to extract data into S3
-  Stored data in Delta format
    - [S3 Scripts – Bronze Layer](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/S3%20Scripts/Bronze%20scripts.md)
- Created Database to test the result
    - [Athena – Bronze Phase Queries](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Amazon%20Athena/Bronze%20phase.md)
    - [Bronze Phase Results (Athena Output)](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Amazon%20Athena/Bronze%20phase%20result)



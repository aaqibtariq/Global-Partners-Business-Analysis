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
 
    - [Project Objectives & Business Goals](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Objectives%20and%20output.md)
 
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
- Athena created Database to test the result
    - [Athena – Bronze Phase Queries](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Amazon%20Athena/Bronze%20phase.md)
    - [Bronze Phase Results (Athena Output)](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Amazon%20Athena/Bronze%20phase%20result)

## Phase 2: Data Transformation (Silver Layer)
- Setup Silver Layer
    -  [Silver Layer Documentation](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Silver/readme.md)  
-  Glue Transformation
    -  [Glue Transformation Scripts](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/S3%20Scripts/Silver%20Scripts.md)  
-  Athena created Database to test the result
    -  [Athena Queries – Silver Layer](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Amazon%20Athena/Silver%20phase.md)  
    -  [Athena Results – Silver Layer](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Amazon%20Athena/silver%20phase%20result)
- All setup reference Files
    -  [Reference Files (Outputs & Screenshots)](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Reference%20Files/Silver)  

# Phase 3: Data Aggregation (Gold Layer)

- Setup Gold Layer
    -  [Gold Layer Documentation](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Gold)  
-  Glue Transformation
    -  [Gold Layer Transformation Scripts](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/S3%20Scripts/Gold%20Scripts.md)  
-  Athena created Database to test the result
    -  [Athena Queries – Gold Layer](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/Amazon%20Athena/Gold%20Phase.md)  
    -  [Athena Results – Gold Layer](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Amazon%20Athena/Gold%20phase%20result)  

# phase 5: Streamlit & Orchestration & Automation

-  Amazon EC2: Hosts the Streamlit application and manages query execution
        - [EC2 Setup & Configuration](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/EC2.md)
-  Glue Workflow Bronze → Silver → Gold flow
        - [AWS Glue Workflows](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/Glue%20Workflows.md)
-  Event Rule & SNS: Configured EventBridge for scheduling and Integrated SNS for notifications
        - [EventBridge Scheduling & SNS Alerts](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/blob/main/Phase/AWS%20setup/EventBridge%20and%20SNS.md)  
-  Streamlit
        - [Streamlit Dashboard Application](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Streamlit)
-  Streamlit Dashboard: Provides an interactive interface for business users to explore insights such as:
    -  Customer lifetime value (CLV)
    -  RFM segmentation
    -  Churn risk
    -  Sales trends
    -  Store performance

The application queries data from AWS Athena, ensuring a serverless and scalable analytics experience.

- **[Analytics Dashboard (Streamlit / Visualization)](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Dashboard)**


- **Reference Files**
      -  [All Reference Files (Screenshots, Outputs, Configurations)](https://github.com/aaqibtariq/Global-Partners-Business-Analysis/tree/main/Phase/Reference%20Files)


  **End**



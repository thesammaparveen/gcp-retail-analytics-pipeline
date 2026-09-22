
# 🛒 GCP Retail Analytics Data Engineering Pipeline

An end-to-end **Retail Analytics Data Engineering Pipeline** built on **Google Cloud Platform (GCP)**.

This project demonstrates how raw retail data can be ingested, stored, validated, transformed, processed using distributed computing, loaded into a cloud data warehouse, analyzed using SQL, orchestrated using Apache Airflow, integrated with event-driven services, archived for future reference, and finally visualized through a business intelligence dashboard.

---

## 📌 Project Overview

The project is designed around a retail analytics use case where three major datasets are processed:

- Sales Data
- Customer Data
- Inventory Data

The pipeline follows a multi-stage architecture using Google Cloud services.

Raw CSV files are first stored in **Google Cloud Storage (GCS)**. The data is then processed using **PySpark on Google Cloud Dataproc**. The processed data is stored in a curated GCS layer and loaded into **Google BigQuery** for analytical processing.

**Cloud Composer with Apache Airflow** is used to orchestrate the pipeline through sensors, validation, branching, Dataproc processing, BigQuery operations, Pub/Sub notifications and archival.

Finally, **Looker Studio** is used to create a retail analytics dashboard.

---

## 🛠️ Tech Stack

`Google Cloud Platform (GCP)` `Google Cloud Storage (GCS)` `Apache Spark` `PySpark` `Google Cloud Dataproc` `BigQuery` `Cloud Composer` `Apache Airflow` `Pub/Sub` `Cloud Functions` `Looker Studio` `Python` `SQL` `Cloud Shell`

---

# 🏗️ Architecture

```text
                         RETAIL SOURCE DATA
                               |
                               v
                  +---------------------------+
                  |   Google Cloud Storage    |
                  |      Landing Zone         |
                  +---------------------------+
                               |
                               v
                  +---------------------------+
                  | Cloud Composer / Airflow  |
                  |                           |
                  | GCS Sensors               |
                  | File Validation           |
                  | Branching                 |
                  +---------------------------+
                               |
                               v
                  +---------------------------+
                  |   Google Cloud Dataproc   |
                  |       PySpark ETL         |
                  +---------------------------+
                               |
                               v
                  +---------------------------+
                  |   GCS Curated Zone       |
                  |      Parquet Data        |
                  +---------------------------+
                               |
                               v
                  +---------------------------+
                  |        BigQuery           |
                  |                           |
                  | Fact & Dimension Tables   |
                  | Analytical Queries        |
                  +---------------------------+
                               |
                  +------------+-------------+
                  |                          |
                  v                          v
        +-------------------+       +-------------------+
        |   Looker Studio   |       |     Pub/Sub       |
        |    Dashboard      |       | Pipeline Status   |
        +-------------------+       +-------------------+
                                             |
                                             v
                                  +-------------------+
                                  |  Cloud Function   |
                                  |   Notification    |
                                  +-------------------+
                                             |
                                             v
                                  +-------------------+
                                  |    GCS Archive    |
                                  +-------------------+
````

---

# 🎯 Project Objectives

The main objectives of this project are:

* Build an end-to-end cloud data engineering pipeline.
* Store raw retail data in a cloud data lake.
* Process large-scale data using PySpark.
* Use Dataproc for managed Spark processing.
* Create curated datasets in GCS.
* Store analytical data in BigQuery.
* Implement fact and dimension tables.
* Use partitioning and clustering in BigQuery.
* Orchestrate pipeline tasks using Apache Airflow.
* Implement file availability sensors.
* Implement validation and branching.
* Send pipeline status using Pub/Sub.
* Trigger a Cloud Function from Pub/Sub.
* Archive source data.
* Build a business dashboard using Looker Studio.

---

# 📂 Source Data

The project works with three primary retail datasets.

### Sales Data

Contains sales transaction information such as:

```text
sale_id
store_id
product_id
customer_id
quantity
sale_amount
sale_date
```

### Customer Data

Contains customer information such as:

```text
customer_id
name
city
state
membership
```

### Inventory Data

Contains product and inventory information such as:

```text
product_id
product_name
category
stock_quantity
```

---

# ☁️ Google Cloud Storage Architecture

Google Cloud Storage is used as the project's cloud data lake.

The project uses separate storage zones for different stages of the data lifecycle.

## 1. Landing Zone

The landing bucket stores raw source data.

```text
retail-landing-samma-2026/
│
├── sales/
│   └── sales.csv
│
├── customers/
│   └── customers.csv
│
├── inventory/
│   └── inventory.csv
│
└── scripts/
    └── retail_etl.py
```

The landing zone acts as the initial ingestion layer.

---

## 2. Curated Zone

The curated bucket stores processed data generated by PySpark.

```text
retail-curated-samma-2026/
│
└── fact_sales/
    └── *.parquet
```

The raw data is transformed before being stored in this layer.

---

## 3. Archive Zone

The archive bucket stores copies of the source data.

```text
retail-archive-samma-2026/
│
├── sales/
├── customers/
└── inventory/
```

The archive layer provides historical retention of source data.

---

## 4. Stage Zone

A separate staging bucket is also configured:

```text
retail-stage-samma-2026
```

This can be used as an intermediate storage area for future pipeline enhancements.

---

# 🔄 ETL Pipeline

The project follows the traditional:

```text
Extract → Transform → Load
```

architecture.

### Extract

Raw CSV files are stored in GCS.

### Transform

PySpark running on Dataproc performs:

* Data cleaning
* Null handling
* Duplicate removal
* Dataset joins
* Data transformation
* Processing timestamp creation
* Analytical transformations

### Load

The transformed data is written as Parquet files into the curated GCS layer and then loaded into BigQuery.

---

# ⚡ PySpark ETL

Apache Spark is used as the distributed processing engine.

The PySpark script:

```text
pyspark/retail_etl.py
```

reads the source files from GCS and processes them.

### Processing Flow

```text
Sales CSV
     |
     +----------------+
                      |
Customer CSV ---------+----> PySpark
                      |
Inventory CSV --------+
                           |
                           v
                    Data Cleaning
                           |
                           v
                     Deduplication
                           |
                           v
                         Joins
                           |
                           v
                    Transformations
                           |
                           v
                  Processing Timestamp
                           |
                           v
                    Curated Dataset
                           |
                           v
                     Parquet Files
```

---

# ☁️ Google Cloud Dataproc

Google Cloud Dataproc provides a managed environment for running Apache Spark jobs.

A Dataproc cluster was created for the retail ETL process.

The PySpark job successfully processed the retail data and generated Parquet output in:

```text
gs://retail-curated-samma-2026/fact_sales/
```

The processed output was then used for loading into BigQuery.

---

# 🏢 BigQuery Data Warehouse

Google BigQuery is used as the analytical data warehouse.

### Dataset

```text
retail_dataset
```

### Main Tables

```text
raw_sales
dim_customer
dim_product
fact_sales
pipeline_audit
```

---

# 📊 Fact Table

The central analytical table is:

```text
fact_sales
```

Schema:

```text
sale_id
store_id
product_id
customer_id
quantity
sale_amount
category
membership
sale_date
processing_timestamp
```

The fact table contains processed retail sales transactions and combines relevant information required for analytics.

---

# 👤 Customer Dimension

Table:

```text
dim_customer
```

Schema:

```text
customer_id
name
city
state
membership
```

This table stores customer-related descriptive information.

---

# 📦 Product Dimension

Table:

```text
dim_product
```

Schema:

```text
product_id
product_name
category
stock_quantity
```

This table stores product and inventory-related information.

---

# 📝 Pipeline Audit Table

Table:

```text
pipeline_audit
```

Schema:

```text
pipeline_name
run_id
start_time
end_time
status
records_processed
error_message
```

The table is designed to maintain information about pipeline executions.

---

# 🚀 BigQuery Partitioning

The `fact_sales` table is partitioned using:

```text
sale_date
```

Partitioning allows data to be organized by date and can reduce the amount of data scanned when analytical queries filter on the partitioning column.

---

# 🔗 BigQuery Clustering

The `fact_sales` table is clustered using:

```text
store_id
```

Clustering helps organize related records together and can improve query performance for queries that frequently filter or group by store.

---

# 📈 BigQuery Analytics

A summary table was created for analytical reporting:

```text
daily_sales_summary
```

The aggregation uses:

```sql
CREATE OR REPLACE TABLE
`PROJECT_ID.retail_dataset.daily_sales_summary` AS

SELECT
    store_id,
    category,
    SUM(sale_amount) AS total_sales,
    COUNT(*) AS transaction_count,
    AVG(sale_amount) AS avg_sales
FROM
    `PROJECT_ID.retail_dataset.fact_sales`
GROUP BY
    store_id,
    category;
```

This provides:

* Total sales
* Transaction count
* Average sale amount
* Store-level sales
* Category-level sales

---

# 🔔 Pub/Sub

Google Cloud Pub/Sub is used for pipeline status communication.

### Topic

```text
retail-pipeline-topic
```

### Subscription

```text
retail-pipeline-sub
```

A test message was successfully published:

```text
Retail ETL pipeline completed successfully
```

Pub/Sub acts as the messaging layer between the pipeline and the notification service.

---

# ⚡ Cloud Function

A Cloud Function was created to receive Pub/Sub messages.

### Function Name

```text
retail_notification
```

The function decodes the Pub/Sub message and logs the received pipeline status.

Example:

```python
import base64

def retail_notification(event, context):

    message = base64.b64decode(
        event['data']
    ).decode('utf-8')

    print("===== PUBSUB MESSAGE RECEIVED =====")
    print(message)

    print("===== PIPELINE COMPLETED =====")
```

The Pub/Sub → Cloud Function integration was successfully tested.

---

# 🔄 Cloud Composer and Apache Airflow

Cloud Composer is used to orchestrate the pipeline.

The Airflow DAG is:

```text
retail_sales_analytics_pipeline
```

The DAG is manually triggered because the project is designed as an on-demand pipeline.

---

# 🧩 Airflow DAG Architecture

```text
               GCS Files
                   |
        +----------+----------+
        |          |          |
        v          v          v
     Sales     Customers   Inventory
     Sensor      Sensor      Sensor
        |          |          |
        +----------+----------+
                   |
                   v
           File Validation
                   |
                   v
          Branch Validation
             /          \
            /            \
           v              v
   Dataproc ETL      Pipeline Failed
           |
           v
    BigQuery Temporary
         Loading
           |
           v
    Insert into Fact
         Sales
           |
           v
     Aggregation
           |
           v
        Pub/Sub
           |
           v
       GCS Archive
```

---

# 🧠 Airflow Tasks

The DAG contains the following major tasks:

```text
wait_for_sales_file
wait_for_customers_file
wait_for_inventory_file
validate_files
branch_validation
pipeline_failed
run_dataproc_etl
load_fact_sales_temp
insert_fact_sales
run_aggregation_query
publish_pipeline_status
archive_sales_file
```

---

# 📡 GCS Sensors

The pipeline uses GCS sensors to check whether the required input files are available.

The sensors check:

```text
sales/sales.csv
customers/customers.csv
inventory/inventory.csv
```

This prevents the downstream ETL process from starting before the required input files are available.

---

# ✅ File Validation

After all required files are detected, a Python validation task runs.

The validation task pushes the validation status to Airflow XCom.

```text
validation_status = success
```

The status is then used by the branching task.

---

# 🌿 Airflow Branching

The `BranchPythonOperator` checks the validation result.

If validation succeeds:

```text
run_dataproc_etl
```

is executed.

If validation fails:

```text
pipeline_failed
```

is executed.

This demonstrates conditional workflow execution in Airflow.

---

# 🔥 Dataproc Task

The Airflow DAG submits the PySpark job to the Dataproc cluster.

The Dataproc configuration uses:

```text
Cluster:
retail-etl-cluster
```

and the PySpark script:

```text
gs://retail-landing-samma-2026/scripts/retail_etl.py
```

---

# 📥 BigQuery Loading

The DAG loads the generated Parquet files into a temporary BigQuery table:

```text
fact_sales_temp
```

The temporary table is then used to insert the data into the final:

```text
fact_sales
```

This approach was used because the Parquet output had a schema difference for `sale_amount`.

The value is explicitly converted using:

```sql
CAST(sale_amount AS FLOAT64)
```

before inserting it into the final fact table.

---

# 📊 Dashboard

Looker Studio is used as the visualization layer.

The dashboard contains the following business KPIs:

```text
Total Sales
Revenue by Store
Revenue by Category
Membership Wise Revenue
Top Performing Store
Daily Sales Trend
```

---

# 📌 Dashboard Components

## Total Sales

Displays the total revenue generated from sales.

## Revenue by Store

Shows sales/revenue distribution across different stores.

## Revenue by Category

Shows revenue generated across product categories.

## Membership Wise Revenue

Shows revenue distribution based on customer membership.

## Top Performing Store

Displays stores ordered according to their sales performance.

## Daily Sales Trend

Shows how sales change across different dates.

---

# 🎛️ Dashboard Filter

A store-level dropdown filter was also added.

The filter uses:

```text
store_id
```

This allows the dashboard user to select a particular store and analyze the corresponding sales information.

---

# 📊 Data Validation Result

After loading the processed data into BigQuery, the fact table was validated.

Result:

```text
Total Records: 10
Total Revenue: 6350.0
```

This confirmed that the processed sales data was successfully loaded into the BigQuery analytical layer.

---

# 🛠️ Challenges Faced and Solutions

This project involved several real-world cloud engineering challenges.

## 1. Dataproc Cluster Configuration

The Dataproc cluster required correct compute resources, IAM permissions and storage configuration.

The cluster was successfully created and used for Spark processing.

---

## 2. GCS Permission Issue

The Dataproc job initially encountered a storage access permission issue.

The required storage permissions were configured so the job could access the necessary GCS resources.

After the permission correction, the PySpark job completed successfully.

---

## 3. BigQuery Schema Mismatch

The Parquet output inferred:

```text
sale_amount → INTEGER
```

while the BigQuery target table expected:

```text
sale_amount → FLOAT64
```

A temporary table was therefore used.

The final insertion explicitly converted the field:

```sql
CAST(sale_amount AS FLOAT64)
```

This resolved the schema mismatch.

---

## 4. Cloud Function Deployment

The Cloud Function deployment initially required correction of the source directory.

The function was then successfully deployed with the Pub/Sub trigger.

The integration was tested by publishing a message to:

```text
retail-pipeline-topic
```

The Cloud Function successfully received and logged the message.

---

## 5. Airflow DAG Deployment

The Airflow DAG was uploaded to the Composer bucket:

```text
gs://us-central1-retail-composer-d27e6dee-bucket/dags/
```

Airflow successfully recognized:

```text
retail_sales_analytics_pipeline
```

and the DAG was visible in the Airflow UI.

---

## 6. Composer Runtime Issue

A manual Airflow DAG execution was initiated.

During execution, the Composer environment encountered an Airflow metadata database connection issue.

The task log showed a connection error to the local Airflow metadata database.

Therefore, the final end-to-end Airflow execution was not completed.

The individual pipeline components were implemented and tested independently.

---

# 📁 Project Structure

```text
gcp-retail-analytics-pipeline/
│
├── README.md
├── .gitignore
│
├── data/
│   ├── sales.csv
│   ├── customers.csv
│   └── inventory.csv
│
├── pyspark/
│   └── retail_etl.py
│
├── airflow/
│   └── retail_analytics_pipeline.py
│
├── cloud_function/
│   ├── main.py
│   └── requirements.txt
│
├── bigquery/
│   ├── table_schemas.sql
│   └── aggregation.sql
│
└── screenshots/
    ├── gcs-buckets.png
    ├── dataproc-job.png
    ├── bigquery-tables.png
    ├── pubsub.png
    ├── cloud-function.png
    ├── airflow-dag.png
    └── looker-dashboard.png
```

---

# 📂 Repository Components

## `data/`

Contains sample retail source datasets.

```text
sales.csv
customers.csv
inventory.csv
```

## `pyspark/`

Contains the PySpark ETL implementation.

```text
retail_etl.py
```

## `airflow/`

Contains the Apache Airflow DAG.

```text
retail_analytics_pipeline.py
```

## `cloud_function/`

Contains the Pub/Sub-triggered Cloud Function.

```text
main.py
requirements.txt
```

## `bigquery/`

Contains BigQuery schemas and analytical SQL queries.

```text
table_schemas.sql
aggregation.sql
```

## `screenshots/`

Contains screenshots of the implemented GCP components and dashboard.

---

# 🔐 Security

Sensitive credentials must never be committed to GitHub.

The repository should not contain:

```text
Service Account JSON files
Private Keys
Passwords
API Keys
Access Tokens
.env files containing secrets
```

A `.gitignore` file should be used to prevent accidental credential uploads.

---

# 📈 End-to-End Data Flow

The complete data lifecycle is:

```text
1. Raw Retail Data
        |
        v
2. GCS Landing Zone
        |
        v
3. Airflow File Sensors
        |
        v
4. File Validation
        |
        v
5. Airflow Branching
        |
        v
6. Dataproc
        |
        v
7. PySpark ETL
        |
        v
8. GCS Curated Zone
        |
        v
9. BigQuery
        |
        v
10. Data Aggregation
        |
        +--------------------+
        |                    |
        v                    v
11. Looker Studio       12. Pub/Sub
                             |
                             v
                       13. Cloud Function
                             |
                             v
                       14. Notification
                             |
                             v
                       15. GCS Archive
```

---

# 🧠 Data Engineering Concepts Demonstrated

This project provides practical implementation experience with:

* Data Lake Architecture
* Landing, Curated and Archive Zones
* ETL Pipelines
* Distributed Data Processing
* Apache Spark
* PySpark
* Dataproc
* BigQuery
* Data Partitioning
* Data Clustering
* Star Schema Concepts
* Fact Tables
* Dimension Tables
* SQL Aggregations
* Apache Airflow
* Cloud Composer
* Airflow Sensors
* Airflow XCom
* Branching
* Task Dependencies
* Retry Mechanisms
* Pub/Sub
* Event-Driven Architecture
* Cloud Functions
* Data Archival
* Business Intelligence
* Data Visualization
* Cloud IAM
* GCP Resource Management

---

# 📚 Key Learnings

Through this project, I gained practical experience in designing and implementing a cloud-based data engineering pipeline.

The project helped me understand how different components work together:

```text
GCS
 ↓
Dataproc / PySpark
 ↓
BigQuery
 ↓
Airflow
 ↓
Pub/Sub
 ↓
Cloud Functions
 ↓
Looker Studio
```

I also gained experience troubleshooting:

* IAM permissions
* Dataproc jobs
* GCS access
* BigQuery schema mismatches
* Cloud Function deployment
* Airflow DAG deployment
* Composer runtime issues

---

# 🚀 Future Improvements

The pipeline can be extended with:

* Incremental data processing
* Slowly Changing Dimensions (SCD Type 2)
* Automated data quality checks
* Advanced pipeline auditing
* Email or Slack notifications
* Automated failure alerts
* CI/CD integration
* More analytical BigQuery tables
* Advanced Looker Studio dashboards
* Data freshness monitoring
* Automated Airflow monitoring
* Production-grade retry strategies
* Cloud Monitoring integration

---

# ✅ Project Status

## Implemented

* [x] GCS Landing Zone
* [x] GCS Curated Zone
* [x] GCS Archive Zone
* [x] GCS Stage Zone
* [x] Dataproc Cluster
* [x] PySpark ETL
* [x] Curated Parquet Output
* [x] BigQuery Dataset
* [x] BigQuery Tables
* [x] Fact Table
* [x] Dimension Tables
* [x] BigQuery Aggregation
* [x] Partitioning
* [x] Clustering
* [x] Pub/Sub Topic
* [x] Pub/Sub Subscription
* [x] Cloud Function
* [x] Pub/Sub → Cloud Function Integration
* [x] Cloud Composer Environment
* [x] Airflow DAG
* [x] GCS Sensors
* [x] Airflow Validation
* [x] Airflow Branching
* [x] Dataproc Airflow Operator
* [x] BigQuery Airflow Operators
* [x] Pub/Sub Airflow Operator
* [x] GCS Archive Operator
* [x] Looker Studio Dashboard
* [x] Store Filter

## Airflow Execution

The Airflow DAG was successfully created, uploaded and recognized by Cloud Composer.

A manual DAG execution was initiated. However, the Composer environment encountered an Airflow metadata database connection issue during execution, so the final end-to-end Airflow run was not completed.

The other major pipeline components were successfully implemented and tested independently.

---

# 👩‍💻 Author

**Samma Parveen**

Data Engineering Project

---

## 🛠️ Tech Stack

`GCP` `GCS` `PySpark` `Apache Spark` `Dataproc` `BigQuery` `Cloud Composer` `Apache Airflow` `Pub/Sub` `Cloud Functions` `Looker Studio` `Python` `SQL` `Cloud Shell`

```
```

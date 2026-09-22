# GCP Retail Analytics Data Engineering Pipeline

An end-to-end retail analytics data engineering project built using Google Cloud Platform.

The project demonstrates how raw retail data can be ingested into Google Cloud Storage, processed using PySpark on Dataproc, stored and analyzed in BigQuery, orchestrated using Cloud Composer (Apache Airflow), and integrated with Pub/Sub, Cloud Functions and Looker Studio.

---

## Project Overview

This project implements a multi-stage retail data pipeline for processing:

- Sales data
- Customer data
- Inventory data

The pipeline follows a layered data architecture:

```text
Retail CSV Files
       |
       v
Google Cloud Storage
   Landing Zone
       |
       v
Cloud Composer / Airflow
       |
       v
Dataproc + PySpark
       |
       v
GCS Curated Zone
       |
       v
BigQuery
       |
       v
Analytics / Aggregation
       |
       +----------------+
       |                |
       v                v
    Pub/Sub       Looker Studio
       |
       v
Cloud Function
       |
       v
Notification / Logging
       |
       v
GCS Archive

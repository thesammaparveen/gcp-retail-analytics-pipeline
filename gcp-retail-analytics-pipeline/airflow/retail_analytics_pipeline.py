from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = "true-orb-509302-j4"

LANDING_BUCKET = "retail-landing-samma-2026"
CURATED_BUCKET = "retail-curated-samma-2026"
ARCHIVE_BUCKET = "retail-archive-samma-2026"

BQ_DATASET = "retail_dataset"
CLUSTER_NAME = "retail-etl-cluster"
DATAPROC_REGION = "us-east4"
TOPIC_NAME = "retail-pipeline-topic"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retail_sales_analytics_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "gcp", "dataproc", "bigquery"],
) as dag:

    # 1. Wait for input files
    wait_for_sales = GCSObjectExistenceSensor(
        task_id="wait_for_sales_file",
        bucket=LANDING_BUCKET,
        object="sales/sales.csv",
        timeout=300,
        poke_interval=15,
    )

    wait_for_customers = GCSObjectExistenceSensor(
        task_id="wait_for_customers_file",
        bucket=LANDING_BUCKET,
        object="customers/customers.csv",
        timeout=300,
        poke_interval=15,
    )

    wait_for_inventory = GCSObjectExistenceSensor(
        task_id="wait_for_inventory_file",
        bucket=LANDING_BUCKET,
        object="inventory/inventory.csv",
        timeout=300,
        poke_interval=15,
    )

    # 2. Validation
    def validate_files(**context):
        print("Validation Successful")
        context["ti"].xcom_push(
            key="validation_status",
            value="success",
        )

    validation_task = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files,
    )

    # 3. Branching
    def branch_logic(**context):
        status = context["ti"].xcom_pull(
            task_ids="validate_files",
            key="validation_status",
        )

        if status == "success":
            return "run_dataproc_etl"

        return "pipeline_failed"

    branching_task = BranchPythonOperator(
        task_id="branch_validation",
        python_callable=branch_logic,
    )

    def failure_task():
        print("Pipeline Validation Failed")

    pipeline_failed = PythonOperator(
        task_id="pipeline_failed",
        python_callable=failure_task,
    )

    # 4. Dataproc PySpark ETL
    pyspark_job = {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "pyspark_job": {
            "main_python_file_uri": (
                f"gs://{LANDING_BUCKET}/scripts/retail_etl.py"
            ),
        },
    }

    run_dataproc_etl = DataprocSubmitJobOperator(
        task_id="run_dataproc_etl",
        job=pyspark_job,
        region=DATAPROC_REGION,
        project_id=PROJECT_ID,
    )

    # 5. Load generated Parquet into a temporary BigQuery table
    load_fact_sales_temp = GCSToBigQueryOperator(
        task_id="load_fact_sales_temp",
        bucket=CURATED_BUCKET,
        source_objects=["fact_sales/*.parquet"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{BQ_DATASET}.fact_sales_temp"
        ),
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # 6. Convert temporary data into the fixed fact_sales schema
    insert_fact_sales = BigQueryInsertJobOperator(
        task_id="insert_fact_sales",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO
                    `{PROJECT_ID}.{BQ_DATASET}.fact_sales`
                    (
                        sale_id,
                        store_id,
                        product_id,
                        customer_id,
                        quantity,
                        sale_amount,
                        category,
                        membership,
                        sale_date,
                        processing_timestamp
                    )
                    SELECT
                        sale_id,
                        store_id,
                        product_id,
                        customer_id,
                        quantity,
                        CAST(sale_amount AS FLOAT64),
                        category,
                        membership,
                        sale_date,
                        processing_timestamp
                    FROM
                    `{PROJECT_ID}.{BQ_DATASET}.fact_sales_temp`
                """,
                "useLegacySql": False,
            }
        },
    )

    # 7. Create daily sales summary
    aggregation_query = BigQueryInsertJobOperator(
        task_id="run_aggregation_query",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                    `{PROJECT_ID}.{BQ_DATASET}.daily_sales_summary`
                    AS
                    SELECT
                        store_id,
                        category,
                        SUM(sale_amount) AS total_sales,
                        COUNT(*) AS transaction_count,
                        AVG(sale_amount) AS avg_sales
                    FROM
                    `{PROJECT_ID}.{BQ_DATASET}.fact_sales`
                    GROUP BY
                        store_id,
                        category
                """,
                "useLegacySql": False,
            }
        },
    )

    # 8. Publish completion notification
    publish_message = PubSubPublishMessageOperator(
        task_id="publish_pipeline_status",
        project_id=PROJECT_ID,
        topic=TOPIC_NAME,
        messages=[
            {
                "data": b"Retail Pipeline Completed Successfully",
            }
        ],
    )

    # 9. Archive sales file
    archive_sales_file = GCSToGCSOperator(
        task_id="archive_sales_file",
        source_bucket=LANDING_BUCKET,
        source_object="sales/sales.csv",
        destination_bucket=ARCHIVE_BUCKET,
        destination_object="archive/sales.csv",
        move_object=False,
    )

    # DAG dependencies
    [
        wait_for_sales,
        wait_for_customers,
        wait_for_inventory,
    ] >> validation_task

    validation_task >> branching_task

    branching_task >> run_dataproc_etl
    branching_task >> pipeline_failed

    run_dataproc_etl >> load_fact_sales_temp
    load_fact_sales_temp >> insert_fact_sales
    insert_fact_sales >> aggregation_query
    aggregation_query >> publish_message
    publish_message >> archive_sales_file

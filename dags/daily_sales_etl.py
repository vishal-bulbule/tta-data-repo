"""Daily sales ETL.

Loads raw order events into BigQuery, aggregates them into the daily sales
mart, then runs data quality checks. Scheduled at 02:00 UTC.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

PROJECT_ID = "gcai-may-26"
DATASET = "sales"
LANDING_BUCKET = "gcai-may-26-dev-artifacts"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="daily_sales_etl",
    description="Load raw orders, build the daily sales mart, run DQ checks.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=4,
    tags=["sales", "etl", "bigquery"],
) as dag:

    load_raw_orders = GCSToBigQueryOperator(
        task_id="load_raw_orders",
        bucket=LANDING_BUCKET,
        source_objects=["orders/{{ ds }}/*.json"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.raw_orders",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    build_daily_agg = BigQueryInsertJobOperator(
        task_id="build_daily_agg",
        configuration={
            "query": {
                "query": "{% include 'sql/02_daily_sales_agg.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    dq_checks = BigQueryInsertJobOperator(
        task_id="dq_checks",
        configuration={
            "query": {
                "query": "{% include 'sql/03_data_quality_checks.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    refresh_lookup = BigQueryInsertJobOperator(
        task_id="refresh_customer_lookup",
        configuration={
            "query": {
                "query": (
                    f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.customer_lookup` AS "
                    f"SELECT * FROM `{PROJECT_ID}.{DATASET}.customers_raw`"
                ),
                "useLegacySql": False,
            }
        },
    )

    load_raw_orders >> refresh_lookup >> build_daily_agg >> dq_checks

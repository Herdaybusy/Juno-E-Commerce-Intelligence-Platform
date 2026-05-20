"""
Juno E-Commerce Pipeline DAG

Runs eBay Browse API extraction for:
- laptops
- mobile phones

Stores cleaned data in PostgreSQL for analytics.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# Ensure project root is available to Airflow

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from ebay_scraper import run as run_pipeline


# Default DAG arguments

DEFAULT_ARGS = {
    "owner": "juno-data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# DAG Definition

with DAG(
    dag_id="juno_ecommerce_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily eBay UK product ingestion via Browse API",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["juno", "ecommerce", "ebay"],
) as dag:

    fetch_laptops = PythonOperator(
        task_id="fetch_laptops",
        python_callable=run_pipeline,
        op_kwargs={
            "search_term": "laptops",
            "result_limit": 1000,
        },
    )

    fetch_phones = PythonOperator(
        task_id="fetch_phones",
        python_callable=run_pipeline,
        op_kwargs={
            "search_term": "mobile phones",
            "result_limit": 1000,
        },
    )

    # Parallel execution (no dependency)
    fetch_laptops
    fetch_phones
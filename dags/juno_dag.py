import sys
import os
from datetime import datetime, timedelta

# Add project root to sys.path so we can import the ebay_scraper module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG 
from airflow.operators.python import PythonOperator 
from ebay_scraper import run as run_pipeline 


DEFAULT_ARGS = {
    'owner': 'juno-data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='juno_ecommerce_pipeline',
    default_args=DEFAULT_ARGS,
    description=(
        'Daily eBay UK product listing pull via Browse API for '
        'Juno market intelligence'
    ),
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['juno', 'ecommerce', 'ebay-api'],
) as dag:

    fetch_laptops = PythonOperator(
        task_id='fetch_laptops',
        python_callable=run_pipeline,
        op_kwargs={
            'search_term': 'laptops',
            'result_limit': 1000,
        },
        doc_md="""
        Pulls up to 1,000 laptop listings from the eBay UK marketplace
        via the Browse API. Results are cleaned and loaded into the
        laptops_listings table in PostgreSQL.
        """,
    )

    fetch_phones = PythonOperator(
        task_id='fetch_phones',
        python_callable=run_pipeline,
        op_kwargs={
            'search_term': 'mobile phones',
            'result_limit': 1000,
        },
        doc_md="""
        Pulls up to 1,000 mobile phone listings from the eBay UK marketplace.
        Feeds the mobile_phones_listings table for cross-category pricing
        analysis alongside laptops.
        """,
    )

    # both tasks run in parallel
    [fetch_laptops, fetch_phones]
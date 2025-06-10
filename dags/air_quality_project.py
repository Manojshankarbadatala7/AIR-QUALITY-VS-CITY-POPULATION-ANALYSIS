import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Make sure we can import from lib/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import pipeline components
from lib.fetch_air_quality import fetch_air_quality
from lib.extract_population_from_csv import extract_population
from lib.load_population_data import load_population_data
from lib.format_air_quality import format_air_quality
from lib.format_population import format_population
from lib.combine_air_population import combine_air_population
from lib.elastic_indexer import elastic_indexer


default_args = {
    "owner": "saiki_fdiq7b5",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 6, 10),
}

with DAG(
    dag_id="air_quality_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,  # Change to "@daily" for auto-run
    catchup=False,
    description="Air Quality vs Population ETL Pipeline",
    tags=["bigdata", "airflow", "air_quality"],
) as dag:



    # 2. Load population data to raw layer
    load_population_task = PythonOperator(
        task_id="load_population_data",
        python_callable=load_population_data
    )

    # 3. Fetch air quality data
    fetch_air_quality_task = PythonOperator(
        task_id="fetch_air_quality_data",
        python_callable=fetch_air_quality
    )

    # 4. Format air quality data
    format_air_quality_task = PythonOperator(
        task_id="format_air_quality_data",
        python_callable=format_air_quality
    )

    # 5. Format population data
    format_population_task = PythonOperator(
        task_id="format_population_data",
        python_callable=format_population
    )

    # 6. Combine datasets
    combine_task = PythonOperator(
        task_id="combine_air_and_population",
        python_callable=combine_air_population
    )

    # 7. Index into Elasticsearch
    elastic_index_task = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=elastic_indexer
    )

    # DAG Dependencies


    load_population_task >> format_population_task
    fetch_air_quality_task >> format_air_quality_task

    format_population_task >> combine_task
    format_air_quality_task >> combine_task

    combine_task >> elastic_index_task

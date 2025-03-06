#import all the reqired libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

# get the dags directory
dag_path = os.getcwd()

# initialising the DAG
default_args = {
    'owner': 'Kasasa',
    'start_date': days_ago(5)
}

def transform_data() -> pd.DataFrame:
    booking = pd.read(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    client = pd.read(f"{dag_path}/raw_data/client.csv", low_memory=False)
    hotel = pd.read(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

    # merge booking.csv with client.csv
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)


def load_data() -> pd.DataFrame:
    pass

ingestion_dag = DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the PythonOperator for booking data ingestion
task_1 = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    dag = ingestion_dag
)

task_2 = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    dag = ingestion_dag
)

task_1 >> task_2


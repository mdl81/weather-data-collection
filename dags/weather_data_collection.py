from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from dotenv import load_dotenv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_collection_etl',
    default_args=default_args,
    description='ETL pipeline to collect weather data for Minsk every hour',
    # schedule_interval='*/5 * * * *',  # Run every 5 minutes
    schedule_interval='0 * * * *',  # Run every hour at the start of the hour
    catchup=False,  # Prevent catching up on missed runs
)

# Extract
def extract_weather_data(**kwargs):
    load_dotenv()
    api_key = os.getenv("OPENWEATHERMAP_API_KEY")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": 53.9,
        "lon": 27.5667,
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print("Data fetched successfully")
        kwargs['ti'].xcom_push(key='weather_data', value=data)
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Transform
def transform_weather_data(execution_date, **kwargs):
    data = kwargs['ti'].xcom_pull(key='weather_data', task_ids='extract_weather_data')
    if not data:
        raise Exception("No data found for transformation")

    execution_time = execution_date.strftime('%Y-%m-%d %H:%M:%S')

    temp_data = {
        "datetime": [execution_time],
        "temp": [data["main"]["temp"]],
        "feels_like": [data["main"]["feels_like"]],
        "temp_min": [data["main"]["temp_min"]],
        "temp_max": [data["main"]["temp_max"]],
        "pressure": [data["main"]["pressure"]]
    }

    wind_data = {
        "datetime": [execution_time],
        "speed": [data["wind"]["speed"]],
        "deg": [data["wind"]["deg"]],
        "gust": [data["wind"].get("gust", None)]
    }

    kwargs['ti'].xcom_push(key='temp_data', value=temp_data)
    kwargs['ti'].xcom_push(key='wind_data', value=wind_data)

# Load
def load_weather_data(execution_date, **kwargs):
    temp_data = kwargs['ti'].xcom_pull(key='temp_data', task_ids='transform_weather_data')
    wind_data = kwargs['ti'].xcom_pull(key='wind_data', task_ids='transform_weather_data')
    if not temp_data or not wind_data:
        raise Exception("No transformed data found for loading")

    temp_df = pd.DataFrame(temp_data)
    wind_df = pd.DataFrame(wind_data)

    date_str = execution_date.strftime('%Y_%m_%d')
    parquet_dir = "/opt/airflow/parquet_files"
    os.makedirs(parquet_dir, exist_ok=True)
    temp_file = f"{parquet_dir}/minsk_{date_str}_temp.parquet"
    wind_file = f"{parquet_dir}/minsk_{date_str}_wind.parquet"

    # Append data to existing parquet files
    if os.path.exists(temp_file):
        existing_temp_df = pd.read_parquet(temp_file)
        temp_df = pd.concat([existing_temp_df, temp_df])
    temp_df.to_parquet(temp_file, index=False)

    if os.path.exists(wind_file):
        existing_wind_df = pd.read_parquet(wind_file)
        wind_df = pd.concat([existing_wind_df, wind_df])
    wind_df.to_parquet(wind_file, index=False)

    print("Data saved to parquet files")

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task

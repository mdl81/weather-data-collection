from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

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
    'weather_data_collection',
    default_args=default_args,
    description='Collect weather data for Minsk every hour',
    schedule_interval='0 * * * *',  # Run every hour at the start of the hour
    catchup=False,  # Prevent catching up on missed runs
)

def fetch_weather_data(execution_date, **kwargs):
    import requests
    import pandas as pd
    from dotenv import load_dotenv
    import os

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

        execution_time = execution_date.strftime('%Y-%m-%d %H:%M:%S')

        # Extract temperature data
        temp_data = {
            "datetime": [execution_time],
            "temp": [data["main"]["temp"]],
            "feels_like": [data["main"]["feels_like"]],
            "temp_min": [data["main"]["temp_min"]],
            "temp_max": [data["main"]["temp_max"]],
            "pressure": [data["main"]["pressure"]]
        }

        # Extract wind data
        wind_data = {
            "datetime": [execution_time],
            "speed": [data["wind"]["speed"]],
            "deg": [data["wind"]["deg"]],
            "gust": [data["wind"].get("gust", None)]
        }

        temp_df = pd.DataFrame(temp_data)
        wind_df = pd.DataFrame(wind_data)

        date_str = execution_date.strftime('%Y_%m_%d')
        parquet_dir = "/opt/airflow/parquet_files"
        os.makedirs(parquet_dir, exist_ok=True)
        temp_file = f"{parquet_dir}/minsk_{date_str}_temp.parquet"
        wind_file = f"{parquet_dir}/minsk_{date_str}_wind.parquet"

        if os.path.exists(temp_file):
            existing_temp_df = pd.read_parquet(temp_file)
            temp_df = pd.concat([existing_temp_df, temp_df])
        temp_df.to_parquet(temp_file, index=False)

        if os.path.exists(wind_file):
            existing_wind_df = pd.read_parquet(wind_file)
            wind_df = pd.concat([existing_wind_df, wind_df])
        wind_df.to_parquet(wind_file, index=False)

        print("Data saved to parquet files")
    else:
        print(f"Failed to fetch data: {response.status_code}")

fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

fetch_weather_data_task
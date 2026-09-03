from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd

from sqlalchemy import create_engine

def load_weather():

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=53.48"
        "&longitude=-2.24"
        "&current=temperature_2m,wind_speed_10m"
    )

    response = requests.get(url)

    data = response.json()

    weather = data["current"]
    df = pd.DataFrame(
        [{
            "observation_time": weather["time"],
            "temperature": weather["temperature_2m"],
            "windspeed": weather["wind_speed_10m"]
        }]

    )

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )

    df.to_sql(
        "weather_data",
        engine,
        if_exists="append",
        index=False
    )

    print(df)
# Adding data quality checks
if response.status_code != 200:
    raise ValueError("API request failed")

if df.empty:
    raise ValueError("No weather data returned")

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 1, 1),    
    schedule="@hourly",
    catchup=False,
) as dag:

    weather_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )
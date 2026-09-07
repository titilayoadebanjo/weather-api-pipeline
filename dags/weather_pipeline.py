from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd

from sqlalchemy import create_engine

def validate_weather_data():

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=53.48"
        "&longitude=-2.24"
        "&current=temperature_2m,wind_speed_10m"
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise ValueError("API request failed")
    
    data = response.json()

    weather = data["current"]

    if weather["temperature_2m"] is None:
        raise ValueError("Temperature is missing")

    if weather["wind_speed_10m"] is None:
        raise ValueError("Windspeed is missing")
    
    if weather["temperature_2m"] < -50:
        raise ValueError("Temperature is unrealistic")
    
    if weather["temperature_2m"] > 60:
        raise ValueError("Temperature is unrealistic")
    
    if weather["wind_speed_10m"] < 0:
        raise ValueError("Windspeed cannot be negative")

    print("Weather data validation passed")

def load_weather():

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=53.48"
        "&longitude=-2.24"
        "&current=temperature_2m,wind_speed_10m"
    )

    response = requests.get(url)

    if response.status_code != 200:
        raise ValueError("API request failed")
        
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

    print(f"Loaded {len(df)} weather record(s)")

def export_weather_csv():

    print ("EXPORT TASK STARTED")

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )
    df = pd.read_sql(
        "SELECT * FROM weather_data ORDER BY observation_time",
        engine
    )

    print(f"Rows exported: {len(df)}")

    output_file = "/opt/airflow/data/weather_data.csv"

    df.to_csv(
        output_file,
        index=False
        )


    print(f"File written to: {output_file}")


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 1, 1),    
    schedule="@hourly",
    catchup=False,
    tags=["weather", "postgresql", "powerbi",]
) as dag:


    validation_task = PythonOperator(
        task_id="validate_weather",
        python_callable=validate_weather_data
    )   
 
    weather_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    export_task = PythonOperator(
        task_id="export_weather_csv",
        python_callable=export_weather_csv
    )
  
    validation_task >> weather_task >> export_task
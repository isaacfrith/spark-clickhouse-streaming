from datetime import datetime
import os
import uuid
import time
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

MELBOURNE_LAT = -37.8136
MELBOURNE_LON = 144.9631

default_args = {
    "owner": "admin",
    "start_date": datetime(2025, 1, 12, 9, 0),
}


def get_data():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is not set. Check your .env file.")

    params = {
        "lat": MELBOURNE_LAT,
        "lon": MELBOURNE_LON,
        "appid": api_key,
        "units": "metric",
    }
    res = requests.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params=params,
        timeout=10,
    )
    res.raise_for_status()
    return res.json()


def format_data(res):
    return {
        "record_id": str(uuid.uuid4()),
        "city": res.get("name"),
        "country": res["sys"].get("country"),
        "latitude": res["coord"].get("lat"),
        "longitude": res["coord"].get("lon"),
        "weather_main": res["weather"][0].get("main"),
        "weather_description": res["weather"][0].get("description"),
        "temp": res["main"].get("temp"),
        "feels_like": res["main"].get("feels_like"),
        "temp_min": res["main"].get("temp_min"),
        "temp_max": res["main"].get("temp_max"),
        "pressure": res["main"].get("pressure"),
        "humidity": res["main"].get("humidity"),
        "wind_speed": res["wind"].get("speed"),
        "wind_deg": res["wind"].get("deg"),
        "clouds": res["clouds"].get("all"),
        "dt": res["dt"],
    }


def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=["broker:29092"],
        max_block_ms=5000,
    )
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:  # run for ~1 minute per task run
            break
        try:
            data = get_data()
            formatted = format_data(data)
            producer.send("weather_data", json.dumps(formatted).encode("utf-8"))
            print(f"Sent -> {formatted['city']} {formatted['temp']}°C {formatted['weather_main']}")
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(5)


with DAG(
    "weather_2kafka",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "kafka"],
) as dag:
    streaming_task = PythonOperator(
        task_id="pull_melbourne_weather",
        python_callable=stream_data,
    )
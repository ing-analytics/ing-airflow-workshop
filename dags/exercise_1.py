"""
## Exercise 1:

Create a DAG that runs daily to retrieve the weather in Amsterdam,
in a following task output the average temperature of the day, then
in the lask task print an output based on the avg temperature:

- If lower than 12, print "cold".
- Between 12 and 25, print "moderate"
- If higher than 25 print "hot"

You can retrieve today's weather in Amsterdam with the following API:
https://api.open-meteo.com/v1/forecast?latitude=52.374&longitude=4.8897&hourly=temperature_2m
"""

from typing import Any
from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import requests

default_args = {
    "owner": "airflow",
    "retries": 1,
}

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.374&longitude=4.8897&hourly=temperature_2m"


@task
def retrieve_weather() -> dict[str, Any]:
    weather_response = requests.get(URL)
    weather_response.raise_for_status()
    data = weather_response.json()
    return data


@task
def get_avg_temperature(weather_data: dict[str, Any]) -> float:
    hourly_temperature = weather_data["hourly"]["temperature_2m"]
    avg_temperature = sum(hourly_temperature) / len(hourly_temperature)
    return avg_temperature


@task
def output_weather(avg_temperature: float) -> None:
    if avg_temperature < 12:
        print("cold")
    elif 12 <= avg_temperature <= 25:
        print("moderate")
    else:
        print("hot")


with DAG(
    "exercise-1",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["workshop", "exercise"],
) as dag:
    weather = retrieve_weather()
    avg_temp = get_avg_temperature(weather)
    output_weather(avg_temp)

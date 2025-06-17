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


default_args = {
    "owner": "airflow",
    "retries": 1,
}


@task
def retrieve_weather() -> dict[str, Any]:
    # TODO: IMPLEMENT
    pass


@task
def get_avg_temperature() -> float:
    # TODO: IMPLEMENT
    pass


@task
def output_weather() -> None:
    # TODO: IMPLEMENT
    pass


with DAG(
    "exercise-1",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=["workshop", "exercise"],
) as dag:
    # TODO: COMPLETE
    pass

from airflow import DAG
from airflow.decorators import task

from datetime import datetime


default_args = {
    "owner": "airflow",
    "retries": 1,
}


@task
def hello_world():
    print("Hello world!!")


@task
def bye():
    print("Bye!")


with DAG(
    "hello-world",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["workshop", "example"],
) as dag:
    hello_world_task = hello_world()
    bye_task = bye()
    hello_world_task >> bye_task

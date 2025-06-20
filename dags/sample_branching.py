"""
Example DAG demonstrating TaskFlow branching using @task.branch.

- Uses @task.branch to choose a path based on a random number.
"""

import random
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago


@task
def start() -> int:
    print("Starting the DAG. Generating a random number.")
    number = random.randint(1, 100)
    print(f"Random number is: {number}")
    return number


@task.branch
def choose_branch(number: int) -> str:
    if number % 2 == 0:
        return "branch_even_number"
    else:
        return "branch_odd_number"


@task
def branch_even_number():
    print("This is the branch for even numbers.")


@task
def branch_odd_number():
    print("This is the branch for odd numbers.")


with DAG(
    dag_id="sample-branching",
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["workshop", "example"],
    doc_md=__doc__,
) as dag:
    # Initiate the tasks:
    start_number = start()
    branch = choose_branch(number=start_number)
    even_numbers_branch = branch_even_number()
    odd_numbers_branch = branch_odd_number()
    # Set the task dependencies:
    start_number >> branch
    branch >> [even_numbers_branch, odd_numbers_branch]

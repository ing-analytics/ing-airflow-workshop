"""
## Exercise 2:

Use the AWS Operators and a branching task to process a file in an S3 bucket
based on if it already exists or not.

1. Use an S3 Sensor to check the input file exists
2. List all objects in the output bucket
3. If the output file does not exist, create the file
4. Otherwise, just print the contents of the file in the bucket.

## Extra exercise (Pushing XComs):

1. On the show_file() task, calculate the sum of the 'amount' column in the Pandas DataFrame
2. In the same task, push an XCom called 'total_amount' with the result of the sum.
3. On a new task, get the total amount from the XCom and print it on the console.
"""

from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.decorators.branch_python import branch_task

import io
import pandas as pd

S3_CONN_ID = "workshop_s3"
SRC_BUCKET_NAME = "workshop"
SRC_FILE = "input.csv"
DEST_BUCKET_NAME = "workshop-output"
DEST_FILE = "yourname.parquet"


@branch_task
def branch_for_existing_input(files: list[str]) -> str:
    print(files)
    if DEST_FILE in files:
        return "show_file"
    else:
        return "process_file"


@task
def show_file(ti: TaskInstance):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    print(f"Reading file from s3://{DEST_BUCKET_NAME}/{DEST_FILE}")
    object = s3_hook.get_key(DEST_FILE, DEST_BUCKET_NAME)
    fb = io.BytesIO()
    object.download_fileobj(fb)
    fb.seek(0)
    parquet_data = pd.read_parquet(fb)
    print(parquet_data)
    # Extra exercise:
    total_amount = parquet_data["amount"].sum()
    ti.xcom_push(key="total_amount", value=total_amount)


@task
def process_file():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    print(f"Reading file from s3://{SRC_BUCKET_NAME}/{SRC_FILE}")
    file_contents = s3_hook.read_key(SRC_FILE, SRC_BUCKET_NAME)
    file_io = io.StringIO(file_contents)
    csv_data = pd.read_csv(file_io)
    print(csv_data)
    parquet_contents = csv_data.to_parquet(index=False)
    s3_hook.load_bytes(parquet_contents, DEST_FILE, DEST_BUCKET_NAME, replace=True)
    print(f"File uploaded to s3://{DEST_BUCKET_NAME}/{DEST_FILE}")


@task
def show_amount(ti: TaskInstance):
    total_amount = ti.xcom_pull(task_ids="show_file")
    print(f"Total amount in transactions is: {total_amount}")


with DAG(
    dag_id="exercise-2",
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["workshop", "exercise"],
    doc_md=__doc__,
) as dag:
    # 1. List all objects in the bucket
    # 2. Check if file exists in the bucket
    # 3. If file does not exist, create the file
    # 4. Print the contents of the file in the bucket.
    list_files = S3ListOperator(
        task_id="s3-list-files", bucket=DEST_BUCKET_NAME, aws_conn_id=S3_CONN_ID
    )
    branch_for_input = branch_for_existing_input(list_files.output)
    show_file_task = show_file()
    process_file_task = process_file()
    list_files >> branch_for_input >> [show_file_task, process_file_task]
    show_file_task >> show_amount()

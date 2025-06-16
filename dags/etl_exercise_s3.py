from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.decorators.python import python_task
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


@python_task
def show_file():
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    print(f"Reading file from s3://{DEST_BUCKET_NAME}/{DEST_FILE}")
    object = s3_hook.get_key(DEST_FILE, DEST_BUCKET_NAME)
    fb = io.BytesIO()
    object.download_fileobj(fb)
    fb.seek(0)
    parquet_data = pd.read_parquet(fb)
    print(parquet_data)


@python_task
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


with DAG(
    dag_id="etl-exercise-s3-dag",
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["workshop-exercise"],
) as dag:
    # 1. List all objects in the bucket
    # 2. Check if file exists in the bucket
    # 3. If file does not exist, create the file
    # 4. Print the contents of the file in the bucket.
    list_files = S3ListOperator(
        task_id="s3-list-files", bucket=DEST_BUCKET_NAME, aws_conn_id=S3_CONN_ID
    )
    branch_for_input = branch_for_existing_input(list_files.output)
    list_files >> branch_for_input >> [show_file(), process_file()]

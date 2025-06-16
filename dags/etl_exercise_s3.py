from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.decorators.python import python_task
from airflow.decorators.branch_python import branch_task

import boto3
from botocore.client import Config

S3_CONN_ID = "workshop_s3"
SRC_BUCKET_NAME = "workshop"
SRC_FILE = "input.csv"
DEST_BUCKET_NAME = "workshop-output"
DEST_FILE = "yourname.parquet"


@branch_task
def branch_for_existing_input(files: list[str]) -> str:
    print(files)
    if SRC_FILE in files:
        return "show_file"
    else:
        return "process_file"


@python_task
def show_file():
    handler = S3Handler(conn_id=S3_CONN_ID)
    file_contents = handler.get_file_contents(SRC_BUCKET_NAME, SRC_FILE)
    print(file_contents)


@python_task
def process_file():
    print("Not implemented yet")


class S3Handler:
    def __init__(self, conn_id: str):
        # Fetch connection from Airflow
        conn = BaseHook.get_connection(conn_id)
        # Extract credentials and extra parameters
        access_key = conn.login
        secret_key = conn.password
        endpoint_url = conn.extra_dejson.get("endpoint_url", "http://localhost:9000")
        # Create the S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="eu-central-1",
        )

    def upload_to_bucket(self, bucket_name: str, file_contents: str, path: str):
        try:
            self.s3_client.put_object(
                Bucket=bucket_name, Key=path, Body=file_contents.encode()
            )
            print(f"Successfully uploaded to bucket '{bucket_name}' at path '{path}'.")
        except Exception as e:
            print(f"Failed to upload to bucket: {e}")

    def get_file_contents(self, bucket_name: str, path: str) -> str:
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=path)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            print(f"Failed to retrieve file from bucket: {e}")
            return ""


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
        task_id="s3-list-files", bucket=SRC_BUCKET_NAME, aws_conn_id=S3_CONN_ID
    )
    check_file_exists = branch_for_existing_input(list_files.output)
    list_files >> check_file_exists >> [show_file(), process_file()]

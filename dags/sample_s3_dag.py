import io
from airflow.models.dag import DAG
from airflow.operators.python import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.dates import days_ago

BUCKET_NAME = "workshop-output"
CONN_ID = "workshop_s3"


@task
def upload_files():
    """This is a python callback to add files into the s3 bucket"""
    # Create some files and add them to the bucket
    s3_hook = S3Hook(aws_conn_id=CONN_ID)
    for i in range(0, 3):
        s3_hook.load_string(
            key=f"path/data{i}",
            bucket_name=BUCKET_NAME,
            string_data=f"input{i}",
            replace=True,
        )


@task
def show_file_contents(file_idx: int):
    # Get the file object:
    s3_hook = S3Hook(aws_conn_id=CONN_ID)
    file_key = f"path/data{file_idx}"
    file_object = s3_hook.get_key(key=file_key, bucket_name=BUCKET_NAME)
    # Download the object to a file buffer:
    file_buffer = io.BytesIO()
    file_object.download_fileobj(file_buffer)
    file_buffer.seek(0)
    # Print the contents of the file:
    file_contents = file_buffer.read().decode()
    print(f"Contents of {file_key} are:")
    print(file_contents)


with DAG(
    dag_id="sample-s3-dag",
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["workshop", "example"],
) as dag:
    create_bucket = S3CreateBucketOperator(
        task_id="s3_bucket_dag_create",
        aws_conn_id=CONN_ID,
        bucket_name=BUCKET_NAME,
    )

    add_files_to_bucket = upload_files()
    show_files_tasks = []
    for i in range(3):
        task = show_file_contents.override(task_id=f"show_file_{i}")
        task_instance = task(file_idx=i)
        show_files_tasks.append(task_instance)

    create_bucket >> add_files_to_bucket >> show_files_tasks

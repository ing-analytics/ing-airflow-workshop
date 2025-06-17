"""
## Exercise 2:

Use the AWS Operators and a branching task to process a file in an S3 bucket
based on if it already exists or not.

1. Use an S3 Sensor to check the input file exists
2. List all objects in the output bucket
3. If the output file does not exist, create the file
4. Otherwise, just print the contents of the file in the bucket.

## Extra exercise (Pushing XComs):

1. On the task for step 4, calculate the sum of the 'amount' column in the Pandas DataFrame
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


with DAG(
    dag_id="exercise-2",
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=["workshop", "exercise"],
    doc_md=__doc__,
) as dag:
    # TODO: IMPLEMENT
    pass

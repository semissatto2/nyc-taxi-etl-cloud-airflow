# Header reference for Airflow: 03/02/2025 - 23:36
import os

from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ny_taxi_ingest_airflow_dataset')

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green' 
URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/green_trip_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
LOCAL_FILE_TEMPLATE = AIRFLOW_HOME + '/green_trip_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GCS_OUTPUT_FILE_TEMPLATE = 'green_trip_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


def format_to_parquet(src_file):
    parquet_file = src_file.replace('.csv', '.parquet')
    table = pv.read_csv(src_file)
    pq.write_table(table, parquet_file)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1),  # We are going to ingest data from 2020
    "end_date": datetime(2021, 1, 1),  # Up to 2021
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_ny_green",
    schedule_interval="0 6 1 * *",  # Execute At 06:00 on day-of-month 1.
    default_args=default_args,
    catchup=True,  # Allow DAG execute for the 'past'
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_and_extract_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} | gunzip > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{GCS_OUTPUT_FILE_TEMPLATE}",
            "local_file": f"{LOCAL_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}',
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{GCS_OUTPUT_FILE_TEMPLATE}"],
            },
        },
    )

    download_and_extract_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
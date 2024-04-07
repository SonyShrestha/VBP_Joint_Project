from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from minio import Minio
import os
import pandas as pd
from google.cloud import storage
import time

# Airflow DAG definitions
default_args = {
    'owner': 'Peace',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    "schedule":"@once",
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'minio_to_gcs_final',
    default_args=default_args,
    description='Automated transfer and transformation of datasets from MinIO to GCS',
    schedule=timedelta(days=1), 
)

# Function to list files in MinIO bucket
def list_files_from_minio(ti):
    minio_client = Minio(
        '127.0.0.1:9000',
        access_key='nXTh6mLbFXXOKUiuAdYA',
        secret_key='kOPBmO0meeBsnQFJ3OjBNnwd3RfvWkM7S4DUXsgY',
        secure=False
    )
    files = minio_client.list_objects('testfile', recursive=True)
    file_paths = [file.object_name for file in files if file.object_name.endswith(('.csv', '.json'))]
    ti.xcom_push(key='file_paths', value=file_paths)

# Function to download, transform, and upload a file
def process_file(**kwargs):
    # Set up Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/pce/Documents/VBP_Joint_Project-main/formal-atrium-418823-7fbbc75ebbc6.json'
  
    ti = kwargs['ti']
    start = time.time()

    file_paths = ti.xcom_pull(key='file_paths', task_ids='list_files')
    minio_client = Minio(
        '127.0.0.1:9000',
        access_key='nXTh6mLbFXXOKUiuAdYA',
        secret_key='kOPBmO0meeBsnQFJ3OjBNnwd3RfvWkM7S4DUXsgY',
        secure=False
    )

    for file_path in file_paths:
        local_file_path = f'/tmp/{os.path.basename(file_path)}'
        minio_client.fget_object('testfile', file_path, local_file_path)

        if file_path.endswith('.csv'):
            df = pd.read_csv(local_file_path,  dtype={'Postage_pick_up': str})
        elif file_path.endswith('.json'):
            df = pd.read_json(local_file_path)

        # timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        date_today = datetime.now().strftime('%Y%m%d%H%M%S') 
        parquet_file_path = f"/tmp/{os.path.splitext(os.path.basename(file_path))[0]}_{date_today}.parquet"
        df.to_parquet(parquet_file_path)

        client = storage.Client()
        bucket = client.get_bucket('spicy_1')
        blob = bucket.blob(parquet_file_path.replace('/tmp/', ''))
        blob.upload_from_filename(parquet_file_path)
        
        os.remove(local_file_path)
        os.remove(parquet_file_path)

        print(f"Uploaded {parquet_file_path} to GCS")
        
        end = time.time()
        duration = end - start
        ti.xcom_push(key='duration', value=duration)
        
# Define task to send a notification to Slack
def send_slack_message(content_message, id, **kwargs):
    
    ti = kwargs['ti']
    duration = ti.xcom_pull(task_ids='process_file', key='duration')
    duration_minutes = int(duration // 60)
    duration_seconds = int(duration % 60)

    payload = {
        "text": f"{content_message} \n Total time taken: {duration_minutes} minutes and {duration_seconds} seconds."
    
    
    }
    header= {
        "Content-type" : "application/json"
    }
    url = f"https://hooks.slack.com/services/{id}"

    response = requests.post(url, json=payload, headers=header, )
    if response.status_code == 200:
        print("Successful")
    else:
        print("Unseccessful")



list_files = PythonOperator(
    task_id='list_files',
    python_callable=list_files_from_minio,
    dag=dag,
)

process_files = PythonOperator(
    task_id='process_file',
    python_callable=process_file,
    provide_context=True,
    dag=dag,
)

message_slack= PythonOperator(
    task_id="message_slack",
    python_callable= send_slack_message,
    op_kwargs= {
        "content_message" : "Data Migration from MinIO (Temporal Loading) to Google Cloud Storage (Persistent Loading) has completed successfully",
        "id" : "T031G6GA8M9/B066ZGYKU1H/wyj4OcNIgYDJTI8yB1SRr5Yn"  },
    dag=dag,
)  

list_files >> process_files >> message_slack


# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from minio import Minio
# import os
# import pyarrow.csv as pv
# import pyarrow.json as pj
# import pyarrow.parquet as pq
# from google.cloud import storage

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'minio_to_gcs_test_one',
#     default_args=default_args,
#     description='Transfer and transform files from MinIO to GCS using PyArrow',
#     schedule=timedelta(days=1),
# )

# def list_files_from_minio(ti):
#     minio_client = Minio(
#         '127.0.0.1:9000',
#         access_key='nXTh6mLbFXXOKUiuAdYA',
#         secret_key='kOPBmO0meeBsnQFJ3OjBNnwd3RfvWkM7S4DUXsgY',
#         secure=False
#     )
#     files = minio_client.list_objects('testfile', recursive=True)
#     file_paths = [file.object_name for file in files if file.object_name.endswith(('.csv', '.json'))]
#     ti.xcom_push(key='file_paths', value=file_paths)

# def process_file(**kwargs):
#      # Set up Google Cloud credentials
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/pce/Documents/VBP_Joint_Project-main/formal-atrium-418823-7fbbc75ebbc6.json'

#     ti = kwargs['ti']
#     file_paths = ti.xcom_pull(key='file_paths', task_ids='list_files')
#     minio_client = Minio(
#         '127.0.0.1:9000',
#         access_key='nXTh6mLbFXXOKUiuAdYA',
#         secret_key='kOPBmO0meeBsnQFJ3OjBNnwd3RfvWkM7S4DUXsgY',
#         secure=False
#     )

#     client = storage.Client()
#     bucket = client.get_bucket('spicy_1')

#     for file_path in file_paths:
#         local_file_path = f'/tmp/{os.path.basename(file_path)}'
#         minio_client.fget_object('testfile', file_path, local_file_path)

#         if file_path.endswith('.csv'):
#             table = pv.read_csv(local_file_path)
#         elif file_path.endswith('.json'):
#             table = pj.read_json(local_file_path)

#         date_today = datetime.now().strftime('%Y%m%d')  # Use date only for versioning
#         gcs_file_name = os.path.splitext(os.path.basename(file_path))[0] + f"_{date_today}.parquet"
#         parquet_file_path = f"/tmp/{gcs_file_name}"
#         pq.write_table(table, parquet_file_path)

#         blob = bucket.blob(gcs_file_name)
#         blob.upload_from_filename(parquet_file_path)
#         print(f"Uploaded {gcs_file_name} to GCS")

#         os.remove(local_file_path)  # Clean up local file
#         os.remove(parquet_file_path)  # Clean up parquet file

# list_files = PythonOperator(
#     task_id='list_files',
#     python_callable=list_files_from_minio,
#     dag=dag,
# )

# process_files = PythonOperator(
#     task_id='process_file',
#     python_callable=process_file,
#     dag=dag,
# )

# list_files >> process_files

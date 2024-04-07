# # from datetime import datetime, timedelta
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from minio import Minio
# # from google.cloud import storage
# # import os
# # import configparser
# # import logging




# # # Initialize logging
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger("MinIO_GCS_Transfer")

# # config = configparser.ConfigParser()
# # # Uses the directory where the current script is located
# # dag_dir = os.path.dirname(os.path.realpath(__file__))
# # config_path = os.path.join(dag_dir, 'config.ini')
# # config.read(config_path)

# # # Airflow DAG definitions
# # default_args = {
# #     'owner': 'Peace',
# #     'depends_on_past': False,
# #     'start_date': datetime(2024, 4, 7),
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=5),
# #     'catchup': False
# # }

# # dag = DAG(
# #     'minio_to_gcs_transfer',
# #     default_args=default_args,
# #     description='Transfer files from local to MinIO then to GCS',
# #     schedule_interval="@daily"
# # )

# # # Functions for DAG tasks
# # def upload_files_to_minio():
# #     minio_client = Minio(
# #         config['MINIO']['endpoint'],
# #         access_key=config['MINIO']['access_key'],
# #         secret_key=config['MINIO']['secret_key'],
# #         secure=False
# #     )
# #     if not minio_client.bucket_exists(config['MINIO']['bucket_name']):
# #         minio_client.make_bucket(config['MINIO']['bucket_name'])

# #     directory = config['PATHS']['local_directory']
# #     for file_name in os.listdir(directory):
# #         file_path = os.path.join(directory, file_name)
# #         minio_client.fput_object(config['MINIO']['bucket_name'], file_name, file_path)
# #         logger.info(f"Uploaded {file_name} to MinIO bucket {config['MINIO']['bucket_name']}")

# # def transfer_files_to_gcs():
# #     minio_client = Minio(
# #         config['MINIO']['endpoint'],
# #         access_key=config['MINIO']['access_key'],
# #         secret_key=config['MINIO']['secret_key'],
# #         secure=False
# #     )
# #     files = minio_client.list_objects(config['MINIO']['bucket_name'], recursive=True)

# #     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GCS']['credentials_path']
# #     client = storage.Client()
# #     bucket = client.get_bucket(config['GCS']['bucket_name'])

# #     for file in files:
# #         local_path = f'/tmp/{file.object_name}'
# #         minio_client.fget_object(config['MINIO']['bucket_name'], file.object_name, local_path)
# #         blob = bucket.blob(file.object_name)
# #         blob.upload_from_filename(local_path)
# #         os.remove(local_path)
# #         logger.info(f"Transferred {file.object_name} from MinIO to GCS")

# # # Define DAG tasks
# # upload_to_minio = PythonOperator(
# #     task_id='upload_to_minio',
# #     python_callable=upload_files_to_minio,
# #     dag=dag,
# # )

# # transfer_to_gcs = PythonOperator(
# #     task_id='transfer_to_gcs',
# #     python_callable=transfer_files_to_gcs,
# #     dag=dag,
# # )

# # upload_to_minio >> transfer_to_gcs


# import os
# import pandas as pd
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from minio import Minio
# from google.cloud import storage
# import configparser
# import logging
# from datetime import datetime, timedelta
# import requests

# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("MinIO_GCS_Transfer")

# # Load configuration settings
# config = configparser.ConfigParser()
# dag_dir = os.path.dirname(os.path.realpath(__file__))
# config_path = os.path.join(dag_dir, 'config.ini')
# config.read(config_path)

# # Airflow DAG definitions
# default_args = {
#     'owner': 'admin',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 4, 7),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     'catchup': False,
#     "schedule":"@once",
# }

# dag = DAG(
#     'minio_to_gcs_transfer_final',
#     default_args=default_args,
#     description='Transfer files from MinIO to GCS with Parquet conversion',
#      schedule=timedelta(days=1)
# )

# def notify_slack(channel_msg):
#     webhook_url = config['SLACK']['webhook_url']
#     message = {"text": channel_msg}
#     response = requests.post(webhook_url, json=message)
#     if response.status_code != 200:
#         raise ValueError('Request to slack returned an error %s, the response is:\n%s'
#                          % (response.status_code, response.text))

# def upload_files_to_minio():
#     minio_client = Minio(
#         config['MINIO']['endpoint'],
#         access_key=config['MINIO']['access_key'],
#         secret_key=config['MINIO']['secret_key'],
#         secure=config.getboolean('MINIO', 'secure')
#     )
#     uploaded_files = []
#     if not minio_client.bucket_exists(config['MINIO']['bucket_name']):
#         minio_client.make_bucket(config['MINIO']['bucket_name'])

#     directory = config['PATHS']['local_directory']
    
#      # Upload files, skipping lock files
#     for file_name in os.listdir(directory):
#         if file_name.startswith('.~lock.'):
#             logger.info(f"Skipping lock file {file_name}")
#             continue  # Skip uploading this file
#         file_path = os.path.join(directory, file_name)
#         if os.path.isfile(file_path):  # Make sure it's a file
#             minio_client.fput_object(config['MINIO']['bucket_name'], file_name, file_path)
#             logger.info(f"Uploaded {file_name} to MinIO bucket {config['MINIO']['bucket_name']}")

#     # Sending a notification to Slack after uploading
#     if uploaded_files:
#         notify_slack(f"Successfully uploaded {len(uploaded_files)} files to MinIO.")

# def transfer_files_to_gcs():
#     minio_client = Minio(
#         config['MINIO']['endpoint'],
#         access_key=config['MINIO']['access_key'],
#         secret_key=config['MINIO']['secret_key'],
#         secure=config.getboolean('MINIO', 'secure')
#     )
#     files = minio_client.list_objects(config['MINIO']['bucket_name'], recursive=True)
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GCS']['credentials_path']
#     client = storage.Client()
#     bucket = client.get_bucket(config['GCS']['bucket_name'])
#     transferred_files = []

#     for file in files:
#         # Correctly accessing file object's name for local path creation
#         local_path = f'/tmp/{file.object_name}'
#         minio_client.fget_object(config['MINIO']['bucket_name'], file.object_name, local_path)

#         # Continue if file is lock file or unsupported type
#         if file.object_name.startswith('.~lock.') or not file.object_name.endswith(('.csv', '.json')):
#             logger.info(f"Unsupported file type for {file.object_name}, skipping...")
#             os.remove(local_path)
#             continue

#         if file.object_name.endswith('.csv'):
#             df = pd.read_csv(local_path,  dtype={'Postage_pick_up': str})
#         elif file.object_name.endswith('.json'):
#             df = pd.read_json(local_path)
        
#         timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
#         parquet_filename = f'{os.path.splitext(file.object_name)[0]}_{timestamp}.parquet'
        
#         parquet_filepath = f"/tmp/{os.path.splitext(os.path.basename(file))[0]}_{timestamp}.parquet"
#         df.to_parquet(parquet_filepath)

#         blob = bucket.blob(parquet_filename)
#         blob.upload_from_filename(parquet_filepath)
#         os.remove(local_path)  # Remove the original file
#         os.remove(parquet_filepath)  # Remove the converted file
        
#         transferred_files.append(parquet_filename)

#         logger.info(f"Transferred {parquet_filename} from MinIO to GCS as Parquet")
        
#         if transferred_files:
#             notify_slack(f"Successfully transferred {len(transferred_files)} files from MinIO to GCS.")
        


# upload_to_minio = PythonOperator(
#     task_id='upload_to_minio',
#     python_callable=upload_files_to_minio,
#     dag=dag,
# )

# transfer_to_gcs = PythonOperator(
#     task_id='transfer_to_gcs',
#     python_callable=transfer_files_to_gcs,
#     dag=dag,
# )

# upload_to_minio >> transfer_to_gcs


from datetime import datetime, timedelta
import os
import pandas as pd
import requests
from minio import Minio
from google.cloud import storage
import configparser
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MinIO_GCS_Transfer")

# Load configurations
config = configparser.ConfigParser()
dag_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(dag_dir, 'config.ini')
config.read(config_path)

# Define default arguments for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'minio_to_gcs_transfer_final',
    default_args=default_args,
    description='Transfer files from MinIO to GCS with Parquet conversion',
    schedule_interval="@daily"  # Run daily
)

def notify_slack(message):
    """Send a notification message to Slack."""
    webhook_url = config['SLACK']['webhook_url']
    payload = {"text": message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        logger.error('Request to Slack returned an error %s, the response is:\n%s' % (response.status_code, response.text))

def upload_files_to_minio():
    """Upload files from local directory to MinIO bucket."""
    minio_client = Minio(
        config['MINIO']['endpoint'],
        access_key=config['MINIO']['access_key'],
        secret_key=config['MINIO']['secret_key'],
        secure=config.getboolean('MINIO', 'secure', fallback=False)
    )

    if not minio_client.bucket_exists(config['MINIO']['bucket_name']):
        minio_client.make_bucket(config['MINIO']['bucket_name'])

    directory = config['PATHS']['local_directory']
    uploaded_files = []

    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if not file_name.startswith('.~lock.') and os.path.isfile(file_path):
            minio_client.fput_object(config['MINIO']['bucket_name'], file_name, file_path)
            uploaded_files.append(file_name)
            logger.info(f"Uploaded {file_name} to MinIO bucket {config['MINIO']['bucket_name']}")

    notify_slack(f"Successfully uploaded {len(uploaded_files)} files to MinIO.")

def transfer_files_to_gcs():
    """Download files from MinIO, convert to Parquet, and upload to GCS."""
    minio_client = Minio(
        config['MINIO']['endpoint'],
        access_key=config['MINIO']['access_key'],
        secret_key=config['MINIO']['secret_key'],
        secure=config.getboolean('MINIO', 'secure', fallback=False)
    )

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GCS']['credentials_path']
    client = storage.Client()
    bucket = client.get_bucket(config['GCS']['bucket_name'])

    files = minio_client.list_objects(config['MINIO']['bucket_name'], recursive=True)
    transferred_files = []

    for file in files:
        if file.object_name.startswith('.~lock.') or not file.object_name.endswith(('.csv', '.json')):
            continue

        local_path = f'/tmp/{file.object_name}'
        minio_client.fget_object(config['MINIO']['bucket_name'], file.object_name, local_path)
        df = pd.read_csv(local_path,  dtype={'Postage_pick_up': str}) if file.object_name.endswith('.csv') else pd.read_json(local_path)

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        parquet_filename = f'{os.path.splitext(file.object_name)[0]}_{timestamp}.parquet'
        parquet_filepath = f'/tmp/{parquet_filename}'
        df.to_parquet(parquet_filepath)

        blob = bucket.blob(parquet_filename)
        blob.upload_from_filename(parquet_filepath)
        os.remove(local_path)
        os.remove(parquet_filepath)
        transferred_files.append(parquet_filename)
        logger.info(f"Transferred {parquet_filename} from MinIO to GCS as Parquet")

    notify_slack(f"Successfully transferred {len(transferred_files)} files from MinIO to GCS.")

# Define DAG tasks
upload_to_minio = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_files_to_minio,
    dag=dag,
)

transfer_to_gcs = PythonOperator(
    task_id='transfer_to_gcs',
    python_callable=transfer_files_to_gcs,
    dag=dag,
)

# Task dependencies
upload_to_minio >> transfer_to_gcs

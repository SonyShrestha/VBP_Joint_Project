from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq

# Define the function to read Parquet file
def read_parquet_file():
    # Path to the Parquet file
    parquet_file = 'path/to/your/file.parquet'

    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(parquet_file)

    # Convert the Table to a Pandas DataFrame
    df = table.to_pandas()

    # Display the DataFrame
    print(df)

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'collectors_monthly_processing',
    default_args=default_args,
    description='Send notification on daily basis',
    schedule_interval=timedelta(days=1),  # Monthly on the first day
    catchup=False
)

# Define PythonOperator to execute read_parquet_file function
send_notification = PythonOperator(
    task_id='read_parquet',
    python_callable=read_parquet_file,
    dag=dag,
)

# Set task dependencies
send_notification

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import configparser


def load_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG for synthetic scripts
dag_synthetic = DAG(
    'synthetic_daily_processing',
    default_args=default_args,
    description='Process synthetic scripts daily',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False
)

config = load_config(config_path)
synthetic_dir = config['COMMON']['synthetic_dir']

def run_synthetic_script(filename):
    exec(open(f"{synthetic_dir}/{filename}").read(), {'__name__': '__main__'})

# Define tasks for each synthetic script
for filename in os.listdir(synthetic_dir):
    if filename.endswith('.py'):
        task = PythonOperator(
            task_id=f'run_{filename[:-3]}',  # Remove .py extension for task_id
            python_callable=run_synthetic_script,
            op_kwargs={'filename': filename},
            dag=dag_synthetic,
        )

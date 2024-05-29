from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_collectors = DAG(
    'etl_formatted_zone',
    default_args=default_args,
    description='ETL for moving data to formatted zone',
    schedule_interval='0 0 1 * *',  
    catchup=False
)

# business_review_sentiment
business_review_sentiment = BashOperator(
    task_id='business_review_sentiment',
    bash_command='python ./formatted_zone/business_review_sentiment.py',
    dag=dag_collectors,
)

# customer_location
customer_location = BashOperator(
    task_id='customer_location',
    bash_command='python ./formatted_zone/customer_location.py',
    dag=dag_collectors,
)

# customer_purchase
customer_purchase = BashOperator(
    task_id='customer_purchase',
    bash_command='python ./formatted_zone/customer_purchase.py',
    dag=dag_collectors,
)

# customer_sales
customer_sales = BashOperator(
    task_id='customer_sales',
    bash_command='python ./formatted_zone/customer_sales.py',
    dag=dag_collectors,
)

# customers
customers = BashOperator(
    task_id='customers',
    bash_command='python ./formatted_zone/customers.py',
    dag=dag_collectors,
)

# dynamic_pricing
dynamic_pricing = BashOperator(
    task_id='dynamic_pricing',
    bash_command='python ./formatted_zone/dynamic_pricing.py',
    dag=dag_collectors,
)

# establishments_catalonia
establishments_catalonia = BashOperator(
    task_id='establishments_catalonia',
    bash_command='python ./formatted_zone/establishments_catalonia.py',
    dag=dag_collectors,
)

# estimate_expiry_date
estimate_expiry_date = BashOperator(
    task_id='estimate_expiry_date',
    bash_command='python ./formatted_zone/estimate_expiry_date.py',
    dag=dag_collectors,
)

# estimate_perishability
estimate_perishability = BashOperator(
    task_id='estimate_perishability',
    bash_command='python ./formatted_zone/estimate_perishability.py',
    dag=dag_collectors,
)

# expiry_notification
expiry_notification = BashOperator(
    task_id='expiry_notification',
    bash_command='python ./formatted_zone/expiry_notification.py',
    dag=dag_collectors,
)

# individual_review_sentiment
individual_review_sentiment = BashOperator(
    task_id='individual_review_sentiment',
    bash_command='python ./formatted_zone/individual_review_sentiment.py',
    dag=dag_collectors,
)

# location
location = BashOperator(
    task_id='location',
    bash_command='python ./formatted_zone/location.py',
    dag=dag_collectors,
)

# mealdbrecommend
mealdbrecommend = BashOperator(
    task_id='location',
    bash_command='python ./formatted_zone/mealdbrecommend.py',
    dag=dag_collectors,
)
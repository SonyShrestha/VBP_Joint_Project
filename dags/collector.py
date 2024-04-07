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
    'collectors_monthly_processing',
    default_args=default_args,
    description='Run collector scripts monthly',
    schedule_interval='0 0 1 * *',  # Monthly on the first day
    catchup=False
)

# Task for Scrapy crawler for approvedfood_groceries
scrapy_approvedfood_task = BashOperator(
    task_id='scrapy_approvedfood_groceries',
    bash_command='scrapy crawl approvedfood_groceries -O ./data/raw/Approvedfood.json',
    dag=dag_collectors,
)

# Task for Scrapy crawler for mealdb
scrapy_mealdb_task = BashOperator(
    task_id='scrapy_mealdb',
    bash_command='scrapy crawl mealdb -O ./data/raw/mealdb.json',
    dag=dag_collectors,
)

# Tasks for running Python scripts
big_basket_task = BashOperator(
    task_id='big_basket',
    bash_command='python ./landing_zone/collectors/big_basket/big_basket.py',
    dag=dag_collectors,
)

establishments_scraper_task = BashOperator(
    task_id='establishments_scraper',
    bash_command='python ./landing_zone/collectors/catalonia_establishment_location/establishments_scraper.py',
    dag=dag_collectors,
)

sm_retail_customer_task = BashOperator(
    task_id='sm_retail_customer',
    bash_command='python ./landing_zone/collectors/customers/sm_retail_customer.py',
    dag=dag_collectors,
)

eat_by_date_task = BashOperator(
    task_id='eat_by_date',
    bash_command='python ./landing_zone/collectors/eat_by_date/eat_by_date.py',
    dag=dag_collectors,
)

scrap_flipkart_pages_task = BashOperator(
    task_id='scrap_flipkart_pages',
    bash_command='python ./landing_zone/collectors/flipkart/scrap_flipkart_pages_sel.py',
    dag=dag_collectors,
)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd

default_args = {
    'owner': 'Caio César',
    'depends_on_past': False,
    'email': ['your_email@email.com'],
    'start_date': datetime(2020, 12, 20, 00, 00, 00),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'titanic',
    description='Mean age of titanic\'s people.',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
)

get_dataset = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/titanic.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/titanic.csv')
    med = df.Age.mean()
    return med

def print_mean_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f'Mean age: {value} years')

task_mean_age = PythonOperator(
    task_id='calculate-mean-age',
    python_callable=calculate_mean_age,
    dag=dag
)

task_print_mean_age = PythonOperator(
    task_id='print-mean-age',
    python_callable=print_mean_age,
    provide_context=True,
    dag=dag
)

get_dataset >> task_mean_age >> task_print_mean_age
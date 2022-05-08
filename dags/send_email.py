from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

from airflow import DAG

default_args = {
    'owner': 'toanbui1991',
    'depends_on_past': False,
    'email': ['toanbui1991@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'send_email',
    default_args=default_args,
    description='A simple email ',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021,1,1),
    catchup=False
) as dag:

    send_email_notification= EmailOperator(
        task_id="send_test_email",
        to= "djangouserone@gmail.com",
        subject="Test email",
        html_content="<h2>this is test email"   
    )
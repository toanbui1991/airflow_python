from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import io



default_args={"owner":"toanbui1991", "email_on_failure": True, "email": "toanbui1991@gmail.com"}

with DAG(dag_id="email_morning_daily",start_date=datetime(2022,5,8),schedule_interval="0 1 * * *",default_args=default_args)as dag:

    email_function=EmailOperator(
        task_id="email_morning_daily",
        to="toanbui1991@gmail.com",
        subject="Airflow success alert",
        html_content="""
        <h1>Test email for daily morning notifincation</h1>
        """
    )

    email_function

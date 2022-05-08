from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import io



def test_function():
    with io.open("/opt/airflow/output_files/test_v2.txt","w",encoding="utf-8")as f1:
        f1.write("hello from airflow dag, we are check email notification")
        f1.close()

default_args={"owner":"toanbui1991", "email_on_failure": True, "email": "toanbui1991@gmail.com"}

with DAG(dag_id="test_workflow_for_email",start_date=datetime(2022,5,8),schedule_interval="@hourly",default_args=default_args)as dag:
    test_function=PythonOperator(
        task_id="test_function",
        python_callable=test_function
    )
    email_function=EmailOperator(
        task_id="email_function",
        to="djangouserone@gmail.com",
        subject="Airflow success alert",
        html_content="""
        <h1>Test email notifincation</h1>
        """
    )

    test_function>>email_function

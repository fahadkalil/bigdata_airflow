from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def processar_dados():
    print("Processando dados...")
    return "OK"

with DAG(
    dag_id="dag_com_python",
    start_date=datetime(2026, 11, 6),
    schedule_interval="@hourly",
    catchup=False,
):

    task = PythonOperator(
        task_id="processar",
        python_callable=processar_dados
    )

    task
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="meu_primeiro_dag",
    start_date=datetime(2026, 6, 12),
    schedule_interval="@daily",
    catchup=False,
    description="Meu primeiro pipeline com Airflow"
):

    tarefa_1 = BashOperator(
        task_id="exibe_mensagem",
        bash_command="echo 'Airflow funcionando!'"
    )

    tarefa_2 = BashOperator(
        task_id="segunda_tarefa",
        bash_command="echo 'Segunda tarefa executada!'"
    )

    tarefa_1 >> tarefa_2
"""
MODELO DE DAG DA ATIVIDADE  (copie para  airflow/dags/dag_grupo.py  e complete os TODO)

Fluxo esperado:

   aguarda_parquet (S3KeySensor)  ->  parquet_para_kafka (@task)  ->  confirma_kafka (PythonSensor)

1. O S3KeySensor espera aparecer um .parquet em  s3://default/entrada/  (Alarik).
2. A task lê o(s) arquivo(s) com PyArrow e publica CADA LINHA como JSON no tópico Kafka do grupo.
3. Depois de publicar, o arquivo é MOVIDO para  processados/  (senão o sensor dispara de novo para sempre).
4. O PythonSensor confere no Kafka se as mensagens chegaram.

Dicas e armadilhas estão no EDITAL.md. Rode  `docker compose exec airflow-scheduler python /opt/airflow/dags/dag_grupo.py`
para ver se o arquivo tem erro de sintaxe/import (a DAG também aparece na interface se estiver correta).
"""
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.python import PythonSensor

# ---------------------------------------------------------------- configuração do grupo
GRUPO = os.environ.get("GRUPO_ID", "00")
CONEXAO_S3 = "alarik_s3"                      # já criada pelo docker-compose (Admin > Connections)
BUCKET = "default"
PREFIXO_ENTRADA = "entrada/"
PREFIXO_PROCESSADOS = "processados/"
TOPICO = f"g{GRUPO}_parquet"                  # tópico do seu grupo (o Kafka cria sozinho no primeiro envio)
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


# ---------------------------------------------------------------- funções de apoio
def criar_producer():
    """TODO: devolva um kafka.KafkaProducer apontando para KAFKA.  (pip: kafka-python, já instalado na imagem)"""
    raise NotImplementedError("TODO: criar_producer")


def linhas_do_parquet(conteudo: bytes, tamanho_lote: int = 500):
    """TODO: leia os bytes de um Parquet com PyArrow e devolva (yield) UMA LINHA POR VEZ, como dict.

    Dica: pyarrow.parquet.ParquetFile(io.BytesIO(conteudo)).iter_batches(batch_size=...) e lote.to_pylist().
    Não carregue o arquivo inteiro em listas gigantes: processe em lotes.
    """
    raise NotImplementedError("TODO: linhas_do_parquet")


def serializar(linha: dict) -> bytes:
    """TODO: transforme o dict em JSON (bytes UTF-8).

    Atenção: o Parquet traz Decimal e datetime, que o json padrão NÃO sabe serializar (TypeError).
    Não altere os valores: o professor confere a assinatura de cada linha na correção.
    """
    raise NotImplementedError("TODO: serializar")


def mover_para_processados(hook: S3Hook, chave: str) -> None:
    """TODO: mova  entrada/x.parquet  ->  processados/x.parquet  (copiar e depois apagar a original)."""
    raise NotImplementedError("TODO: mover_para_processados")


def kafka_recebeu(esperado: int, **_) -> bool:
    """TODO (PythonSensor): True quando o tópico já tem pelo menos `esperado` mensagens.

    Dica: kafka.KafkaConsumer(bootstrap_servers=KAFKA).  Compare end_offsets() com beginning_offsets()
    de todas as partições do tópico (consumer.partitions_for_topic(TOPICO), kafka.TopicPartition).
    Lembre de fechar o consumer.
    """
    raise NotImplementedError("TODO: kafka_recebeu")


# ---------------------------------------------------------------- a DAG
@dag(
    dag_id=f"parquet_para_kafka_g{GRUPO}",
    start_date=datetime(2026, 11, 1),
    schedule=None,            # TODO: escolha o agendamento (dica: veja "@continuous" + max_active_runs=1 no EDITAL)
    catchup=False,
    max_active_runs=1,
    tags=["atividade", f"grupo{GRUPO}"],
)
def parquet_para_kafka():

    aguarda_parquet = S3KeySensor(
        task_id="aguarda_parquet",
        bucket_name=BUCKET,
        bucket_key="TODO_trocar_pelo_padrao_com_curinga",    # TODO: padrão dos arquivos em entrada/ (curinga *.parquet)
        # TODO: wildcard_match, aws_conn_id (a conexão é CONEXAO_S3)
        # TODO: mode ("poke" ou "reschedule"), poke_interval, timeout, soft_fail ... e explique a escolha no README
    )

    @task
    def parquet_para_kafka_task() -> int:
        """TODO: listar os .parquet de entrada/, ler, publicar, dar flush, mover. Devolva o total de mensagens."""
        raise NotImplementedError("TODO: parquet_para_kafka_task")

    total = parquet_para_kafka_task()

    confirma_kafka = PythonSensor(
        task_id="confirma_kafka",
        python_callable=kafka_recebeu,
        op_kwargs={"esperado": total},       # o valor devolvido pela task chega aqui (XCom)
        poke_interval=5,
        timeout=120,
    )

    aguarda_parquet >> total >> confirma_kafka


parquet_para_kafka()

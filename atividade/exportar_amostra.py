"""
Exporta uma amostra do tópico Kafka do grupo para  atividade/amostra_kafka.json  (entra no repositório).

    docker compose exec airflow-scheduler python /opt/airflow/atividade/exportar_amostra.py

Lê o tópico g<GRUPO>_parquet desde o começo e grava as primeiras N mensagens (padrão 30) com partição e offset.
O professor confere as assinaturas das linhas: não edite o arquivo.
"""
import argparse
import json
import os
import sys
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--grupo", default=os.environ.get("GRUPO_ID", ""))
    ap.add_argument("--topico", default="")
    ap.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--max", type=int, default=30)
    ap.add_argument("--saida", default=str(Path(__file__).resolve().parent / "amostra_kafka.json"))
    a = ap.parse_args(argv)
    topico = a.topico or f"g{a.grupo}_parquet"

    from kafka import KafkaConsumer
    consumidor = KafkaConsumer(topico, bootstrap_servers=a.kafka, auto_offset_reset="earliest",
                               enable_auto_commit=False, consumer_timeout_ms=8000)
    saida, total_lido = [], 0
    for msg in consumidor:
        total_lido += 1
        if len(saida) < a.max:
            try:
                valor = json.loads(msg.value.decode("utf-8"))
            except ValueError:
                valor = {"_invalido": msg.value.decode("utf-8", "replace")}
            saida.append({"topic": msg.topic, "partition": msg.partition, "offset": msg.offset, "value": valor})
    consumidor.close()
    if not saida:
        print(f"Nenhuma mensagem no tópico {topico}. A DAG já publicou?", file=sys.stderr)
        return 1
    Path(a.saida).write_text(json.dumps(saida, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{len(saida)} mensagem(ns) gravadas em {a.saida} (o tópico tem pelo menos {total_lido})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

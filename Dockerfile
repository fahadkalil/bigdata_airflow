# Imagem do Airflow usada na disciplina: apache/airflow:2.8.1 + PyArrow + cliente Kafka.
# Publicada como fahadkalil/my-airflow:2.8.1 (o docker-compose.yml já aponta para ela).
# Para reconstruir:  docker build -t fahadkalil/my-airflow:2.8.1 .
FROM apache/airflow:2.8.1

RUN pip install --no-cache-dir "pyarrow==14.0.2" "kafka-python==2.0.2"

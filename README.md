## Container Docker com [Apache Airflow 2.8.1](https://hub.docker.com/layers/apache/airflow/2.8.1/)

### Airflow UI: http://localhost:8086

    username: admin
    password: admin

##### Adicionada comunicação com o container [bigdata_docker](https://github.com/fahadkalil/bigdata_docker) no ``docker-compose.yml``:

    networks:
      bigdata_docker_network:
        external: true
        name: bigdata_docker_default    

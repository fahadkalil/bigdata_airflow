# Grupo NN: Parquet → Alarik → Airflow → Kafka

> Modelo do README da entrega. Substitua tudo que está entre `<...>` e apague este aviso.

## Integrantes

| Nome | Matrícula |
|---|---|
| <nome> | <matrícula> |
| <nome> | <matrícula> |

## Como rodar

<comandos, na ordem, para subir o ambiente e executar a DAG; inclua o `GRUPO_ID`, mas **não** a `CHAVE_GRUPO`>

## O que a DAG faz

<3 a 6 linhas, em suas palavras. Cite o `dag_id`, o tópico Kafka e o caminho dos arquivos no Alarik (`entrada/` e `processados/`).>

## Decisões e justificativas

- **S3KeySensor:** `mode=<poke|reschedule>`, `poke_interval=<..>`, `timeout=<..>`, `soft_fail=<..>`. Por quê? <...>
- **Agendamento (`schedule`):** <...>. Por quê? <...>
- **Leitura com PyArrow:** <tamanho do lote e por quê>
- **Serialização:** <como tratou Decimal e datetime>
- **Idempotência:** <como evitam publicar o mesmo arquivo duas vezes; o que acontece se a task falhar no meio>
- **PythonSensor:** <o que ele confere e por que isso importa>

## Evidências

<GIF em `evidencias/` ou link do vídeo no Google Drive, compartilhado com fahad.kalil@atitus.edu.br>

O vídeo mostra: <(1) upload no Alarik, (2) DAG no Airflow, (3) mensagens no Kafbat>.

## Problemas que enfrentamos

<2 ou 3 problemas reais e como resolveram>

## Divisão do trabalho

<quem fez o quê>

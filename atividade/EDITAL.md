# Atividade: do Parquet no Alarik ao Kafka, orquestrado pelo Airflow

Em grupo (até 4 alunos), vocês vão montar uma **DAG do Apache Airflow** que fica de olho no **Alarik (S3)**: quando um arquivo
**Parquet** é enviado para a pasta `entrada/`, a DAG o detecta com um **sensor**, lê o arquivo com **PyArrow** e publica
**cada linha como uma mensagem JSON** no seu **tópico Kafka**, que vocês enxergam no **Kafbat**.

```
.parquet -> Alarik (entrada/) -> S3KeySensor (detecta)
   -> task Python: PyArrow le o arquivo, publica cada
      linha no Kafka e move o arquivo p/ processados/
   -> PythonSensor: confere que as mensagens chegaram
   -> Kafbat (http://localhost:8085) mostra o topico
```

**Vale 2 pontos na G2** (nota de trabalho). Aula de abertura: 13/11/2026 (a aula inteira é para a atividade).
**Prazo de entrega: 22/11/2026 (domingo), às 23:59.**

## O que entregar

Um **repositório privado no GitHub** (um por grupo, nome sugerido `airflow-grupoNN`) com o professor (usuário GitHub `fahadkalil`) como
**colaborador**. Não precisa ser um fork completo: só estes arquivos, nos mesmos caminhos do `bigdata_airflow`:

| Arquivo | O que é |
|---|---|
| `airflow/dags/dag_grupo.py` | a sua DAG (parta do `atividade/modelo_dag.py`) |
| `atividade/amostra_kafka.json` | gerado por `exportar_amostra.py` (não edite!) |
| `evidencias/` | o **GIF** (até 20 MB) **ou** o link de um vídeo de até 2 min no `README.md`, hospedado no **Google Drive e compartilhado com `fahad.kalil@atitus.edu.br`** (confira se o link abre para esse e-mail) |
| `README.md` | modelo em `atividade/README_ENTREGA.md`: integrantes, decisões e como rodar |
| `docker-compose.yml`, `Dockerfile` | só se vocês tiverem alterado algo neles (e expliquem no README) |

**A evidência (vídeo ou GIF) precisa mostrar, em sequência:** (1) o arquivo chegando em `entrada/` no console do Alarik;
(2) a DAG no Airflow: sensor esperando e depois as tarefas ficando verdes; (3) o tópico `gNN_parquet` no Kafbat com as mensagens e
o arquivo indo para `processados/`. Não mostre senhas pessoais; a `CHAVE_GRUPO` pode aparecer.

## Pontuação (10 pontos, que valem 2 pontos na G2)

| Critério | Pontos |
|---|---|
| Ambiente e conexão (Airflow sobe, conexão `alarik_s3` funcionando, DAG aparece e roda) | 1 |
| `S3KeySensor` bem configurado (curinga, `mode`, `poke_interval`, `timeout`) e a escolha justificada no README | 2 |
| Leitura com PyArrow **em lotes** e serialização correta (Decimal e datas) | 2 |
| Publicação no Kafka (tópico do grupo, `flush`, todas as linhas, assinaturas intactas em `amostra_kafka.json`) | 2 |
| Idempotência: o arquivo processado sai de `entrada/` e rodar de novo não duplica mensagens | 1 |
| `PythonSensor` confirmando a chegada no Kafka | 1 |
| Evidência (vídeo/GIF) e README claros | 1 |

Trabalho idêntico entre grupos (cópia) zera os critérios copiados: os dados de cada grupo são diferentes e a assinatura
das linhas identifica de quem é cada mensagem.

## Passo a passo

### 1. Preparar o ambiente (uma vez, na "máquina oficial" do grupo)

```bash
# a ordem importa: a rede bigdata_docker_default é criada pela primeira stack
cd bigdata_docker             && git pull && docker compose up -d --pull=always kafka kafbat-ui
cd ../bigdata_datalake_docker && git pull && docker compose up -d --pull=always --force-recreate
cd ../bigdata_airflow         && git pull
cp atividade/.env.exemplo .env          # edite GRUPO_ID (ex.: 07) e CHAVE_GRUPO (da sua folha)
docker compose pull                      # imagem pronta; se falhar:  docker compose build
docker compose up -d --force-recreate
```

**Esta máquina já rodou o Alarik antes** (outra aula ou outro grupo)? Ele pode ter sido criado com chaves antigas, e o Airflow falha com `InvalidAccessKeyId`.
O `--force-recreate` dos comandos acima já recria os contêineres com a configuração atual. Se mesmo assim continuar falhando, na pasta `bigdata_datalake_docker`
rode `docker compose down -v` e depois `docker compose up -d` (**apaga os dados do Alarik desta máquina**).

Interfaces: **Airflow** http://localhost:8086 (`admin`/`admin`) · **Kafbat** http://localhost:8085 ·
**Console do Alarik** http://localhost:3000 (`alarik`/`alarik`, bucket `default`).

O compose já cria a conexão `alarik_s3` (veja em *Admin → Connections*). A DAG nasce **pausada**: ligue a chave na interface.

### 2. Gerar e enviar um Parquet

```bash
# gera o arquivo do SEU grupo (dados diferentes dos outros) e envia para entrada/ no Alarik
docker compose exec airflow-scheduler python /opt/airflow/atividade/gerar_parquet.py --upload
```

Para um segundo arquivo (outro conteúdo): acrescente `--lote 2 --linhas 300`. Sem `--upload` o arquivo só é salvo em
`atividade/saida/`, e vocês podem enviá-lo pelo console do Alarik. Colunas: `grupo, lote, id_evento, pedido_id, cliente,
zona, valor (decimal), criado_em (timestamp), status, observacao (pode ser nula), assinatura`.

### 3. Escrever a DAG

Copie `atividade/modelo_dag.py` para `airflow/dags/dag_grupo.py` e complete os `TODO`. O Airflow relê a pasta a cada ~10 s.

Tarefas esperadas:

1. Ambiente no ar (Kafka, Alarik e Airflow) e a DAG do grupo aparecendo na interface do Airflow, sem erro.
2. Gerar um Parquet do grupo e enviá-lo para `entrada/` no Alarik.
3. Configurar o `S3KeySensor` para detectar os arquivos `.parquet` em `entrada/`.
4. Ler o Parquet com PyArrow, em lotes, e serializar cada linha como JSON (atenção a `Decimal` e `datetime`).
5. Publicar cada linha no tópico Kafka do grupo e dar `flush` ao final.
6. Mover o arquivo processado de `entrada/` para `processados/`, para que a DAG não o processe de novo.
7. Criar o `PythonSensor` que confere se as mensagens chegaram ao tópico.
8. Gerar a amostra (`amostra_kafka.json`), gravar o vídeo ou GIF, preencher o `README.md` e entregar o repositório.

### 4. Gerar as evidências e a amostra

```bash
docker compose exec airflow-scheduler python /opt/airflow/atividade/exportar_amostra.py   # cria atividade/amostra_kafka.json
```

Grave a tela (macOS: `Shift+Cmd+5`; Windows: `Win+G`) enviando um arquivo novo (`--lote 3`, por exemplo) e acompanhando
Alarik, Airflow e Kafbat.

## Armadilhas conhecidas (leiam antes de perder uma hora)

- **O sensor dispara para sempre** se o arquivo continuar em `entrada/`. Por isso o arquivo é movido depois de publicado.
  Publique **antes** de mover: se a task falhar no meio, o arquivo continua lá e a próxima execução tenta de novo.
- `json.dumps` quebra com `Decimal` e `datetime` (`TypeError: Object of type Decimal is not JSON serializable`).
  Converta para texto, **sem arredondar nem alterar** (o professor confere a assinatura de cada linha).
- `InvalidAccessKeyId` ou `403` ao falar com o Alarik: ele foi criado antes com outras chaves. Veja o aviso do passo 1 (se o `--force-recreate` não resolver, `down -v`).
- Dentro dos contêineres, os endereços são `kafka:9092` e `alarik:8080` (**não** `localhost`). No seu navegador é o contrário.
- `KafkaProducer.send()` é assíncrono: sem `flush()` a task termina antes das mensagens saírem.
- Sensor em `mode="poke"` ocupa uma vaga de execução o tempo todo; `mode="reschedule"` libera a vaga entre as verificações.
  Para uma DAG que fica sempre de vigia, estudem `schedule="@continuous"` com `max_active_runs=1`.
  Se o sensor estoura o `timeout`, a task falha (`soft_fail=True` marca como *skipped* em vez de falha).
- Mensagem de erro de DAG ao abrir a interface (*Broken DAG*): rode `docker compose exec airflow-scheduler python /opt/airflow/dags/dag_grupo.py`.
- A conexão `alarik_s3` usa endereçamento por caminho (`path`), já configurado: não troque.

## Desafios extras (não valem nota; valem a experiência)

- O Alarik consegue avisar por **webhook** quando há objeto novo. Em vez de sondar (sensor), como seria uma DAG disparada por esse aviso?
  Comparem as duas abordagens no README (latência, carga, complexidade).
- Garantir que um mesmo arquivo nunca seja publicado duas vezes mesmo se a task for repetida (dica: `id_evento` como chave da mensagem).

## Regras

- Um computador por grupo roda a stack, mas todos devem entender e poder explicar a solução.
- Pode consultar a documentação do Airflow, PyArrow e kafka-python, e usar ferramentas de IA, desde que vocês consigam explicar cada linha.
- Não publique este repositório (privado) nem compartilhe a `CHAVE_GRUPO` com outros grupos.
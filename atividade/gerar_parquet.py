"""
Gerador de arquivos Parquet da atividade (Airflow + Alarik + Kafka).

Cada grupo recebe dados DIFERENTES (a semente vem da CHAVE_GRUPO) e cada linha leva uma `assinatura`
que o professor confere na correção: os dados que chegam ao Kafka precisam ser os do seu grupo.

Rode DENTRO do contêiner do Airflow (já tem pyarrow e a conexão com o Alarik):

    docker compose exec airflow-scheduler python /opt/airflow/atividade/gerar_parquet.py --upload
    docker compose exec airflow-scheduler python /opt/airflow/atividade/gerar_parquet.py --upload --lote 2 --linhas 300

Sem --upload, o arquivo só é salvo em atividade/saida/ (pasta do seu computador), e você pode enviá-lo pelo
console do Alarik (http://localhost:3000) para a pasta  entrada/  do bucket  default.

O grupo e a chave vêm das variáveis GRUPO_ID e CHAVE_GRUPO (arquivo .env) ou de --grupo/--chave.
"""
import argparse
import hashlib
import hmac
import os
import random
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = os.environ.get("ALARIK_BUCKET", "default")
PREFIXO_ENTRADA = "entrada/"
CONEXAO = "alarik_s3"

ZONAS = ["Centro", "Norte", "Sul", "Leste", "Oeste", "Industrial", "Universitária"]
STATUS = ["CRIADO", "PREPARANDO", "A_CAMINHO", "ENTREGUE", "CANCELADO"]
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabi", "Hugo", "Iara", "João", "Karen", "Lucas",
         "Marina", "Nando", "Olívia", "Paulo", "Quésia", "Rafa", "Sônia", "Tiago"]
OBSERVACOES = ["sem cebola", "entregar na portaria", "troco para 50", "campainha quebrada", "ligar ao chegar"]

ESQUEMA = pa.schema([
    ("grupo", pa.string()),
    ("lote", pa.int32()),
    ("id_evento", pa.string()),
    ("pedido_id", pa.int64()),
    ("cliente", pa.string()),
    ("zona", pa.string()),
    ("valor", pa.decimal128(10, 2)),
    ("criado_em", pa.timestamp("us")),
    ("status", pa.string()),
    ("observacao", pa.string()),          # pode ser nula
    ("assinatura", pa.string()),
])


def assinar(chave: str, grupo: str, id_evento: str, pedido_id, valor, criado_em) -> str:
    """HMAC da linha. Os campos entram como TEXTO, exatamente como saem do json.dumps(default=str)."""
    msg = f"{grupo}|{id_evento}|{pedido_id}|{valor}|{criado_em}"
    return hmac.new(chave.encode(), msg.encode(), hashlib.sha256).hexdigest()[:16]


def gerar_linhas(grupo: str, chave: str, lote: int, linhas: int):
    """Determinístico: o mesmo (chave, lote) sempre produz as mesmas linhas."""
    semente = int(hashlib.sha256(f"{chave}|lote{lote}".encode()).hexdigest()[:16], 16)
    rnd = random.Random(semente)
    base = datetime(2026, 11, 13, 19, 35, 0)
    for i in range(1, linhas + 1):
        id_evento = f"g{grupo}-l{lote:02d}-{i:05d}"
        pedido_id = lote * 100000 + i
        valor = Decimal(rnd.randint(1500, 18990)) / 100
        valor = valor.quantize(Decimal("0.01"))
        criado_em = base + timedelta(seconds=rnd.randint(0, 3 * 3600))
        yield {
            "grupo": grupo,
            "lote": lote,
            "id_evento": id_evento,
            "pedido_id": pedido_id,
            "cliente": rnd.choice(NOMES),
            "zona": rnd.choice(ZONAS),
            "valor": valor,
            "criado_em": criado_em,
            "status": rnd.choice(STATUS),
            "observacao": rnd.choice(OBSERVACOES) if rnd.random() < 0.3 else None,
            "assinatura": assinar(chave, grupo, id_evento, pedido_id, valor, criado_em),
        }


def montar_tabela(grupo: str, chave: str, lote: int, linhas: int) -> pa.Table:
    registros = list(gerar_linhas(grupo, chave, lote, linhas))
    tabela = pa.Table.from_pylist(registros, schema=ESQUEMA)
    meta = {b"grupo": grupo.encode(), b"lote": str(lote).encode()}
    return tabela.replace_schema_metadata(meta)


def enviar(caminho: Path, chave_s3: str) -> None:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook      # só existe dentro do contêiner do Airflow
    hook = S3Hook(aws_conn_id=CONEXAO)
    hook.load_file(filename=str(caminho), key=chave_s3, bucket_name=BUCKET, replace=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--grupo", default=os.environ.get("GRUPO_ID", ""), help="número do grupo, ex.: 07")
    ap.add_argument("--chave", default=os.environ.get("CHAVE_GRUPO", ""), help="CHAVE_GRUPO da sua folha")
    ap.add_argument("--linhas", type=int, default=1000, help="linhas por arquivo (padrão 1000)")
    ap.add_argument("--lote", type=int, default=1, help="número do 1º lote (muda o conteúdo do arquivo)")
    ap.add_argument("--arquivos", type=int, default=1, help="quantos arquivos gerar (lotes consecutivos)")
    ap.add_argument("--saida", default=str(Path(__file__).resolve().parent / "saida"))
    ap.add_argument("--upload", action="store_true", help="envia para entrada/ no Alarik")
    a = ap.parse_args(argv)

    if not a.grupo or not a.chave:
        print("Defina GRUPO_ID e CHAVE_GRUPO no .env (ou use --grupo e --chave).", file=sys.stderr)
        return 2
    grupo = f"{int(a.grupo):02d}" if a.grupo.isdigit() else a.grupo
    if not 1 <= a.linhas <= 200000 or not 1 <= a.arquivos <= 50:
        print("--linhas deve estar entre 1 e 200000 e --arquivos entre 1 e 50.", file=sys.stderr)
        return 2

    pasta = Path(a.saida)
    pasta.mkdir(parents=True, exist_ok=True)
    for lote in range(a.lote, a.lote + a.arquivos):
        nome = f"g{grupo}_lote{lote:02d}.parquet"
        caminho = pasta / nome
        tabela = montar_tabela(grupo, a.chave, lote, a.linhas)
        pq.write_table(tabela, caminho, compression="snappy")
        msg = f"gerado {caminho} ({tabela.num_rows} linhas)"
        if a.upload:
            enviar(caminho, PREFIXO_ENTRADA + nome)
            msg += f" -> enviado para s3://{BUCKET}/{PREFIXO_ENTRADA}{nome}"
        print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())

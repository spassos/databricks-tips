"""L5 · Data skipping em tabela Delta: função na coluna vs filtro em intervalo (+ OPTIMIZE ZORDER BY).

Ticket do cliente: "Filtering one day of events scans the whole table, even though the table holds 30 days of data."

Como rodar:  python local/L5_data_skipping_delta.py        (ou F5 com a configuração "L5 · data skipping")
Spark UI:    http://localhost:4040 → SQL / DataFrame → clique na query → nó de scan:
             'number of files read' (arquivos lidos) e o plano físico (PushedFilters / DataFilters)

Nota: no Databricks o nó de scan mostra também 'files pruned'. No Delta open-source o skipping acontece ANTES do scan
(no índice de arquivos), então o sinal é 'number of files read' cair em relação ao total de arquivos da tabela
(arquivos podados = total - lidos).
"""
import json

from delta.tables import DeltaTable
from spark_session import (
    DATA_DIR,
    cronometrar,
    get_spark,
    limpar_dir,
    metricas_scan_sql,
    pausar_ui,
    titulo,
)
from pyspark.sql import functions as F

CONFS = {"spark.driver.memory": "4g"}
N = 4_000_000
spark = get_spark("L5_data_skipping_delta", **CONFS)

PATH = (DATA_DIR / "l5_events").as_posix()
TABELA = f"delta.`{PATH}`"


def total_arquivos():
    return spark.sql(f"DESCRIBE DETAIL {TABELA}").first()["numFiles"]


def consulta(rotulo, sql):
    """Roda a query, mostra tempo e quantos arquivos o scan leu (aba SQL → nó de scan)."""
    (r, _) = cronometrar(rotulo, lambda: spark.sql(sql).collect())
    metricas_scan_sql(spark, ultimas=1)
    return r[0][0]


# ----------------------------------------------------------------------------------------------
titulo("1. REPRODUZIR — tabela Delta ordenada por ts, filtrada com função na coluna")
# ----------------------------------------------------------------------------------------------
limpar_dir(PATH)
eventos = (
    spark.range(N)
    .withColumn("ts", (F.lit(1735689600) + (F.col("id") * F.lit(0.65)).cast("long")).cast("timestamp"))   # ~30 dias
    .withColumn("user_id", (F.rand(3) * 100_000).cast("long"))
    .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
    .select("id", "ts", "user_id", "payload")
)
# repartitionByRange(60, ts): cada arquivo cobre uma faixa estreita de ts → o skipping PODERIA funcionar
eventos.repartitionByRange(60, "ts").write.format("delta").mode("overwrite").save(PATH)
print("arquivos na tabela:", total_arquivos())

Q_RUIM = f"SELECT count(*) FROM {TABELA} WHERE date_format(ts, 'yyyy-MM-dd') = '2025-01-15'"
n_ruim = consulta("filtro com função na coluna", Q_RUIM)
print("linhas do dia 2025-01-15:", n_ruim)

# ----------------------------------------------------------------------------------------------
titulo("2. DIAGNOSTICAR — plano físico, arquivos lidos e estatísticas")
# ----------------------------------------------------------------------------------------------
print("""
Spark UI (http://localhost:4040):
  • SQL / DataFrame → clique na query (descrição = o 'count') → grafo do plano:
      - nó 'Scan parquet': 'number of files read' ≈ total de arquivos da tabela (aqui, os 60) e 'size of files read' ≈ tamanho todo
      - o Filter vem DEPOIS do scan (a função é aplicada linha a linha), em vez de ser empurrado para a leitura
  • No texto do plano (Details / EXPLAIN): 'PushedFilters: []' (ou sem o predicado sobre ts) e 'DataFilters' sem ts.
  • Jobs / Stages: o job lê tudo (Input Size do stage ≈ tamanho da tabela).
  • Delta: as estatísticas min/max por arquivo ficam no _delta_log (coluna 'stats' de cada 'add'); só predicados
    diretos na coluna conseguem usá-las.
""")
spark.sql(Q_RUIM).explain(mode="formatted")

print("== DESCRIBE DETAIL ==")
spark.sql(f"DESCRIBE DETAIL {TABELA}").select("numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)

print("== Estatísticas min/max do 1º arquivo no _delta_log ==")
with open(f"{PATH}/_delta_log/00000000000000000000.json") as fh:
    for linha in fh:
        acao = json.loads(linha)
        if "add" in acao:
            stats = json.loads(acao["add"]["stats"])
            print("arquivo:", acao["add"]["path"][:40], "...")
            print("minValues.ts =", stats["minValues"]["ts"], "| maxValues.ts =", stats["maxValues"]["ts"])
            print("minValues.user_id =", stats["minValues"]["user_id"], "| maxValues.user_id =", stats["maxValues"]["user_id"])
            break

# ----------------------------------------------------------------------------------------------
titulo("3. CORRIGIR — filtro em intervalo + OPTIMIZE ZORDER BY")
# ----------------------------------------------------------------------------------------------
Q_BOM = (f"SELECT count(*) FROM {TABELA} "
         "WHERE ts >= TIMESTAMP '2025-01-15 00:00:00' AND ts < TIMESTAMP '2025-01-16 00:00:00'")
n_bom = consulta("a) filtro em INTERVALO direto na coluna", Q_BOM)
print(f"mesmo resultado? {n_ruim == n_bom} | arquivos totais: {total_arquivos()}  → compare com 'files read' acima")
spark.sql(Q_BOM).explain(mode="formatted")

# b) Lookup por user_id: a tabela está ordenada por ts, então user_id NÃO tem skipping (min/max de cada arquivo cobre quase tudo)
Q_USER = f"SELECT count(*) FROM {TABELA} WHERE user_id = 123"
consulta("b) lookup por user_id ANTES do ZORDER", Q_USER)

# c) OPTIMIZE ZORDER BY (user_id): reorganiza os dados por user_id e recalcula min/max por arquivo.
#    maxFileSize pequeno só porque o dataset local é pequeno (padrão 1 GB geraria um único arquivo e nada a podar).
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(4 * 1024 * 1024))
cronometrar("OPTIMIZE ZORDER BY (user_id)", lambda: spark.sql(f"OPTIMIZE {TABELA} ZORDER BY (user_id)").collect())
print("arquivos após o OPTIMIZE:", total_arquivos())
consulta("   lookup por user_id DEPOIS do ZORDER", Q_USER)

print("== Histórico: operação OPTIMIZE com zOrderBy ==")
(DeltaTable.forPath(spark, PATH).history()
 .select("version", "operation", "operationParameters").show(5, truncate=80))

print("""
Prevenção de longo prazo:
  • Filtros SEM função/cast no lado da coluna (ts >= ... AND ts < ...); coluna de data derivada (generated column) se o filtro for por dia.
  • Organizar o layout pelos filtros mais comuns: ZORDER BY (Delta OSS) ou liquid clustering (CLUSTER BY, Databricks/Delta 3.1+).
  • Manter as colunas de filtro entre as primeiras 32 do schema (delta.dataSkippingNumIndexedCols) e rodar OPTIMIZE com regularidade.
  • Conferir 'files read' vs total na aba SQL depois de mudar uma query ou o layout.
""")

# ----------------------------------------------------------------------------------------------
titulo("4. AO CLIENTE")
# ----------------------------------------------------------------------------------------------
print("""
The query was slow because it applied a function to the timestamp column inside the WHERE clause, so Delta could not use the per-file minimum and maximum statistics and the scan node in the Spark UI read every file in the table. As an immediate fix, we rewrote the filter as a plain range on the column, which lets Delta skip every file outside that day, and we ran OPTIMIZE with ZORDER BY on the user identifier so that lookups on that column also skip most files. For the long term, please keep filters free of functions on the filtered column, choose the clustering columns from your most common predicates, and check the number of files read in the SQL tab after each change.
""")

pausar_ui(spark)

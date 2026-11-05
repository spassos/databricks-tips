# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Delta Lake — D1 a D5
# MAGIC
# MAGIC | ID | Cenário |
# MAGIC |---|---|
# MAGIC | D1 | `MERGE` com múltiplas linhas de origem para a mesma linha de destino |
# MAGIC | D2 | Schema mismatch no `append` |
# MAGIC | D3 | Time travel, `RESTORE` e `VACUUM` |
# MAGIC | D4 | Small files, `OPTIMIZE` e liquid clustering |
# MAGIC | D5 | Escrita concorrente (`ConcurrentModificationException`) |
# MAGIC
# MAGIC Todos os cenários recriam suas tabelas (`CREATE OR REPLACE`), então podem ser rodados de novo sem limpeza manual.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1 — MERGE com múltiplas linhas de origem
# MAGIC
# MAGIC **Ticket do cliente:** "Our MERGE has run fine for months and today fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`."
# MAGIC
# MAGIC ### 1. Reproduzir — D1
# MAGIC
# MAGIC A origem traz **duas** atualizações para o `customer_id = 1` (e duas para o 2). O Delta se recusa a escolher qual delas vale.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE lab.d1_customers (customer_id INT, name STRING, tier STRING, updated_at TIMESTAMP)")
spark.sql("""
INSERT INTO lab.d1_customers VALUES
  (1, 'Ana',   'silver', TIMESTAMP '2025-01-01 10:00:00'),
  (2, 'Bruno', 'gold',   TIMESTAMP '2025-01-01 10:00:00'),
  (3, 'Carla', 'silver', TIMESTAMP '2025-01-01 10:00:00')
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW d1_updates AS
SELECT * FROM VALUES
  (1, 'Ana',   'gold',     TIMESTAMP '2025-02-01 09:00:00'),
  (1, 'Ana',   'platinum', TIMESTAMP '2025-02-01 18:00:00'),   -- duplicada
  (2, 'Bruno', 'silver',   TIMESTAMP '2025-02-01 09:00:00'),
  (2, 'Bruno', 'silver',   TIMESTAMP '2025-02-01 09:00:00'),   -- duplicada exata
  (4, 'Davi',  'silver',   TIMESTAMP '2025-02-01 09:00:00')
AS t(customer_id, name, tier, updated_at)
""")

MERGE_SQL = """
MERGE INTO lab.d1_customers AS t
USING {src} AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

expect_error(lambda: spark.sql(MERGE_SQL.format(src="d1_updates")), "D1 · MERGE com origem duplicada")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — D1
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - Chaves duplicadas na **origem** (`GROUP BY chave HAVING count(*) > 1`).
# MAGIC - `DESCRIBE HISTORY` do destino: **nenhuma versão nova** (o MERGE falhou de forma atômica, nada foi gravado).
# MAGIC - Se as duplicatas são idênticas (dedupe trivial) ou conflitantes (precisa de regra: mais recente, maior prioridade...).
# MAGIC - Pergunta ao cliente: "o que mudou na origem?" (reprocessamento, join que multiplica linhas, CDC com vários eventos por chave no mesmo lote).

# COMMAND ----------

print("== Chaves duplicadas na origem ==")
display(spark.sql("SELECT customer_id, count(*) AS n FROM d1_updates GROUP BY customer_id HAVING count(*) > 1"))

print("== Duplicatas idênticas ou conflitantes? ==")
display(spark.sql("SELECT customer_id, count(*) AS linhas, count(DISTINCT tier) AS tiers_distintos FROM d1_updates GROUP BY customer_id"))

print("== Histórico do destino (sem versão de MERGE: nada foi gravado) ==")
display(history("lab.d1_customers").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — D1
# MAGIC
# MAGIC **Fix imediato:** deduplicar a origem com regra determinística (`ROW_NUMBER` + `QUALIFY`, mais recente vence) antes do MERGE.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Dedupe como etapa explícita do pipeline + *assert* de unicidade da chave antes do MERGE.
# MAGIC - Condição no `WHEN MATCHED AND s.updated_at > t.updated_at` para evitar regredir dados.
# MAGIC - Para CDC, usar `AUTO CDC` / `APPLY CHANGES` (Lakeflow Declarative Pipelines) com `SEQUENCE BY`, que resolve ordenação e duplicatas.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW d1_updates_dedup AS
SELECT * FROM d1_updates
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
""")

# Guard-rail: a chave tem que ser única antes do MERGE
dups = spark.sql("SELECT count(*) - count(DISTINCT customer_id) FROM d1_updates_dedup").first()[0]
assert dups == 0, f"origem ainda tem {dups} chaves duplicadas"

spark.sql(MERGE_SQL.format(src="d1_updates_dedup"))
display(spark.table("lab.d1_customers").orderBy("customer_id"))
display(history("lab.d1_customers").select("version", "operation", "operationMetrics").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — D1
# MAGIC
# MAGIC > The MERGE failed because the source contained more than one row for the same customer key, and Delta refuses to guess which row should win when several source rows match one target row. As an immediate fix, we deduplicated the source with a deterministic rule that keeps the most recent record per key, after which the MERGE completed successfully. For the long term, please make deduplication an explicit pipeline step with a uniqueness check on the key, or use the built-in change data capture support, which orders events by a sequence column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2 — Schema mismatch no append
# MAGIC
# MAGIC **Ticket do cliente:** "Since this morning our append fails with 'A schema mismatch detected when writing to the Delta table'."
# MAGIC
# MAGIC ### 1. Reproduzir — D2
# MAGIC
# MAGIC O produtor passou a enviar uma coluna nova (`channel`) e `amount`/`sold_at` chegam como string.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE lab.d2_sales (sale_id INT, amount DOUBLE, sold_at DATE)")
spark.sql("INSERT INTO lab.d2_sales VALUES (1, 10.5, DATE '2025-01-01'), (2, 20.0, DATE '2025-01-02')")

new_batch = spark.createDataFrame(
    [(3, "30.5", "2025-01-03", "web")],
    "sale_id INT, amount STRING, sold_at STRING, channel STRING",
)

expect_error(lambda: new_batch.write.mode("append").saveAsTable("lab.d2_sales"), "D2 · append com schema diferente")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — D2
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - A mensagem de erro lista **os dois schemas** (tabela vs dados novos): compare coluna a coluna.
# MAGIC - Colunas **novas** (evolução aditiva) vs colunas com **tipo diferente** (quebra de contrato) vs colunas **ausentes**.
# MAGIC - `DESCRIBE DETAIL` / `DESCRIBE HISTORY`: quando o schema da tabela mudou pela última vez (`ALTER`, `CHANGE COLUMN`)?
# MAGIC - Pergunta ao cliente: foi mudança intencional do produtor ou bug?

# COMMAND ----------

target = {f.name: f.dataType.simpleString() for f in spark.table("lab.d2_sales").schema}
incoming = {f.name: f.dataType.simpleString() for f in new_batch.schema}

print("== Diff de schema (tabela vs dados novos) ==")
for col in sorted(set(target) | set(incoming)):
    t, i = target.get(col), incoming.get(col)
    status = "ok" if t == i else ("NOVA no dado" if t is None else ("AUSENTE no dado" if i is None else "TIPO DIFERENTE"))
    print(f"{col:10} tabela={t!s:8} dado={i!s:8} → {status}")

display(spark.sql("DESCRIBE DETAIL lab.d2_sales").select("name", "createdAt", "lastModified", "numFiles", "properties"))
display(history("lab.d2_sales").select("version", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — D2
# MAGIC
# MAGIC **Fix imediato:**
# MAGIC 1. Coluna nova **intencional** → `mergeSchema=true` no append (ou `ALTER TABLE ... ADD COLUMN` antes).
# MAGIC 2. Tipo diferente → **cast explícito** para o tipo da tabela. `mergeSchema` **não** resolve conflito de tipo.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Contrato de dados com o produtor + validação do schema na entrada (schema enforcement é uma *feature*: protege a tabela).
# MAGIC - Ingestão com Auto Loader (`schemaEvolutionMode`, `rescuedDataColumn`) para absorver coluna nova com controle.
# MAGIC - Em MERGE, evolução automática via `spark.databricks.delta.schema.autoMerge.enabled` (use só se aceitar evolução silenciosa).

# COMMAND ----------

# mergeSchema NÃO resolve conflito de tipo (string vs double):
expect_error(
    lambda: new_batch.write.mode("append").option("mergeSchema", "true").saveAsTable("lab.d2_sales"),
    "D2 · mergeSchema com tipo conflitante",
)

# Correto: cast para o tipo da tabela + mergeSchema só para a coluna nova
fixed_batch = (
    new_batch
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("sold_at", F.to_date("sold_at"))
)
fixed_batch.write.mode("append").option("mergeSchema", "true").saveAsTable("lab.d2_sales")

display(spark.table("lab.d2_sales").orderBy("sale_id"))
spark.table("lab.d2_sales").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **requer cluster clássico** — a evolução automática de schema em `MERGE` depende de uma config de sessão que pode ser bloqueada no serverless.

# COMMAND ----------

try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    print("autoMerge habilitado nesta sessão (MERGE passa a evoluir schema)")
except Exception as e:  # noqa: BLE001
    print("bloqueado no serverless:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — D2
# MAGIC
# MAGIC > The append failed because the incoming data carried a new column and string values for columns that are numeric and date typed in the table, and Delta schema enforcement rejects any write that does not match the table schema. As an immediate fix, we cast the incoming columns to the table types and enabled schema merging only for the new column, so the batch was appended without losing data. For the long term, please agree on a data contract with the producer and ingest through Auto Loader with a rescued data column, so that schema changes are detected and reviewed instead of breaking the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D3 — Time travel, RESTORE e VACUUM
# MAGIC
# MAGIC **Ticket do cliente:** "Someone ran a DELETE on the orders table by mistake. Can we get the rows back? And we also ran VACUUM and now time travel fails."
# MAGIC
# MAGIC ### 1. Reproduzir — D3
# MAGIC
# MAGIC Duas tabelas:
# MAGIC - `lab.d3_orders`: o incidente (DELETE acidental) que vamos **recuperar** com `RESTORE`.
# MAGIC - `lab.d3_trap`: a armadilha do `VACUUM` com retenção zero, depois da qual o time travel **para de funcionar**.

# COMMAND ----------

# --- Incidente: DELETE acidental ---
spark.sql("CREATE OR REPLACE TABLE lab.d3_orders (order_id INT, region STRING, amount DOUBLE)")
spark.sql("INSERT INTO lab.d3_orders SELECT CAST(id AS INT), CASE WHEN id % 2 = 0 THEN 'EU' ELSE 'US' END, id * 10.0 FROM range(1, 101)")
spark.sql("UPDATE lab.d3_orders SET amount = amount * 1.1 WHERE region = 'US'")
spark.sql("DELETE FROM lab.d3_orders WHERE region = 'EU'")  # <- o acidente
print("linhas em d3_orders após o DELETE:", spark.table("lab.d3_orders").count(), "(eram 100)")

# --- Armadilha do VACUUM ---
spark.sql("CREATE OR REPLACE TABLE lab.d3_trap AS SELECT id FROM range(100)")
spark.sql("INSERT INTO lab.d3_trap SELECT id FROM range(100, 200)")
v_trap = history("lab.d3_trap").agg(F.max("version")).first()[0]   # versão com 200 linhas
spark.sql("DELETE FROM lab.d3_trap WHERE id < 150")

# 1) A checagem de segurança bloqueia retenção menor que a configurada na tabela
expect_error(lambda: spark.sql("VACUUM lab.d3_trap RETAIN 0 HOURS"), "D3 · VACUUM 0 HOURS (safety check)")

# 2) "Resolvendo" o erro baixando a retenção da tabela: o VACUUM passa...
spark.sql("ALTER TABLE lab.d3_trap SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')")
spark.sql("VACUUM lab.d3_trap RETAIN 0 HOURS")

# 3) ...e o time travel para a versão antiga quebra, porque os arquivos foram apagados fisicamente
expect_error(
    lambda: spark.sql(f"SELECT count(*) FROM lab.d3_trap VERSION AS OF {v_trap}").show(),
    f"D3 · time travel para a versão {v_trap} após VACUUM",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — D3
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - `DESCRIBE HISTORY`: qual `operation` (`DELETE`), **quando**, **quem** (`userName`), `operationParameters.predicate` e `operationMetrics.numDeletedRows`; a **última versão boa** é a anterior ao DELETE.
# MAGIC - Comparar `VERSION AS OF <boa>` com a versão atual (`EXCEPT`) para dimensionar o estrago.
# MAGIC - Existência de operações `VACUUM START/END` no histórico e propriedades `delta.deletedFileRetentionDuration` / `delta.logRetentionDuration`: se o VACUUM rodou depois da versão desejada, **os dados físicos se foram**.
# MAGIC - `VACUUM ... DRY RUN` lista o que seria apagado sem apagar.

# COMMAND ----------

h = history("lab.d3_orders")
display(h.select("version", "timestamp", "userName", "operation", "operationParameters", "operationMetrics"))

v_delete = h.filter("operation = 'DELETE'").agg(F.max("version")).first()[0]
v_good = v_delete - 1
print(f"DELETE na versão {v_delete} → última versão boa = {v_good}")

lost = spark.sql(f"SELECT * FROM lab.d3_orders VERSION AS OF {v_good}").exceptAll(spark.table("lab.d3_orders"))
print("linhas perdidas pelo DELETE:", lost.count())

print("== Propriedades de retenção e histórico de VACUUM da armadilha ==")
display(spark.sql("DESCRIBE DETAIL lab.d3_trap").select("properties"))
display(history("lab.d3_trap").filter("operation LIKE 'VACUUM%'").select("version", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — D3
# MAGIC
# MAGIC **Fix imediato:** `RESTORE TABLE ... TO VERSION AS OF <última versão boa>` (cria uma **nova** versão; o histórico é preservado).
# MAGIC Se o `VACUUM` já apagou os arquivos, o `RESTORE` falha: só resta reconstruir da origem/backup.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Não baixar retenção do `VACUUM` abaixo de 7 dias; manter `delta.deletedFileRetentionDuration` e `delta.logRetentionDuration` alinhados ao RPO.
# MAGIC - `SHALLOW CLONE` (ou `DEEP CLONE`) antes de operações destrutivas.
# MAGIC - Tabelas **dropadas** (UC managed): `UNDROP TABLE` dentro da janela de retenção.
# MAGIC - Permissões de `MODIFY` restritas; DML destrutivo via job com revisão, não ad-hoc.

# COMMAND ----------

h = history("lab.d3_orders")
v_delete = h.filter("operation = 'DELETE'").agg(F.max("version")).first()[0]
v_good = v_delete - 1

spark.sql("CREATE OR REPLACE TABLE lab.d3_orders_backup SHALLOW CLONE lab.d3_orders")   # prevenção: backup antes de mexer
spark.sql(f"RESTORE TABLE lab.d3_orders TO VERSION AS OF {v_good}")

print("linhas após RESTORE:", spark.table("lab.d3_orders").count(), "(devem ser 100)")
display(history("lab.d3_orders").select("version", "operation", "operationParameters", "operationMetrics").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — D3
# MAGIC
# MAGIC > The rows disappeared because an accidental DELETE was committed to the table, and Delta keeps every previous version in the transaction log, which is how we identified the last good version just before it. As an immediate fix, we ran RESTORE to that version, which brought back all rows and recorded the restore as a new version, and we warn that once VACUUM removes the old files with a short retention this recovery is no longer possible. For the long term, please keep the VACUUM retention at seven days or more, take a clone before destructive operations, and limit who can run DELETE on production tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D4 — Small files, OPTIMIZE e liquid clustering
# MAGIC
# MAGIC **Ticket do cliente:** "Point lookups on our clicks table got slower every week and our storage listing costs keep growing."
# MAGIC
# MAGIC ### 1. Reproduzir — D4
# MAGIC
# MAGIC 3M linhas gravadas em **400 arquivos minúsculos** (`repartition(400)` com optimize write desligado).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE lab.d4_clicks (id BIGINT, user_id BIGINT, ts TIMESTAMP, url STRING)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
""")

(
    spark.range(3_000_000)
    .select(
        "id",
        (F.rand(1) * 50_000).cast("long").alias("user_id"),
        (F.lit(1735689600) + F.col("id")).cast("timestamp").alias("ts"),
        F.concat(F.lit("/p/"), (F.col("id") % 500).cast("string")).alias("url"),
    )
    .repartition(400)
    .write.mode("append")
    .saveAsTable("lab.d4_clicks")
)

Q_D4 = "SELECT count(*) AS n FROM lab.d4_clicks WHERE user_id = 123"
timed("D4 · lookup ANTES", lambda: spark.sql(Q_D4).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — D4
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - `DESCRIBE DETAIL`: `numFiles` alto e `sizeInBytes / numFiles` **muito abaixo** do ideal (dezenas a centenas de MB; aqui poucas centenas de KB).
# MAGIC - `DESCRIBE HISTORY`: muitos `WRITE`/`MERGE`/`STREAMING UPDATE` pequenos, sem `OPTIMIZE`; `operationMetrics.numFiles`/`numOutputBytes` por commit.
# MAGIC - Query Profile: tempo gasto em *scan* com muitos arquivos lidos para pouco dado; `clusteringColumns`/`partitionColumns` vazios.
# MAGIC - Causa raiz provável: `repartition(N)` alto, streaming com trigger curto, partição por coluna de alta cardinalidade, optimize write desligado.

# COMMAND ----------

d = spark.sql("DESCRIBE DETAIL lab.d4_clicks").first()
avg_kb = d["sizeInBytes"] / max(d["numFiles"], 1) / 1024
print(f"numFiles={d['numFiles']} | sizeInBytes={d['sizeInBytes']:,} | tamanho médio por arquivo ≈ {avg_kb:,.0f} KB")
print("partitionColumns:", d["partitionColumns"], "| clusteringColumns:", d["clusteringColumns"])
display(history("lab.d4_clicks").select("version", "operation", "operationMetrics").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — D4
# MAGIC
# MAGIC **Fix imediato:** `OPTIMIZE` compacta os arquivos pequenos. Para lookups por `user_id`, `CLUSTER BY (user_id)` (liquid clustering) + `OPTIMIZE` organiza os dados para data skipping.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - **Predictive Optimization** (UC managed) para `OPTIMIZE`/`VACUUM` automáticos.
# MAGIC - `delta.autoOptimize.optimizeWrite` e `autoCompact` ligados nas tabelas de ingestão.
# MAGIC - Liquid clustering no lugar de `PARTITIONED BY` + `ZORDER` (mudar as chaves não exige reescrever a tabela inteira).
# MAGIC - Evitar `repartition(N)` alto antes de gravar; revisar triggers de streaming.

# COMMAND ----------

before = spark.sql("DESCRIBE DETAIL lab.d4_clicks").first()["numFiles"]
spark.sql("OPTIMIZE lab.d4_clicks")
after_optimize = spark.sql("DESCRIBE DETAIL lab.d4_clicks").first()["numFiles"]
print(f"numFiles: {before} → {after_optimize} após OPTIMIZE")

# Liquid clustering por user_id e reorganização dos dados existentes
spark.sql("ALTER TABLE lab.d4_clicks CLUSTER BY (user_id)")
try:
    spark.sql("OPTIMIZE lab.d4_clicks FULL")   # reclusteriza TODOS os arquivos (runtimes recentes)
except Exception as e:  # noqa: BLE001
    print("OPTIMIZE FULL indisponível, usando OPTIMIZE simples:", type(e).__name__)
    spark.sql("OPTIMIZE lab.d4_clicks")

d2 = spark.sql("DESCRIBE DETAIL lab.d4_clicks").first()
print(f"numFiles={d2['numFiles']} | clusteringColumns={d2['clusteringColumns']}")
timed("D4 · lookup DEPOIS", lambda: spark.sql(Q_D4).collect())
display(history("lab.d4_clicks").filter("operation = 'OPTIMIZE'").select("version", "operationMetrics").limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — D4
# MAGIC
# MAGIC > The table degraded because every write produced hundreds of very small files, so each query had to open and read a large number of files to find a small amount of data. As an immediate fix, we ran OPTIMIZE to compact the files and enabled liquid clustering on the lookup column, which reduces the number of files and lets Delta skip the ones that do not match. For the long term, please enable Predictive Optimization and optimized writes on the ingestion tables so that compaction happens automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D5 — Escrita concorrente
# MAGIC
# MAGIC **Ticket do cliente:** "Two of our jobs update the same table and one of them randomly fails with `ConcurrentAppendException` / `DELTA_CONCURRENT_...`."
# MAGIC
# MAGIC ### 1. Reproduzir — D5
# MAGIC
# MAGIC 8 threads fazem `UPDATE` na **mesma linha** ao mesmo tempo. Como é uma corrida, o número de falhas varia: rode a célula de novo se nenhuma ocorrer.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

spark.sql("CREATE OR REPLACE TABLE lab.d5_counter (id INT, v INT)")
spark.sql("INSERT INTO lab.d5_counter VALUES (1, 0), (2, 0)")


def bump(_):
    try:
        spark.sql("UPDATE lab.d5_counter SET v = v + 1 WHERE id = 1")
        return None
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e)[:160]}"


with ThreadPoolExecutor(max_workers=8) as ex:
    results = list(ex.map(bump, range(8)))

errors = [r for r in results if r]
final_v = spark.sql("SELECT v FROM lab.d5_counter WHERE id = 1").first()[0]
print(f"sucessos={8 - len(errors)} | falhas={len(errors)} | v final={final_v} (esperado 8)")
for err in errors[:3]:
    print("❌", err)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — D5
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - Nome da exceção: `ConcurrentAppendException` (arquivos adicionados no mesmo escopo lido), `ConcurrentDeleteReadException` (arquivo lido foi removido),
# MAGIC   `ConcurrentDeleteDeleteException`, `MetadataChangedException`, `ProtocolChangedException`.
# MAGIC - `DESCRIBE HISTORY`: `readVersion` vs `version` dos commits intercalados, `isolationLevel` (WriteSerializable/Serializable) e operações que se sobrepõem no tempo.
# MAGIC - `DESCRIBE DETAIL`: tabela **não particionada / sem clustering** → toda escrita "toca" tudo, então conflita.
# MAGIC - Os predicados do `UPDATE/MERGE/DELETE` cobrem a(s) mesma(s) partição(ões)/arquivo(s)? Sem predicado de partição, o Delta assume que leu a tabela inteira.

# COMMAND ----------

display(
    history("lab.d5_counter").select("version", "timestamp", "operation", "readVersion", "isolationLevel", "operationMetrics")
)
d = spark.sql("DESCRIBE DETAIL lab.d5_counter").first()
print("partitionColumns:", d["partitionColumns"], "| clusteringColumns:", d["clusteringColumns"], "| properties:", d["properties"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — D5
# MAGIC
# MAGIC **Fix imediato:** *retry* com backoff exponencial + jitter ao redor da operação (o Delta recomeça a transação do commit mais recente).
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Fazer os escritores tocarem dados **disjuntos** (partição/clustering + predicado explícito na condição do MERGE/UPDATE/DELETE).
# MAGIC - Deletion vectors / row-level concurrency reduzem conflitos quando as linhas são diferentes (a mesma linha ainda conflita).
# MAGIC - Ingestão concorrente como `append` puro; operações de manutenção (`OPTIMIZE`, `DELETE`) em janelas separadas.
# MAGIC - Serializar escritores quando a lógica exige (jobs com `max_concurrent_runs = 1`, fila).

# COMMAND ----------

import random


def with_retry(sql, max_tries=10):
    """Re-executa a DML quando há conflito de concorrência do Delta (backoff exponencial + jitter)."""
    for attempt in range(max_tries):
        try:
            return spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            text = f"{type(e).__name__} {e}"
            if "Concurrent" in text or "DELTA_CONCURRENT" in text or "MetadataChanged" in text:
                time.sleep(min(0.2 * 2**attempt, 5) * random.random() + 0.05)
                continue
            raise
    raise RuntimeError(f"desisti após {max_tries} tentativas: {sql}")


spark.sql("UPDATE lab.d5_counter SET v = 0")   # zera o contador para repetir o teste


def bump_safe(_):
    with_retry("UPDATE lab.d5_counter SET v = v + 1 WHERE id = 1")


with ThreadPoolExecutor(max_workers=8) as ex:
    list(ex.map(bump_safe, range(8)))

final_v = spark.sql("SELECT v FROM lab.d5_counter WHERE id = 1").first()[0]
print("v final =", final_v, "(esperado 8)")
assert final_v == 8

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — D5
# MAGIC
# MAGIC > The failures happen because two writers modified overlapping data in the same Delta table at the same time, and Delta optimistic concurrency aborts the transaction that commits second to keep the table consistent. As an immediate fix, we wrapped the write in a retry with exponential backoff, which made all concurrent updates succeed and produced the expected final result. For the long term, please design the writers so that they touch disjoint partitions or clusters with explicit predicates, and serialize jobs that must update the same rows.

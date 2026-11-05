# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Apache Spark — S1, S2, S3
# MAGIC
# MAGIC | ID | Cenário |
# MAGIC |---|---|
# MAGIC | S1 | Driver OOM com `collect()` / `toPandas()` |
# MAGIC | S2 | Data skew em join (chave quente + chaves nulas) |
# MAGIC | S3 | Query lenta sem data skipping (função na coluna vs filtro em intervalo) |
# MAGIC
# MAGIC Cada cenário: **1. Reproduzir → 2. Diagnosticar → 3. Corrigir → 4. Ao cliente**.
# MAGIC
# MAGIC > No serverless não há RDD, a Spark UI é substituída pelo **Query Profile** e várias `spark.conf` são bloqueadas.
# MAGIC > Onde isso importa, a célula está marcada com **requer cluster clássico** e protegida por `try/except`.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## S1 — Driver OOM com `collect()` / `toPandas()`
# MAGIC
# MAGIC **Ticket do cliente:** "Our nightly job dies with `OutOfMemoryError` / 'driver not responding' right after we convert the result to pandas."
# MAGIC
# MAGIC ### 1. Reproduzir — S1
# MAGIC
# MAGIC Tentamos trazer ~200 milhões de linhas para o driver. No serverless o sintoma pode aparecer como erro de tamanho de resultado
# MAGIC **ou** como reinício do kernel Python. Se o kernel reiniciar, reduza `N_S1` e rode de novo: o diagnóstico é o mesmo.

# COMMAND ----------

N_S1 = 200_000_000  # reduza (ex.: 50_000_000) se o kernel reiniciar antes da exceção aparecer

big = (
    spark.range(N_S1)
    .withColumn("payload", F.concat(F.lit("row-"), F.col("id").cast("string"), F.repeat(F.lit("x"), 50)))
)

expect_error(lambda: big.collect(), "S1a · big.collect()")
expect_error(lambda: big.toPandas(), "S1b · big.toPandas()")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — S1
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - Stack trace com `collect`, `toPandas`, `fetchall` → o dado está indo para o driver.
# MAGIC - Estimativa de tamanho em memória **muito maior** que a RAM do driver.
# MAGIC - Em cluster clássico: driver logs com `java.lang.OutOfMemoryError: Java heap space`,
# MAGIC   `Total size of serialized results ... is bigger than spark.driver.maxResultSize`,
# MAGIC   ou "Driver is up but is not responsive, likely due to GC"; métricas do cluster com memória do driver saturada.

# COMMAND ----------

big.explain(mode="formatted")

# Estima o tamanho em memória com uma amostra pequena
sample_pdf = big.limit(10_000).toPandas()
bytes_per_row = sample_pdf.memory_usage(deep=True).sum() / len(sample_pdf)
est_gb = bytes_per_row * N_S1 / 1e9
print(f"~{bytes_per_row:.0f} bytes/linha em pandas × {N_S1:,} linhas ≈ {est_gb:,.1f} GB no driver")

try:
    import psutil

    print(f"RAM disponível neste processo/container: {psutil.virtual_memory().available / 1e9:.1f} GB")
except Exception as e:  # noqa: BLE001
    print("psutil indisponível:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **requer cluster clássico** — ler/alterar `spark.driver.maxResultSize` e o tamanho do driver só faz sentido fora do serverless.

# COMMAND ----------

try:
    print("spark.driver.maxResultSize =", spark.conf.get("spark.driver.maxResultSize"))
except Exception as e:  # noqa: BLE001
    print("Config não disponível no serverless:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — S1
# MAGIC
# MAGIC **Fix imediato:** nunca trazer o dataset inteiro. Agregue no cluster, traga só uma amostra limitada, ou persista o resultado em tabela.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Regra de code review: `toPandas()` só depois de `limit()`/agregação.
# MAGIC - Para pandas em escala, usar `pandas API on Spark` ou `applyInPandas`/`mapInPandas` (processa por partição, no executor).
# MAGIC - Gravar saídas grandes em Delta/Volume e deixar o consumo para BI/ML ler de lá.
# MAGIC - Em cluster clássico: driver dimensionado para o resultado esperado e `spark.driver.maxResultSize` explícito.

# COMMAND ----------

# a) Agregar no cluster e trazer só o resumo
resumo = big.groupBy((F.col("id") % 10).alias("bucket")).agg(F.count("*").alias("n"))
display(resumo)

# b) Amostra limitada para inspeção local
amostra_pdf = big.limit(1_000).toPandas()
print("amostra:", amostra_pdf.shape)

# c) Persistir o que for grande em vez de coletar (re-executável: overwrite)
big.where("id % 100000 = 0").write.mode("overwrite").saveAsTable("lab.s1_result")
print("linhas gravadas:", spark.table("lab.s1_result").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — S1
# MAGIC
# MAGIC > The job failed because the code pulled the entire result set into the driver with collect() or toPandas(), and a single driver process cannot hold hundreds of millions of rows. As an immediate fix, we replaced the pull with an aggregation that runs on the cluster plus a bounded sample for local inspection, and the full result is now written to a Delta table. For the long term, please avoid collecting unbounded data, always apply a limit or an aggregation before toPandas(), and size the driver and its result limit explicitly if a large collect is truly required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## S2 — Data skew em join (chave quente + chaves nulas)
# MAGIC
# MAGIC **Ticket do cliente:** "A join between our fact table and the customer dimension used to take 2 minutes and now one task runs for 30 minutes while the others finish in seconds."
# MAGIC
# MAGIC ### 1. Reproduzir — S2
# MAGIC
# MAGIC Fato com 10M linhas: **50%** das linhas na chave `customer_id = 1` (chave quente) e **10%** com `customer_id` nulo.
# MAGIC Dimensão pequena. Forçamos *sort-merge join* com hint `MERGE` (no serverless não dá para desligar o broadcast pela config).

# COMMAND ----------

N_S2 = 10_000_000
N_KEYS = 1_000

fact_df = (
    spark.range(N_S2)
    .withColumn("r", F.rand(42))
    .withColumn(
        "customer_id",
        F.when(F.col("r") < 0.50, F.lit(1).cast("long"))                      # chave quente: 50%
        .when(F.col("r") < 0.60, F.lit(None).cast("long"))                    # nulos: 10%
        .otherwise((F.col("id") % N_KEYS) + 2),                               # resto uniforme (2..1001)
    )
    .withColumn("amount", F.rand(7) * 100)
    .drop("r")
)
fact_df.write.mode("overwrite").saveAsTable("lab.s2_fact")

dim_df = (
    spark.range(1, N_KEYS + 2)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", F.concat(F.lit("seg-"), (F.col("customer_id") % 10).cast("string")))
)
dim_df.write.mode("overwrite").saveAsTable("lab.s2_dim")

fact = spark.table("lab.s2_fact")
dim = spark.table("lab.s2_dim")
print("fact:", fact.count(), "linhas | dim:", dim.count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **requer cluster clássico** — desligar AQE e o broadcast automático para reproduzir o skew "puro".
# MAGIC No serverless essas configs são bloqueadas (o AQE fica ligado e pode mascarar parte do problema).

# COMMAND ----------

for k, v in {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"}.items():
    try:
        spark.conf.set(k, v)
        print(f"ok  {k}={v}")
    except Exception as e:  # noqa: BLE001
        print(f"bloqueado no serverless: {k} ({type(e).__name__})")

# COMMAND ----------

def run_join(fact_df_, dim_df_, on):
    """Left join + agregação por segmento (resultado pequeno, seguro para collect)."""
    j = fact_df_.join(dim_df_, on, "left")
    return j.groupBy("segment").agg(F.count("*").alias("n"), F.sum("amount").alias("total"))


t_skew = timed("S2 · sort-merge com skew", lambda: run_join(fact, dim.hint("merge"), "customer_id").collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — S2
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - `groupBy(chave).count()` ordenado: uma chave com ordens de grandeza mais linhas que as outras.
# MAGIC - Contagem de **nulos** na chave: todos os nulos caem na mesma partição do shuffle (e não casam num inner join, mas viajam no left join).
# MAGIC - `EXPLAIN FORMATTED`: `SortMergeJoin` + `Exchange hashpartitioning(customer_id ...)`; ausência de `BroadcastHashJoin`.
# MAGIC - **Query Profile** (no lugar da Spark UI): no nó do join/shuffle, tempo/linhas do *maior* task muito acima da mediana.
# MAGIC - Distribuição por partição após `repartition` na chave: uma partição concentra a maior parte das linhas.

# COMMAND ----------

print("== Top chaves ==")
fact.groupBy("customer_id").count().orderBy(F.desc("count")).limit(5).show()

nulls = fact.filter("customer_id IS NULL").count()
print(f"== Nulos na chave: {nulls:,} ({nulls / N_S2:.0%}) ==")

print("== Linhas por partição de shuffle (top 5) ==")
(
    fact.repartition(64, "customer_id")
    .groupBy(F.spark_partition_id().alias("pid"))
    .count()
    .orderBy(F.desc("count"))
    .show(5)
)

print("== Plano ==")
run_join(fact, dim.hint("merge"), "customer_id").explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — S2
# MAGIC
# MAGIC **Fix imediato (do mais simples ao mais pesado):**
# MAGIC 1. **Broadcast** da dimensão pequena: elimina o shuffle do lado grande (nada de skew).
# MAGIC 2. **Separar os nulos** antes do join (eles nunca casam) e uni-los depois.
# MAGIC 3. **Salting** quando as duas tabelas são grandes: espalha a chave quente em N sub-chaves.
# MAGIC 4. **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`): já vem ligado por padrão em runtimes modernos.
# MAGIC
# MAGIC **Prevenção:** monitorar distribuição de chaves (job de data quality), tratar nulos/valores sentinela na ingestão,
# MAGIC preferir broadcast para dimensões, manter AQE ligado, revisar o Query Profile após mudanças de volume.

# COMMAND ----------

# 1) Broadcast da dimensão
timed("S2 · broadcast da dim", lambda: run_join(fact, F.broadcast(dim), "customer_id").collect())

# 2) Separar nulos: não passam pelo shuffle do join
com_chave = fact.filter("customer_id IS NOT NULL")
sem_chave = fact.filter("customer_id IS NULL").withColumn("segment", F.lit(None).cast("string"))
j1 = com_chave.join(dim.hint("merge"), "customer_id", "left").select("id", "customer_id", "amount", "segment")
j2 = sem_chave.select("id", "customer_id", "amount", "segment")
fixed_nulls = j1.unionByName(j2)
timed(
    "S2 · nulos separados (ainda com a chave quente)",
    lambda: fixed_nulls.groupBy("segment").agg(F.count("*").alias("n")).collect(),
)

# 3) Salting: espalha a chave quente (e os nulos) em SALT sub-partições
SALT = 16
fact_s = fact.withColumn("salt", (F.rand(11) * SALT).cast("int"))
dim_s = dim.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(SALT - 1))))
timed("S2 · salting + sort-merge", lambda: run_join(fact_s, dim_s.hint("merge"), ["customer_id", "salt"]).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ **requer cluster clássico** — ligar explicitamente o AQE skew join. No serverless essas configs são bloqueadas (e o AQE já é gerenciado).

# COMMAND ----------

for k, v in {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
}.items():
    try:
        spark.conf.set(k, v)
        print(f"ok  {k}={v}")
    except Exception as e:  # noqa: BLE001
        print(f"bloqueado no serverless: {k} ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — S2
# MAGIC
# MAGIC > The slow join was caused by data skew: roughly half of the fact rows share a single customer key and a further ten percent have a null key, so one shuffle task received most of the data while the others sat idle. As an immediate fix, we broadcast the small customer dimension, which removes the shuffle on the large side, and we showed salting as the option when both sides are large. For the long term, please add a data quality check on key distribution, handle null and sentinel keys at ingestion, and keep adaptive query execution enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## S3 — Query lenta sem data skipping (função na coluna vs filtro em intervalo)
# MAGIC
# MAGIC **Ticket do cliente:** "Filtering one day of events scans the whole table, even though the table is only 30 days of data."
# MAGIC
# MAGIC ### 1. Reproduzir — S3
# MAGIC
# MAGIC Tabela Delta com 4M eventos em 30 dias, gravada **ordenada por `ts`** (cada arquivo cobre uma faixa estreita → o data skipping *poderia* funcionar).
# MAGIC A query aplica uma **função na coluna** (`date_format(ts, ...)`), o que impede o uso das estatísticas min/max por arquivo.

# COMMAND ----------

N_S3 = 4_000_000

spark.sql("""
CREATE OR REPLACE TABLE lab.s3_events (id BIGINT, ts TIMESTAMP, user_id BIGINT, payload STRING)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
""")

events = (
    spark.range(N_S3)
    .withColumn("ts", (F.lit(1735689600) + (F.col("id") * F.lit(0.65)).cast("long")).cast("timestamp"))  # 2025-01-01 + ~30 dias
    .withColumn("user_id", (F.rand(3) * 100_000).cast("long"))
    .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
    .select("id", "ts", "user_id", "payload")
)
events.repartitionByRange(64, "ts").write.mode("append").saveAsTable("lab.s3_events")

Q_BAD = """
SELECT count(*) AS n, avg(user_id) AS avg_user
FROM lab.s3_events
WHERE date_format(ts, 'yyyy-MM-dd') = '2025-01-15'
"""
timed("S3 · filtro com função na coluna", lambda: spark.sql(Q_BAD).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — S3
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - `EXPLAIN FORMATTED`: na seção do `Scan`, `PushedFilters`/`DataFilters` **vazios** (ou sem o predicado sobre `ts`) → o filtro não foi empurrado para a leitura.
# MAGIC - **Query Profile** → nó de scan: *files read* ≈ *files total* (quase nenhum arquivo podado), *bytes read* ≈ tamanho da tabela.
# MAGIC - `DESCRIBE DETAIL`: `numFiles` (granularidade do skipping) e `clusteringColumns`/`partitionColumns` (vazios aqui).
# MAGIC - Estatísticas: Delta coleta min/max só das primeiras 32 colunas (`delta.dataSkippingNumIndexedCols`); coluna fora desse limite não tem skipping.

# COMMAND ----------

print("== EXPLAIN da query ruim ==")
spark.sql(Q_BAD).explain(mode="formatted")

print("== DESCRIBE DETAIL ==")
display(spark.sql("DESCRIBE DETAIL lab.s3_events").select("numFiles", "sizeInBytes", "partitionColumns", "clusteringColumns"))

print("== Faixa de ts por arquivo (amostra via input_file_name) ==")
display(
    spark.table("lab.s3_events")
    .groupBy(F.col("_metadata.file_name").alias("file"))
    .agg(F.min("ts").alias("min_ts"), F.max("ts").alias("max_ts"))
    .orderBy("min_ts")
    .limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — S3
# MAGIC
# MAGIC **Fix imediato:** reescrever o filtro como **intervalo direto na coluna** (`ts >= ... AND ts < ...`), sem função nem cast no lado da coluna.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - **Liquid clustering** (`CLUSTER BY (ts)`) para manter os arquivos organizados pelas colunas de filtro, sem partições rígidas.
# MAGIC - Se o filtro por data é padrão, coluna de data derivada (generated column) e filtros sobre ela.
# MAGIC - Colunas de filtro entre as primeiras 32 do schema (ou ajustar `delta.dataSkippingNumIndexedCols`).
# MAGIC - Revisar no Query Profile "files pruned" após cada mudança de query.

# COMMAND ----------

Q_GOOD = """
SELECT count(*) AS n, avg(user_id) AS avg_user
FROM lab.s3_events
WHERE ts >= TIMESTAMP '2025-01-15 00:00:00' AND ts < TIMESTAMP '2025-01-16 00:00:00'
"""
r_bad = timed("S3 · ANTES (função na coluna)", lambda: spark.sql(Q_BAD).collect())
r_good = timed("S3 · DEPOIS (intervalo)", lambda: spark.sql(Q_GOOD).collect())
print("mesmo resultado?", r_bad[0]["n"] == r_good[0]["n"], "| linhas:", r_good[0]["n"])

print("== EXPLAIN da query boa: procure o predicado em PushedFilters/DataFilters ==")
spark.sql(Q_GOOD).explain(mode="formatted")

# Prevenção: liquid clustering em tabela existente (não particionada) + OPTIMIZE
try:
    spark.sql("ALTER TABLE lab.s3_events CLUSTER BY (ts)")
    spark.sql("OPTIMIZE lab.s3_events")
    display(spark.sql("DESCRIBE DETAIL lab.s3_events").select("numFiles", "clusteringColumns"))
except Exception as e:  # noqa: BLE001
    print("Liquid clustering não aplicado neste ambiente:", type(e).__name__, str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — S3
# MAGIC
# MAGIC > The query was slow because it wrapped the timestamp column in a function inside the WHERE clause, which prevents Delta from using the per-file min and max statistics, so every file in the table was read. As an immediate fix, we rewrote the filter as a plain range on the column, which lets Delta skip the files outside that day and returns the same result much faster. For the long term, we recommend enabling liquid clustering on the columns you filter by most and reviewing the files pruned metric in the Query Profile whenever a query changes.

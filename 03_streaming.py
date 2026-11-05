# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Structured Streaming — ST1 a ST4
# MAGIC
# MAGIC | ID | Cenário |
# MAGIC |---|---|
# MAGIC | ST1 | Stream quebra após `DELETE` na fonte (`skipChangeCommits`, Change Data Feed) |
# MAGIC | ST2 | Agregação sem watermark |
# MAGIC | ST3 | Auto Loader com coluna nova |
# MAGIC | ST4 | Stream atrasando (`recentProgress`) |
# MAGIC
# MAGIC **Regras do lab:** todo stream usa `trigger(availableNow=True)` + `awaitTermination()` (o serverless não mantém streams infinitos
# MAGIC no notebook) e **cada cenário apaga o próprio checkpoint antes de reproduzir**, então pode ser re-executado do início.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## ST1 — Stream quebra após DELETE na fonte
# MAGIC
# MAGIC **Ticket do cliente:** "Our streaming job reading a Delta table has been running for weeks and now fails with 'Detected a data update ... This is currently not supported'."
# MAGIC
# MAGIC ### 1. Reproduzir — ST1

# COMMAND ----------

ST1_SRC = "lab.st1_src"
ST1_SINK = "lab.st1_sink"
ST1_CK = f"{CK}/st1"


def st1_reset():
    for t in (ST1_SRC, ST1_SINK, "lab.st1_sink_cdf"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    reset_dir(ST1_CK)
    reset_dir(f"{ST1_CK}_cdf")


def run_st1(options=None):
    """Lê a fonte Delta como stream e grava no sink (availableNow)."""
    reader = spark.readStream
    for k, v in (options or {}).items():
        reader = reader.option(k, v)
    q = (
        reader.table(ST1_SRC)
        .writeStream.option("checkpointLocation", ST1_CK)
        .trigger(availableNow=True)
        .toTable(ST1_SINK)
    )
    q.awaitTermination()
    return q


st1_reset()
spark.sql(f"CREATE TABLE {ST1_SRC} (id INT, payload STRING)")
spark.sql(f"INSERT INTO {ST1_SRC} SELECT CAST(id AS INT), concat('v1-', id) FROM range(1, 11)")

run_st1()
print("sink após o 1º run:", spark.table(ST1_SINK).count(), "linhas")

spark.sql(f"DELETE FROM {ST1_SRC} WHERE id <= 3")   # <- alguém apaga linhas na fonte (ex.: pedido de LGPD)
expect_error(run_st1, "ST1 · stream após DELETE na fonte")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — ST1
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - Mensagem: "Detected a data update (for example ...) in the source table at version N" + sugestão de `skipChangeCommits`.
# MAGIC - `DESCRIBE HISTORY` da **fonte**: a operação (`DELETE`/`UPDATE`/`MERGE`/`OVERWRITE`) na versão N. Streams só aceitam *appends*.
# MAGIC - Checkpoint (`offsets/`): `reservoirVersion` = onde o stream parou, para comparar com a versão atual da fonte.
# MAGIC - Perguntar: o sink **deve** refletir o delete? (LGPD → sim, CDF; reprocessamento/compactação → pode ignorar.)

# COMMAND ----------

display(history(ST1_SRC).select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

offsets = sorted(
    [f for f in dbutils.fs.ls(f"{ST1_CK}/offsets") if f.name.rstrip("/").isdigit()],
    key=lambda f: int(f.name.rstrip("/")),
)
print(f"Último arquivo de offsets ({offsets[-1].name}):")
print(dbutils.fs.head(offsets[-1].path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — ST1
# MAGIC
# MAGIC **Fix imediato (escolha conforme a regra de negócio):**
# MAGIC - **A) `skipChangeCommits = true`**: o stream ignora commits que apenas alteram/removem dados. O sink **não** recebe os deletes.
# MAGIC - **B) Change Data Feed**: habilite `delta.enableChangeDataFeed` na fonte, leia com `readChangeFeed` e aplique `insert/update/delete` no sink (via `foreachBatch` + `MERGE`, com dedupe por chave!).
# MAGIC
# MAGIC **Prevenção de longo prazo:** decidir o contrato de mudanças na fonte (append-only vs mutável), documentar, e usar CDF para tabelas mutáveis.
# MAGIC Evitar `OVERWRITE`/`DELETE` ad-hoc na fonte de streams.

# COMMAND ----------

# A) Ignorar commits de alteração/remoção (usa o MESMO checkpoint, o stream continua de onde parou)
run_st1({"skipChangeCommits": "true"})
spark.sql(f"INSERT INTO {ST1_SRC} VALUES (20, 'v1-20')")
run_st1({"skipChangeCommits": "true"})
print("A) fonte:", spark.table(ST1_SRC).count(), "linhas | sink:", spark.table(ST1_SINK).count(),
      "linhas → o sink mantém ids 1..3 (delete NÃO propagado) e recebeu o id 20")

# B) Propagar deletes com Change Data Feed
spark.sql(f"ALTER TABLE {ST1_SRC} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql("DROP TABLE IF EXISTS lab.st1_sink_cdf")
spark.sql(f"CREATE TABLE lab.st1_sink_cdf AS SELECT * FROM {ST1_SRC}")      # ponto de partida consistente
v0 = history(ST1_SRC).agg(F.max("version")).first()[0]

spark.sql(f"DELETE FROM {ST1_SRC} WHERE id = 5")
spark.sql(f"INSERT INTO {ST1_SRC} VALUES (11, 'v2-11'), (12, 'v2-12')")


def apply_cdf(batch, batch_id):
    """Aplica o CDC no sink. Dedupe por chave: MERGE não aceita 2 linhas de origem para a mesma chave (ver D1)."""
    latest = (
        batch.filter("_change_type <> 'update_preimage'")
        .withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("_commit_version"))))
        .filter("_rn = 1")
        .drop("_rn")
    )
    latest.createOrReplaceTempView("cdf_latest")
    batch.sparkSession.sql("""
        MERGE INTO lab.st1_sink_cdf AS t
        USING cdf_latest AS s
        ON t.id = s.id
        WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.payload = s.payload
        WHEN NOT MATCHED AND s._change_type <> 'delete' THEN INSERT (id, payload) VALUES (s.id, s.payload)
    """)


q = (
    spark.readStream.option("readChangeFeed", "true").option("startingVersion", v0 + 1)
    .table(ST1_SRC)
    .writeStream.foreachBatch(apply_cdf)
    .option("checkpointLocation", f"{ST1_CK}_cdf")
    .trigger(availableNow=True)
    .start()
)
q.awaitTermination()

diff1 = spark.table(ST1_SRC).exceptAll(spark.table("lab.st1_sink_cdf")).count()
diff2 = spark.table("lab.st1_sink_cdf").exceptAll(spark.table(ST1_SRC)).count()
print(f"B) diferenças fonte→sink_cdf: {diff1} | sink_cdf→fonte: {diff2} (esperado 0 e 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — ST1
# MAGIC
# MAGIC > The stream stopped because the source table received a DELETE, and a Delta streaming read only accepts appended data unless it is told how to treat commits that modify or remove existing rows. As an immediate fix, we restarted the stream with skipChangeCommits so it ignores those commits, and we showed the Change Data Feed alternative that also propagates the deletes to the target table. For the long term, please decide whether the source table is append-only or mutable, and use Change Data Feed with a MERGE into the target whenever deletes and updates must be reflected downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ST2 — Agregação sem watermark
# MAGIC
# MAGIC **Ticket do cliente:** "Our windowed count fails to start with 'Append output mode not supported when there are streaming aggregations'. When we switch to update mode, the state keeps growing."
# MAGIC
# MAGIC ### 1. Reproduzir — ST2

# COMMAND ----------

ST2_CK = f"{CK}/st2"
ST2_OUT = "lab.st2_out"

spark.sql("""
CREATE OR REPLACE TABLE lab.st2_events AS
SELECT timestamp_seconds(1735689600 + id * 3) AS ts, CAST(id % 50 AS INT) AS user_id
FROM range(20000)
""")   # ~16,7 horas de eventos (1 a cada 3 s)

agg_no_wm = (
    spark.readStream.table("lab.st2_events")
    .groupBy(F.window("ts", "10 minutes"))
    .count()
)


def start_no_watermark():
    spark.sql(f"DROP TABLE IF EXISTS {ST2_OUT}")
    reset_dir(ST2_CK)
    q = (
        agg_no_wm.writeStream.outputMode("append")
        .option("checkpointLocation", ST2_CK)
        .trigger(availableNow=True)
        .toTable(ST2_OUT)
    )
    q.awaitTermination()


expect_error(start_no_watermark, "ST2 · append + agregação sem watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — ST2
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - O erro é de **análise do plano** (acontece ao iniciar): agregação em streaming + `append` exige watermark para saber quando uma janela está "fechada".
# MAGIC - Com `update` mode (aceito sem watermark), `stateOperators` em `recentProgress`: `numRowsTotal` e `memoryUsedBytes` **só crescem** (nenhuma janela é descartada).
# MAGIC - `numRowsDroppedByWatermark` = 0 e `watermark` ausente no progresso.
# MAGIC - Cluster clássico: aba *Structured Streaming* → state rows/memory ao longo do tempo.

# COMMAND ----------

print("é streaming?", agg_no_wm.isStreaming)

reset_dir(f"{ST2_CK}_diag")
q = (
    agg_no_wm.writeStream.outputMode("update").format("noop")
    .option("checkpointLocation", f"{ST2_CK}_diag")
    .trigger(availableNow=True)
    .start()
)
q.awaitTermination()

for p in q.recentProgress:
    for s in p.get("stateOperators", []):
        print(f"batch={p['batchId']} state numRowsTotal={s.get('numRowsTotal')} "
              f"memoryUsedBytes={s.get('memoryUsedBytes')} watermark={p.get('eventTime', {}).get('watermark')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — ST2
# MAGIC
# MAGIC **Fix imediato:** `withWatermark("ts", "<atraso tolerado>")` **antes** do `groupBy`. No modo `append`, cada janela só é emitida (uma vez) quando o watermark passa do fim dela;
# MAGIC o estado das janelas antigas é descartado. Por isso as **últimas janelas ainda abertas não aparecem** no sink (comportamento esperado).
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Dimensionar o watermark pela latência real dos dados (mais curto = menos estado, mais eventos tardios descartados).
# MAGIC - Monitorar `stateOperators` e `numRowsDroppedByWatermark` (alertas).
# MAGIC - Se precisar de resultado contínuo por janela aberta: `update` mode + `foreachBatch` com `MERGE`, sempre com watermark para limitar o estado.
# MAGIC - State store com RocksDB para estados grandes (clássico).

# COMMAND ----------

agg_wm = (
    spark.readStream.table("lab.st2_events")
    .withWatermark("ts", "15 minutes")
    .groupBy(F.window("ts", "10 minutes"))
    .count()
)

spark.sql(f"DROP TABLE IF EXISTS {ST2_OUT}")
reset_dir(ST2_CK)
q = (
    agg_wm.writeStream.outputMode("append")
    .option("checkpointLocation", ST2_CK)
    .trigger(availableNow=True)
    .toTable(ST2_OUT)
)
q.awaitTermination()

total_windows = (20000 * 3) // 600 + 1
emitidas = spark.table(ST2_OUT).count()
print(f"janelas emitidas: {emitidas} de ~{total_windows} (as últimas ficam abertas até o watermark avançar)")
display(spark.table(ST2_OUT).orderBy("window").limit(5))

for p in q.recentProgress:
    for s in p.get("stateOperators", []):
        print(f"batch={p['batchId']} state numRowsTotal={s.get('numRowsTotal')} "
              f"droppedByWatermark={s.get('numRowsDroppedByWatermark')} watermark={p.get('eventTime', {}).get('watermark')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — ST2
# MAGIC
# MAGIC > The query failed to start because a streaming aggregation in append mode needs a watermark to know when a time window is complete, and without one Spark also keeps the state of every window forever. As an immediate fix, we added a watermark before the aggregation, so each window is emitted once it is closed and old state is discarded, which also keeps memory bounded. For the long term, please size the watermark from the real lateness of your data and monitor the state size and the number of rows dropped by the watermark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ST3 — Auto Loader com coluna nova
# MAGIC
# MAGIC **Ticket do cliente:** "Our Auto Loader stream failed after the source system started sending a new field, and restarting it keeps failing in a different way."
# MAGIC
# MAGIC ### 1. Reproduzir — ST3
# MAGIC
# MAGIC Primeiro arquivo JSON com `id, name`; depois chega um arquivo com a coluna `email`.

# COMMAND ----------

ST3_DIR = f"{LANDING}/st3"
ST3_SCHEMA = f"{CK}/st3_schema"
ST3_CK = f"{CK}/st3"
ST3_TBL = "lab.st3_people"


def st3_reset():
    spark.sql(f"DROP TABLE IF EXISTS {ST3_TBL}")
    for p in (ST3_DIR, ST3_SCHEMA, ST3_CK):
        reset_dir(p)


def put_json(name, rows):
    dbutils.fs.put(f"{ST3_DIR}/{name}", "\n".join(json.dumps(r) for r in rows), True)


def run_st3(merge_schema=False, evolution_mode=None):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", ST3_SCHEMA)
    )
    if evolution_mode:
        reader = reader.option("cloudFiles.schemaEvolutionMode", evolution_mode)
    writer = reader.load(ST3_DIR).writeStream.option("checkpointLocation", ST3_CK).trigger(availableNow=True)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    q = writer.toTable(ST3_TBL)
    q.awaitTermination()
    return q


st3_reset()
put_json("file1.json", [{"id": 1, "name": "Ana"}, {"id": 2, "name": "Bruno"}])
run_st3()
print("1º run OK →", spark.table(ST3_TBL).columns, spark.table(ST3_TBL).count(), "linhas")

put_json("file2.json", [{"id": 3, "name": "Carla", "email": "carla@example.com"}])   # coluna nova!
expect_error(run_st3, "ST3 · 1ª tentativa após a coluna nova")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — ST3
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - Erro `UNKNOWN_FIELD_EXCEPTION` / `UnknownFieldException` com a lista de colunas novas e o arquivo: com `addNewColumns` (padrão) o Auto Loader **falha de propósito** na primeira vez, **grava o schema novo** em `schemaLocation/_schemas` e espera um restart.
# MAGIC - O conteúdo do último arquivo em `_schemas` já contém `email`.
# MAGIC - Reiniciar "às cegas" produz um **segundo erro**, agora do Delta: a tabela destino não tem a coluna → schema mismatch (a falha é no sink, não mais na leitura).

# COMMAND ----------

schemas = sorted(dbutils.fs.ls(f"{ST3_SCHEMA}/_schemas"), key=lambda f: f.name)
print("Arquivos de schema:", [f.name for f in schemas])
print(dbutils.fs.head(schemas[-1].path))

print("Schema atual da tabela destino:", spark.table(ST3_TBL).columns)

expect_error(run_st3, "ST3 · 2ª tentativa (restart sem mergeSchema no sink)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — ST3
# MAGIC
# MAGIC **Fix imediato:** reiniciar o stream com `mergeSchema=true` no **sink**, para a tabela receber a coluna nova.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Rodar o Auto Loader num **Job com retries** (o primeiro restart é esperado com `addNewColumns`).
# MAGIC - `cloudFiles.schemaEvolutionMode = rescue` + `_rescued_data`: nenhuma falha, colunas novas ficam num JSON de rescue até alguém decidir.
# MAGIC - `failOnNewColumns` quando o contrato é rígido; `cloudFiles.schemaHints` para fixar tipos.
# MAGIC - Alerta quando `_rescued_data` deixa de ser nulo.

# COMMAND ----------

run_st3(merge_schema=True)
spark.table(ST3_TBL).printSchema()
display(spark.table(ST3_TBL).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — ST3
# MAGIC
# MAGIC > The stream failed because the incoming files contained a new field, and Auto Loader is designed to stop once, record the evolved schema, and wait for a restart, after which the Delta target rejected the write because it did not have the new column. As an immediate fix, we restarted the stream with schema merging enabled on the target table, so the new column was added and all files were ingested. For the long term, please run Auto Loader in a job with automatic retries, and consider rescue mode so that unexpected fields are captured in the rescued data column instead of stopping the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ST4 — Stream atrasando
# MAGIC
# MAGIC **Ticket do cliente:** "Our stream is hours behind. The cluster is not busy, but the lag keeps growing."
# MAGIC
# MAGIC ### 1. Reproduzir — ST4
# MAGIC
# MAGIC Fonte com 30 arquivos pequenos e um stream limitado a **1 arquivo por micro-batch** (`maxFilesPerTrigger = 1`): 30 batches, cada um dominado por overhead.

# COMMAND ----------

ST4_SRC = "lab.st4_src"
ST4_SINK = "lab.st4_sink"
ST4_CK = f"{CK}/st4"

spark.sql(f"""
CREATE OR REPLACE TABLE {ST4_SRC} (id BIGINT, k INT, v DOUBLE)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
""")
(
    spark.range(1_500_000)
    .select("id", (F.col("id") % 1000).cast("int").alias("k"), F.rand(5).alias("v"))
    .repartition(30)
    .write.mode("append")
    .saveAsTable(ST4_SRC)
)
print("arquivos na fonte:", spark.sql(f"DESCRIBE DETAIL {ST4_SRC}").first()["numFiles"])


def run_st4(max_files, tag):
    """Roda o stream com maxFilesPerTrigger=max_files (checkpoint e sink novos a cada chamada)."""
    spark.sql(f"DROP TABLE IF EXISTS {ST4_SINK}_{tag}")
    reset_dir(f"{ST4_CK}_{tag}")
    t0 = time.time()
    q = (
        spark.readStream.option("maxFilesPerTrigger", max_files).table(ST4_SRC)
        .withColumn("v_sqrt", F.sqrt("v"))
        .writeStream.option("checkpointLocation", f"{ST4_CK}_{tag}")
        .trigger(availableNow=True)
        .toTable(f"{ST4_SINK}_{tag}")
    )
    q.awaitTermination()
    return q, time.time() - t0


q_slow, t_slow = run_st4(1, "slow")
print(f"ST4 · maxFilesPerTrigger=1 → {len(q_slow.recentProgress)} batches em {t_slow:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — ST4
# MAGIC
# MAGIC **O que procurar em `recentProgress` / `lastProgress`:**
# MAGIC - `inputRowsPerSecond` > `processedRowsPerSecond` de forma sustentada → o stream **não acompanha** a entrada.
# MAGIC - `durationMs`: `triggerExecution` total vs `addBatch` (processamento + escrita), `latestOffset` (listagem da fonte), `walCommit`/`commitOffsets` (latência do checkpoint), `queryPlanning`.
# MAGIC - Fonte Delta: `sources[0].metrics.numFilesOutstanding` / `numBytesOutstanding` = **backlog** ainda não lido.
# MAGIC - `numInputRows` minúsculo por batch com `triggerExecution` alto → overhead por micro-batch dominando (limites de trigger baixos demais / arquivos pequenos).
# MAGIC - `stateOperators` crescendo (se houver estado) e `batchDuration` maior que o intervalo do trigger.

# COMMAND ----------

import pandas as pd


def progress_table(q):
    rows = []
    for p in q.recentProgress:
        src = (p.get("sources") or [{}])[0]
        m = src.get("metrics", {}) or {}
        d = p.get("durationMs", {}) or {}
        rows.append((
            p.get("batchId"),
            p.get("numInputRows"),
            round(p.get("inputRowsPerSecond") or 0, 1),
            round(p.get("processedRowsPerSecond") or 0, 1),
            d.get("triggerExecution"),
            d.get("addBatch"),
            d.get("latestOffset"),
            d.get("walCommit"),
            m.get("numFilesOutstanding"),
            m.get("numBytesOutstanding"),
        ))
    cols = ["batch", "input_rows", "in_rows/s", "proc_rows/s", "trigger_ms", "addBatch_ms", "latestOffset_ms",
            "walCommit_ms", "files_outstanding", "bytes_outstanding"]
    return pd.DataFrame(rows, columns=cols)


pdf = progress_table(q_slow)
display(pdf)
print(f"batches: {len(pdf)} | linhas/batch (média): {pdf['input_rows'].mean():.0f} | trigger_ms (média): {pdf['trigger_ms'].mean():.0f}")
print("lastProgress:", q_slow.lastProgress and {k: q_slow.lastProgress[k] for k in ("batchId", "numInputRows", "durationMs")})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — ST4
# MAGIC
# MAGIC **Fix imediato:** aumentar o tamanho do micro-batch (`maxFilesPerTrigger` maior ou `maxBytesPerTrigger`, ex.: `"1g"`) para amortizar o overhead; compactar a fonte (`OPTIMIZE`).
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Monitorar backlog (`numFilesOutstanding`/`numBytesOutstanding`), `processedRowsPerSecond` vs `inputRowsPerSecond` e duração do batch com um `StreamingQueryListener` ou o event log, com alerta.
# MAGIC - Evitar fontes com muitos arquivos pequenos (optimize write / auto compaction / predictive optimization).
# MAGIC - Cluster/serverless dimensionado para o pico, não para a média; `foreachBatch` eficiente (sem `collect`, `MERGE` com predicado de partição).
# MAGIC - Rever o intervalo de trigger vs duração real do batch.

# COMMAND ----------

spark.sql(f"OPTIMIZE {ST4_SRC}")   # compacta a fonte
q_fast, t_fast = run_st4(100, "fast")
print(f"ST4 · maxFilesPerTrigger=100 → {len(q_fast.recentProgress)} batches em {t_fast:.0f}s (antes: {len(q_slow.recentProgress)} batches em {t_slow:.0f}s)")
display(progress_table(q_fast))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — ST4
# MAGIC
# MAGIC > The stream was falling behind because each micro-batch was limited to a single small file, so the fixed per-batch overhead dominated and the processing rate stayed below the arrival rate while the backlog kept growing. As an immediate fix, we compacted the source table and raised the per-trigger limits, which let each batch process far more data and cleared the backlog in a fraction of the time. For the long term, please monitor the backlog metrics and the processed versus input rate with alerts, and keep the source tables compacted.

# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Cheatsheet: sintoma → primeiro comando → fix típico
# MAGIC
# MAGIC Só markdown. Use como roteiro mental na entrevista: **sintoma → hipótese → primeiro comando → confirmar → fix → prevenção → mensagem ao cliente**.
# MAGIC
# MAGIC ## Spark
# MAGIC
# MAGIC | Sintoma | Primeiro comando | Fix típico |
# MAGIC |---|---|---|
# MAGIC | Driver OOM / "driver not responding" / resultado grande demais | Stack trace (`collect`/`toPandas`), `df.limit(10000).toPandas()` para estimar tamanho | Agregar no cluster, `limit`, gravar em tabela; driver maior só em clássico |
# MAGIC | Executor OOM / spill em disco | Query Profile (spill, maior task) ou Spark UI no clássico | Mais partições, tratar skew, evitar `explode` gigante, nós maiores |
# MAGIC | Uma task demora muito mais que as outras | `df.groupBy(chave).count().orderBy(desc("count"))` e contagem de nulos | Broadcast, separar nulos, salting, AQE skew join |
# MAGIC | Query lê a tabela inteira para filtrar pouco | `EXPLAIN FORMATTED` (PushedFilters vazio?) e *files pruned* no Query Profile | Filtro em intervalo na coluna (sem função), `CLUSTER BY`, `OPTIMIZE` |
# MAGIC | Join inesperadamente lento sem skew | `EXPLAIN FORMATTED` (SortMergeJoin em vez de Broadcast?) | `broadcast()` na tabela pequena, estatísticas (`ANALYZE TABLE`) |
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC | Sintoma | Primeiro comando | Fix típico |
# MAGIC |---|---|---|
# MAGIC | `MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | `SELECT chave, count(*) FROM origem GROUP BY 1 HAVING count(*) > 1` | Dedupe da origem (`QUALIFY ROW_NUMBER()`), condição no `WHEN MATCHED` |
# MAGIC | "A schema mismatch detected when writing" | Comparar `df.schema` com `spark.table(t).schema` | Cast para o tipo da tabela; `mergeSchema` só para coluna nova |
# MAGIC | Dados apagados/alterados por engano | `DESCRIBE HISTORY t` | `RESTORE TABLE t TO VERSION AS OF n` |
# MAGIC | Time travel falha (arquivo não encontrado) | `DESCRIBE HISTORY t` (procure `VACUUM`) e `DESCRIBE DETAIL t` (propriedades de retenção) | Sem arquivos não há restore: reconstruir da origem/backup; retenção ≥ 7 dias |
# MAGIC | `VACUUM` recusa retenção curta | Mensagem do `retentionDurationCheck` | Não desligar a checagem; manter ≥ 7 dias |
# MAGIC | Leituras lentas, muitos arquivos pequenos | `DESCRIBE DETAIL t` → `sizeInBytes / numFiles` | `OPTIMIZE`, liquid clustering, optimize write / Predictive Optimization |
# MAGIC | `ConcurrentAppend/DeleteRead/DeleteDelete Exception` | `DESCRIBE HISTORY t` (`readVersion`, `isolationLevel`) e predicados da DML | Retry com backoff, escritores em partições/clusters disjuntos, serializar jobs |
# MAGIC | Tabela dropada por engano | `SHOW TABLES DROPPED IN schema` | `UNDROP TABLE` (UC, dentro da janela de retenção) |
# MAGIC
# MAGIC ## Streaming
# MAGIC
# MAGIC | Sintoma | Primeiro comando | Fix típico |
# MAGIC |---|---|---|
# MAGIC | "Detected a data update ... not supported" na fonte Delta | `DESCRIBE HISTORY fonte` (qual operação, qual versão) | `skipChangeCommits=true` ou Change Data Feed + `foreachBatch`/`MERGE` |
# MAGIC | "Append output mode not supported ... without watermark" | `df.isStreaming` e plano; `stateOperators` no progresso | `withWatermark` antes do `groupBy`; `update` + `foreachBatch` |
# MAGIC | Estado/memória só cresce | `q.recentProgress[-1]["stateOperators"]` (`numRowsTotal`, `memoryUsedBytes`) | Watermark adequado, `dropDuplicatesWithinWatermark`, RocksDB |
# MAGIC | Auto Loader `UnknownFieldException` (coluna nova) | `ls <schemaLocation>/_schemas` e `head` do último arquivo | Restart (job com retry) + `mergeSchema` no sink; modo `rescue` |
# MAGIC | Stream atrasando | `q.recentProgress`: `inputRowsPerSecond` vs `processedRowsPerSecond`, `durationMs`, backlog (`numFilesOutstanding`) | `maxFilesPerTrigger`/`maxBytesPerTrigger`, compactar fonte, `foreachBatch` eficiente, capacidade |
# MAGIC | Stream não retoma / checkpoint corrompido ou incompatível | Conteúdo de `<checkpoint>/offsets` e `commits` | Restaurar o mesmo plano/opções; último recurso: novo checkpoint com `startingVersion` controlado |
# MAGIC
# MAGIC ## Plataforma
# MAGIC
# MAGIC | Sintoma | Primeiro comando | Fix típico |
# MAGIC |---|---|---|
# MAGIC | Timeout ao acessar sistema externo | `socket.create_connection((host, porta), timeout=5)` | Liberar firewall/NSG/UDR; no serverless: política de rede + NCC/private endpoint |
# MAGIC | `Name or service not known` | `socket.getaddrinfo(host, porta)` | Corrigir nome, DNS privado, resolvedor |
# MAGIC | `Connection refused` | `socket.create_connection` (host responde, porta não) | Serviço no ar? porta/IP corretos? |
# MAGIC | Erro de certificado / SSL | `ssl.create_default_context().wrap_socket(...)` | CA corporativa, ajustar inspeção TLS |
# MAGIC | Model Serving: `ModuleNotFoundError` no build | `mlflow.pyfunc.get_model_dependencies(uri)` e `mlflow.models.predict(..., env_manager="virtualenv")` | Relogar com `pip_requirements` fixos, `signature`, `input_example`; testar no CI |
# MAGIC | Endpoint sobe mas falha na inferência | Service logs do endpoint + input vs `signature` | Corrigir schema de entrada / código do pyfunc |
# MAGIC | `PERMISSION_DENIED` / "table not found" no UC | `SELECT current_user()`, `SHOW GRANTS ON CATALOG/SCHEMA/TABLE` | Cadeia `USE CATALOG` → `USE SCHEMA` → `SELECT` para um **grupo** |
# MAGIC | Acesso a dados externos negado | `SHOW GRANTS ON EXTERNAL LOCATION` / `READ VOLUME` | Conceder privilégio na *external location* / volume |
# MAGIC | Ninguém sabe "quem fez isso" | `DESCRIBE HISTORY` (`userName`) e `system.access.audit` | Auditoria, menos privilégios, DML destrutivo via job |
# MAGIC
# MAGIC ## Limites do serverless (lembrar na entrevista)
# MAGIC
# MAGIC - Sem RDD / `sparkContext`; Spark UI → **Query Profile**.
# MAGIC - Várias `spark.conf` bloqueadas (AQE on/off, `autoBroadcastJoinThreshold`, memória do driver...): "isso eu faria num cluster clássico".
# MAGIC - Streams em notebook: `trigger(availableNow=True)` + `awaitTermination()`.
# MAGIC
# MAGIC ## Roteiro de resposta (fale em voz alta)
# MAGIC
# MAGIC 1. **Esclarecer:** o que mudou? quando começou? compute, runtime, volume, último sucesso?
# MAGIC 2. **Reproduzir** ou colher evidência (logs, histórico, plano).
# MAGIC 3. **Hipótese** em uma frase e **o comando** que a confirma ou descarta.
# MAGIC 4. **Fix imediato** (desbloquear) separado da **prevenção** (não voltar a acontecer).
# MAGIC 5. **Comunicar** em 3 frases: causa raiz, correção imediata, ação de longo prazo.

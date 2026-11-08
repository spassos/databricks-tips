"""L1 · Driver com pouca memória: collect()/toPandas() de dataset grande.

Ticket do cliente: "Our job fails with 'Total size of serialized results ... is bigger than
spark.driver.maxResultSize' as soon as we convert the DataFrame to pandas."

Como rodar:  python local/L1_driver_oom.py        (ou F5 com a configuração "L1 · driver OOM")
Spark UI:    http://localhost:4040 (fica no ar até você apertar Enter no final)
"""
from spark_session import (
    DATA_DIR,
    cronometrar,
    get_spark,
    heap_da_jvm,
    pausar_ui,
    resumo_executores,
    titulo,
)
from pyspark import StorageLevel
from pyspark.sql import functions as F

# Driver pequeno de propósito. No serverless NÃO dá para mudar isso (por isso este cenário é local).
CONFS = {
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "256m",
}
N = 8_000_000
spark = get_spark("L1_driver_oom", **CONFS)


def dataset_grande():
    """8M linhas com strings pouco compressíveis (hashes). O maxResultSize conta bytes JÁ COMPRIMIDOS,
    por isso usamos hashes: com texto repetitivo (ex.: 'xxxx') o limite nunca seria atingido."""
    return (
        spark.range(0, N, 1, 24)
        .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
        .withColumn("token", F.md5(F.col("id").cast("string")))
        .withColumn("valor", F.rand(1) * 1000)
    )


# ----------------------------------------------------------------------------------------------
titulo("1. REPRODUZIR — collect() e toPandas() de 8M linhas com maxResultSize=256m")
# ----------------------------------------------------------------------------------------------
big = dataset_grande()

for rotulo, acao in [("collect()", lambda: big.collect()), ("toPandas()", lambda: big.toPandas())]:
    try:
        acao()
        print(f"⚠️ {rotulo} NÃO falhou; aumente N ou reduza spark.driver.maxResultSize.")
    except Exception as e:  # noqa: BLE001 - queremos ver o erro real do cliente
        msg = str(e)
        # Py4JJavaError traz a causa Java; imprimimos a linha que importa
        linha = next((ln for ln in msg.splitlines() if "maxResultSize" in ln), msg.splitlines()[0])
        print(f"❌ {rotulo} → {type(e).__name__}\n   {linha.strip()[:300]}")

# ----------------------------------------------------------------------------------------------
titulo("2. DIAGNOSTICAR — onde olhar")
# ----------------------------------------------------------------------------------------------
print("""
Spark UI (http://localhost:4040):
  • Jobs     → o job do collect/toPandas aparece como FAILED (Failure Reason = o erro de maxResultSize).
  • Stages   → o stage falhou; clique nele e veja 'Result Serialization Time' / 'Size' nas tasks.
  • Executors → linha 'driver': Storage Memory (usada / total), Max Heap e 'Failed Tasks'.
  • Environment → Spark Properties: confirme spark.driver.memory=1g e spark.driver.maxResultSize=256m
                  (a config 'efetiva' que o cliente está usando, não a que ele acha que usa).
Logs (console desta execução / driver log do cluster):
  • 'Total size of serialized results of N tasks (X MiB) is bigger than spark.driver.maxResultSize (256.0 MiB)'
  • Se o limite fosse maior e o heap estourasse: 'java.lang.OutOfMemoryError: Java heap space' no driver.
Detalhe de suporte: maxResultSize conta o tamanho serializado e COMPRIMIDO dos resultados das tasks; já o heap
do driver recebe os dados DESCOMPRIMIDOS. Por isso dados repetitivos podem passar pelo limite e ainda derrubar o driver.
""")

print("Heap da JVM do driver (MB):", heap_da_jvm(spark))

# Storage Memory: cache moderado só para a aba Storage/Executors mostrar uso de memória do driver
pequeno = big.limit(1_500_000).select("id", "payload").persist(StorageLevel.MEMORY_ONLY)
print("linhas em cache:", pequeno.count())
print("Aba Executors (via REST) — repare em Storage Memory usada vs total do driver:")
resumo_executores(spark)
print("→ Abra também a aba Storage: RDD em cache, tamanho em memória e % cacheado.")
pequeno.unpersist()

# ----------------------------------------------------------------------------------------------
titulo("3. CORRIGIR — nunca trazer tudo para o driver")
# ----------------------------------------------------------------------------------------------
# a) Agregar no cluster e trazer só o resumo
resumo, _ = cronometrar("agregação no cluster", lambda: big.groupBy((F.col("id") % 10).alias("bucket")).count().toPandas())
print(resumo.sort_values("bucket").head(3).to_string(index=False))

# b) Amostra limitada para inspeção local
amostra, _ = cronometrar("limit(50_000).toPandas()", lambda: big.limit(50_000).toPandas())
print("amostra:", amostra.shape)


# c) Processar tudo em streaming para o driver, partição a partição (cada job < maxResultSize)
def somar_por_iterador():
    total = 0.0
    for row in big.select("valor").toLocalIterator():
        total += row["valor"]
    return total


soma, _ = cronometrar("toLocalIterator() (consome 8M linhas sem estourar o limite)", somar_por_iterador)
print(f"soma de 'valor' = {soma:,.0f}")

# d) Persistir o grande em vez de coletar
cronometrar("gravar parquet", lambda: big.where("id % 1000 = 0").write.mode("overwrite").parquet(str(DATA_DIR / "l1_resultado")))

print("""
Prevenção de longo prazo:
  • toPandas() só depois de limit()/agregação (regra de code review).
  • pandas em escala: pandas API on Spark ou applyInPandas/mapInPandas (rodam no executor).
  • Saídas grandes em Delta/Parquet; o consumidor lê de lá.
  • Cluster clássico: dimensionar driver e spark.driver.maxResultSize de forma explícita e consciente.
""")

# ----------------------------------------------------------------------------------------------
titulo("4. AO CLIENTE")
# ----------------------------------------------------------------------------------------------
print("""
The job failed because collect() and toPandas() tried to bring hundreds of megabytes of serialized rows to the driver, which exceeds the configured spark.driver.maxResultSize of 256 MiB and would otherwise exhaust the one gigabyte driver heap. As an immediate fix, we replaced the pull with an aggregation that runs on the executors, a bounded sample for inspection and an iterator for full scans, and we write the large result to Parquet instead of collecting it. For the long term, please always apply a limit or an aggregation before toPandas(), and size the driver memory and its result limit explicitly if a large collect is truly required.
""")

pausar_ui(spark)

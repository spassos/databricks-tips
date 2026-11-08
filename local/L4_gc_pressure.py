"""L4 · Pressão de GC: cache de objetos grandes + UDF Python ineficiente.

Ticket do cliente: "Tasks are getting slower and slower, executors show a high GC time and we sometimes
see 'GC overhead limit exceeded'. We cache the DataFrame to speed things up."

Como rodar:  python local/L4_gc_pressure.py        (ou F5 com a configuração "L4 · GC")
Spark UI:    http://localhost:4040 → Executors (GC Time em vermelho) e Stages (GC Time por task) e Storage
"""
from spark_session import (
    cronometrar,
    gc_total,
    get_spark,
    heap_da_jvm,
    pausar_ui,
    resumo_executores,
    titulo,
)
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

CONFS = {
    "spark.driver.memory": "1g",     # heap curto de propósito (no serverless você não controla o heap)
    "spark.sql.shuffle.partitions": "8",
}
N = 5_000_000
spark = get_spark("L4_gc_pressure", **CONFS)


def largo():
    """DataFrame com várias colunas de texto (largo e pesado em memória)."""
    return spark.range(0, N, 1, 64).select(
        "id",
        F.concat(F.lit("user-"), F.col("id").cast("string")).alias("nome"),
        F.sha2(F.col("id").cast("string"), 256).alias("hash"),
        F.repeat(F.md5(F.col("id").cast("string")), 4).alias("texto"),
        (F.rand(5) * 100).alias("valor"),
    )


def gc_pct(fn, rotulo):
    """Executa fn() e mostra quanto do tempo de tarefa foi GC (aba Executors: GC Time / Task Time)."""
    gc0, d0 = gc_total(spark)
    _, dt = cronometrar(rotulo, fn)
    gc1, d1 = gc_total(spark)
    pct = 100 * (gc1 - gc0) / max(d1 - d0, 1)
    print(f"   GC Time: {(gc1 - gc0) / 1000:.1f}s de {(d1 - d0) / 1000:.1f}s de tarefas → {pct:.1f}%")
    return dt, pct


# UDF Python linha a linha: cada linha é serializada (pickle), vira objeto Java, vai ao Python e volta
@udf(StringType())
def rotular(nome, texto):
    return nome.upper() + "-" + str(len(texto)) + "-" + texto[:6]


# ----------------------------------------------------------------------------------------------
titulo("1. REPRODUZIR — cache de TODAS as colunas (MEMORY_ONLY) + UDF Python em 5M linhas, heap de 1g")
# ----------------------------------------------------------------------------------------------
cache_ruim = largo().persist(StorageLevel.MEMORY_ONLY)        # cache do que nem será usado, e em memória só
dt_cache, pct_cache = gc_pct(lambda: cache_ruim.count(), "materializar o cache (count)")


def com_udf():
    return (
        cache_ruim.withColumn("rotulo", rotular("nome", "texto"))
        .groupBy(F.length("rotulo").alias("tam"))
        .count()
        .collect()
    )


dt_udf, pct_udf = gc_pct(com_udf, "UDF Python sobre o cache")
dt_udf2, pct_udf2 = gc_pct(com_udf, "mesma UDF de novo (blocos que não couberam são recalculados)")

# ----------------------------------------------------------------------------------------------
titulo("2. DIAGNOSTICAR — o que olhar na Spark UI")
# ----------------------------------------------------------------------------------------------
print("""
Spark UI (http://localhost:4040):
  • Executors → coluna 'GC Time' (fica vermelha quando GC > 10% do Task Time). Compare 'Task Time (GC Time)'.
      Regra prática: GC acima de ~10% do tempo de tarefa já é sinal; acima de 20% a 30% é problema.
  • Stages → abra o stage da UDF: 'GC Time' em Summary Metrics e por task; 'Duration' dominada por tempo fora do shuffle.
  • Storage → RDD 'InMemoryTableScan': 'Fraction Cached' < 100% e 'Size in Memory' perto do limite do Storage Memory;
      cache MEMORY_ONLY que não cabe = blocos descartados e recalculados a cada uso.
  • SQL / DataFrame → plano com 'BatchEvalPython' (UDF Python row-based) em vez de projeções nativas.
  • Logs: 'java.lang.OutOfMemoryError: GC overhead limit exceeded' / 'Java heap space'.
""")
print("Heap da JVM:", heap_da_jvm(spark))
print("Aba Executors (via REST):")
resumo_executores(spark)
print("\nPlano da UDF (procure BatchEvalPython):")
cache_ruim.withColumn("rotulo", rotular("nome", "texto")).select("rotulo").explain()

# ----------------------------------------------------------------------------------------------
titulo("3. CORRIGIR — função nativa, cache só do necessário e unpersist")
# ----------------------------------------------------------------------------------------------
cache_ruim.unpersist(blocking=True)                          # a) liberar o cache que não cabia
print("cache antigo liberado.")

# b) cache só das colunas necessárias (e em MEMORY_AND_DISK, que não recalcula o que não coube)
minimo = largo().select("nome", "texto").persist(StorageLevel.MEMORY_AND_DISK)
minimo.count()


# c) função nativa no lugar da UDF: roda dentro da JVM (codegen), sem ida e volta ao Python
def nativo():
    return (
        minimo.withColumn("rotulo", F.concat(F.upper("nome"), F.lit("-"), F.length("texto").cast("string"), F.lit("-"), F.substring("texto", 1, 6)))
        .groupBy(F.length("rotulo").alias("tam"))
        .count()
        .collect()
    )


dt_nat, pct_nat = gc_pct(nativo, "função nativa sobre o cache enxuto")
minimo.unpersist(blocking=True)                              # d) liberar quando terminar de usar

print(f"\nComparativo: UDF {dt_udf2:.1f}s (GC {pct_udf2:.1f}%) → nativo {dt_nat:.1f}s (GC {pct_nat:.1f}%)")
print("""
Se a lógica não existir nativamente: pandas_udf (Arrow, vetorizada) é bem melhor que udf linha a linha.
Prevenção de longo prazo:
  • Preferir funções nativas (pyspark.sql.functions) e SQL; UDF Python só como último recurso.
  • Cache só do que é reutilizado, só das colunas necessárias, com o nível de storage adequado, e SEMPRE unpersist().
  • Acompanhar GC Time (Executors) e Fraction Cached (Storage); ajustar memória por executor/core, não só o número de nós.
  • Em cluster clássico: G1GC (padrão) e evitar objetos gigantes (collect_list enorme, arrays por linha).
""")

# ----------------------------------------------------------------------------------------------
titulo("4. AO CLIENTE")
# ----------------------------------------------------------------------------------------------
print("""
The tasks were slow because the job cached every column in memory only, which did not fit in the executor storage memory, and then applied a row-by-row Python UDF that creates a large number of short-lived objects, so the executors spent a high share of their time in garbage collection, visible in the GC Time column of the Spark UI. As an immediate fix, we replaced the UDF with native Spark functions, cached only the two columns that are reused with a memory-and-disk level, and released the cache with unpersist as soon as it was no longer needed. For the long term, please prefer native functions or vectorized pandas UDFs, cache only what is reused, and monitor GC time and the cached fraction in the Spark UI.
""")

pausar_ui(spark)

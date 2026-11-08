"""L2 · Data skew em join: chave quente (50%) + chaves nulas (10%), com AQE desligado e broadcast desligado.

Ticket do cliente: "A join between our fact table and the customer dimension used to take a couple of
minutes and now one task runs far longer than all the others."

Como rodar:  python local/L2_skew_join.py        (ou F5 com a configuração "L2 · skew join")
Spark UI:    http://localhost:4040 → Stages → stage do join → Summary Metrics e Event Timeline
"""
from spark_session import (
    DATA_DIR,
    cronometrar,
    get_spark,
    marca_stage,
    pausar_ui,
    resumo_stages,
    titulo,
)
from pyspark.sql import functions as F

# No serverless essas configs são bloqueadas; aqui desligamos AQE e broadcast para ver o skew "puro".
CONFS = {
    "spark.driver.memory": "4g",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "16",
}
N = 30_000_000
N_KEYS = 1_000
spark = get_spark("L2_skew_join", **CONFS)


def run_join(fato, dim, on):
    """Left join + agregação por segmento (resultado pequeno)."""
    return (
        fato.join(dim, on, "left")
        .groupBy("segment")
        .agg(F.count("*").alias("n"), F.sum("amount").alias("total"))
        .collect()
    )


def medir(rotulo, fn):
    """Roda uma variante e imprime Median vs Max dos stages de shuffle dela (o que a aba Stages mostra)."""
    marca = marca_stage(spark)
    _, dt = cronometrar(rotulo, fn)
    resumo_stages(spark, top=2, so_com_shuffle=True, desde=marca)
    return dt


# ----------------------------------------------------------------------------------------------
titulo("1. REPRODUZIR — fato com 50% na chave 1 e 10% de nulos, sort-merge join")
# ----------------------------------------------------------------------------------------------
fato_df = (
    spark.range(N)
    .withColumn("r", F.rand(42))
    .withColumn(
        "customer_id",
        F.when(F.col("r") < 0.50, F.lit(1).cast("long"))            # chave quente: 50%
        .when(F.col("r") < 0.60, F.lit(None).cast("long"))          # nulos: 10%
        .otherwise((F.col("id") % N_KEYS) + 2),                     # resto uniforme (2..1001)
    )
    .withColumn("amount", F.rand(7) * 100)
    .drop("r")
)
fato_df.write.mode("overwrite").parquet(str(DATA_DIR / "l2_fact"))
dim_df = (
    spark.range(1, N_KEYS + 2)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", F.concat(F.lit("seg-"), (F.col("customer_id") % 10).cast("string")))
)
dim_df.write.mode("overwrite").parquet(str(DATA_DIR / "l2_dim"))

fato = spark.read.parquet(str(DATA_DIR / "l2_fact"))
dim = spark.read.parquet(str(DATA_DIR / "l2_dim"))
print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"),
      "| autoBroadcastJoinThreshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

t_skew = medir("sort-merge join com skew", lambda: run_join(fato, dim, "customer_id"))

# ----------------------------------------------------------------------------------------------
titulo("2. DIAGNOSTICAR — o que olhar na Spark UI")
# ----------------------------------------------------------------------------------------------
print("""
Spark UI (http://localhost:4040):
  • Stages → clique no stage do join (o de maior duração; 'Shuffle Read' > 0) → 'Summary Metrics for N Completed Tasks':
      - Duration: Max MUITO acima de Median (Max/Median >> 1)  → skew
      - Shuffle Read Size / Records: Max concentra a maior parte dos registros
    Event Timeline: uma barra enorme (Executor Computing Time) enquanto as outras terminam cedo e ficam ociosas.
    Tabela 'Tasks' ordenada por Duration: a task lenta tem Shuffle Read Size muito maior que as demais.
  • SQL / DataFrame → plano físico: SortMergeJoin + Exchange hashpartitioning(customer_id, 16); nenhum BroadcastHashJoin
    e nenhum 'AQEShuffleRead'/'skew' (AQE está desligado).
  • Environment → spark.sql.adaptive.enabled=false e spark.sql.autoBroadcastJoinThreshold=-1 (as causas da configuração).
Confirmando nos dados (equivalente a 'groupBy().count()' na chave):
""")
fato.groupBy("customer_id").count().orderBy(F.desc("count")).show(3)
nulos = fato.filter("customer_id IS NULL").count()
print(f"nulos na chave: {nulos:,} ({nulos / N:.0%}) — todos vão para a MESMA partição do shuffle")
run_join_plan = fato.join(dim, "customer_id", "left").groupBy("segment").count()
run_join_plan.explain(mode="formatted")

# ----------------------------------------------------------------------------------------------
titulo("3. CORRIGIR — broadcast, separar nulos, salting e AQE skew join")
# ----------------------------------------------------------------------------------------------
tempos = {"skew (baseline)": t_skew}

# a) broadcast da dimensão pequena: elimina o shuffle do lado grande
tempos["broadcast"] = medir("a) broadcast da dim", lambda: run_join(fato, F.broadcast(dim), "customer_id"))


# b) separar os nulos: eles nunca casam, então não precisam passar pelo shuffle do join
def sem_nulos():
    com_chave = fato.filter("customer_id IS NOT NULL")
    sem_chave = fato.filter("customer_id IS NULL").withColumn("segment", F.lit(None).cast("string"))
    j1 = com_chave.join(dim, "customer_id", "left").select("id", "customer_id", "amount", "segment")
    j2 = sem_chave.select("id", "customer_id", "amount", "segment")
    return (j1.unionByName(j2).groupBy("segment").agg(F.count("*").alias("n"), F.sum("amount").alias("total")).collect())


tempos["nulos separados"] = medir("b) nulos separados (chave quente ainda presente)", sem_nulos)

# c) salting: espalha a chave quente (e os nulos) em SALT sub-partições
SALT = 16
fato_s = fato.withColumn("salt", (F.rand(11) * SALT).cast("int"))
dim_s = dim.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(SALT - 1))))
tempos["salting"] = medir("c) salting (16 sub-chaves)", lambda: run_join(fato_s, dim_s, ["customer_id", "salt"]))

# d) AQE skew join: o Spark divide em runtime a partição enorme em várias tasks
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "8m")   # dados locais são pequenos
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8m")
tempos["AQE skew join"] = medir("d) AQE skew join ligado", lambda: run_join(fato, dim, "customer_id"))

print("\nComparativo de tempos (s):")
for nome, dt in tempos.items():
    print(f"  {nome:<20} {dt:6.1f}s  ({t_skew / dt:4.1f}x vs baseline)")
print("""
Como ler o comparativo (o que a Spark UI mostra, e não só o tempo total):
  • broadcast: some o shuffle do lado grande, então não existe mais skew de shuffle.
  • nulos separados: NÃO resolve a chave quente (Max/Median continua alto): só tira os nulos do caminho.
  • salting: Max/Median cai (tasks equilibradas) e o Shuffle Read máximo diminui; em local[4] o tempo total pode não
    melhorar, porque a dimensão replicada 16x e o shuffle extra custam; com mais cores a diferença aparece.
  • AQE skew join: o Spark divide a partição gigante em runtime (mais tasks no stage, Max/Median menor).
No plano do AQE (aba SQL, execução final), procure 'AQEShuffleRead' com 'skewed' / 'coalesced' no SortMergeJoin.
Prevenção de longo prazo: monitorar distribuição de chaves (data quality), tratar nulos/sentinelas na ingestão,
preferir broadcast para dimensões, manter AQE ligado e revisar o Summary Metrics após mudanças de volume.
""")

# ----------------------------------------------------------------------------------------------
titulo("4. AO CLIENTE")
# ----------------------------------------------------------------------------------------------
print("""
The slow join was caused by data skew: half of the fact rows share one customer key and a further ten percent have a null key, so a single shuffle task received most of the data while the others finished early, which the Max versus Median task duration in the Spark UI made obvious. As an immediate fix, we broadcast the small customer dimension, which removes the shuffle on the large side, and we showed null isolation, salting and adaptive skew join as alternatives when both sides are large. For the long term, please keep adaptive query execution enabled, check key distribution as a data quality rule, and handle null and sentinel keys at ingestion.
""")

pausar_ui(spark)

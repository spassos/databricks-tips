"""L3 · Spill em shuffle: groupBy/sort grande com poucas partições e pouca memória.

Ticket do cliente: "Our aggregation job is much slower than before and the disks on the workers keep filling up.
The tasks are not failing, they are just very slow."

Como rodar:  python local/L3_spill_shuffle.py        (ou F5 com a configuração "L3 · spill")
Spark UI:    http://localhost:4040 → Stages → colunas 'Spill (Memory)' e 'Spill (Disk)'
Requisito:   ~5 GB livres em disco para o spill (em local/_data/spark-tmp, apagado ao final).
"""
from spark_session import (
    DATA_DIR,
    cronometrar,
    get_spark,
    limpar_dir,
    marca_stage,
    pausar_ui,
    resumo_executores,
    resumo_stages,
    titulo,
)
from pyspark.sql import functions as F

# Pouca memória (1g) + 4 partições de shuffle (cada task recebe um pedaço enorme). No serverless não dá para controlar nada disso.
CONFS = {
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "4",
}
N = 20_000_000
spark = get_spark("L3_spill_shuffle", **CONFS)


def agregacao(df):
    """groupBy com ~20M grupos (uma chave por linha): a agregação parcial não reduz nada e o mapa de agregação
    de cada task (milhões de entradas) não cabe na memória de execução → o Spark despeja em disco (spill)."""
    df.groupBy("id").agg(F.sum("valor").alias("soma"), F.count("*").alias("n")).write.format("noop").mode("overwrite").save()


def ordenacao(df):
    """sort global gravado no sink 'noop' (sem ele o otimizador descarta o sort): cada partição de range
    tem ~5M linhas com payload e o sorter precisa gravar 'runs' em disco."""
    df.orderBy("valor").write.format("noop").mode("overwrite").save()


def medir(rotulo, fn, df):
    marca = marca_stage(spark)
    _, dt = cronometrar(rotulo, lambda: fn(df))
    resumo_stages(spark, top=2, so_com_shuffle=True, desde=marca)
    return dt


# ----------------------------------------------------------------------------------------------
titulo("1. REPRODUZIR — groupBy e orderBy de 20M linhas com shuffle.partitions=4 e driver de 1g")
# ----------------------------------------------------------------------------------------------
dados = (
    spark.range(N)
        .withColumn("valor", F.rand(3))
    .withColumn("payload", F.sha2(F.col("id").cast("string"), 256).substr(1, 32))
)
dados.write.mode("overwrite").parquet(str(DATA_DIR / "l3_dados"))
df = spark.read.parquet(str(DATA_DIR / "l3_dados"))
print("spark.sql.shuffle.partitions =", spark.conf.get("spark.sql.shuffle.partitions"), "| AQE =", spark.conf.get("spark.sql.adaptive.enabled"))

t_agg = medir("groupBy (4 partições)", agregacao, df)
t_sort = medir("orderBy (4 partições)", ordenacao, df)

# ----------------------------------------------------------------------------------------------
titulo("2. DIAGNOSTICAR — o que olhar na Spark UI")
# ----------------------------------------------------------------------------------------------
print("""
Spark UI (http://localhost:4040):
  • Stages → na lista, as colunas 'Shuffle Read' grandes e, abrindo o stage:
      - 'Spill (Memory)' e 'Spill (Disk)' > 0 nos Summary Metrics e na tabela de tasks.
        Spill (Memory) = tamanho DESERIALIZADO dos dados que foram para disco; Spill (Disk) = tamanho gravado (serializado/comprimido).
      - Poucas tasks (4) com Shuffle Read Size de centenas de MB cada → partições grandes demais para a memória de execução.
      - Duration alto com GC Time baixo → o tempo está indo para disco, não para GC.
  • Executors → 'Shuffle Write/Read' e Disk Used; Storage Memory pequeno (1g de heap).
  • SQL / DataFrame → nós Exchange/Sort/HashAggregate: 'spill size' e 'number of sort/aggregate spills'.
  • Environment → spark.sql.shuffle.partitions=4 (a causa), spark.driver.memory=1g, spark.memory.fraction (padrão 0,6).
Regra prática: partição de shuffle entre ~100 e 200 MB; muito acima disso → mais partições (ou AQE coalesce/skew).
""")
print("Executors (via REST):")
resumo_executores(spark)

# ----------------------------------------------------------------------------------------------
titulo("3. CORRIGIR — mais partições de shuffle + AQE coalesce")
# ----------------------------------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "200")                     # começa com muitas partições pequenas...
spark.conf.set("spark.sql.adaptive.enabled", "true")                      # ...e o AQE junta só as pequenas demais
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# Alvo de ~16 MB por partição (folga para a memória de execução): com 1g de heap e 4 tasks simultâneas, cada task tem ~100 MB de memória de execução.
# (Em produção o alvo costuma ser 64 a 128 MB, porque os executores têm bem mais memória por core.)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m")
t_agg2 = medir("groupBy (200 partições + AQE coalesce)", agregacao, df)
t_sort2 = medir("orderBy (200 partições + AQE coalesce)", ordenacao, df)

print("O ganho principal é eliminar o Spill (Memory/Disk = 0 nos stages acima); o tempo total muda pouco porque aqui o custo")
print("é dominado por CPU. Em produção, spill recorrente também enche o disco local dos workers e derruba o throughput.")
print(f"\nComparativo: groupBy {t_agg:.1f}s → {t_agg2:.1f}s | orderBy {t_sort:.1f}s → {t_sort2:.1f}s")
print("""
Atenção: mais partições não é grátis (overhead por task); por isso AQE coalesce: começa com muitas e junta as pequenas.
Prevenção de longo prazo:
  • Dimensionar shuffle partitions pelo VOLUME de shuffle (alvo ~128 MB/partição) ou deixar o AQE decidir.
  • Executores com memória adequada ao tamanho das partições; evitar colunas largas desnecessárias (select antes do shuffle).
  • Alertas em Spill (Disk) > 0 recorrente e em Disk Used dos executores.
""")
limpar_dir(DATA_DIR / "spark-tmp")   # libera o espaço do spill

# ----------------------------------------------------------------------------------------------
titulo("4. AO CLIENTE")
# ----------------------------------------------------------------------------------------------
print("""
The job was slow because the shuffle used only four partitions, so each task had to aggregate and sort hundreds of megabytes that did not fit in its execution memory and spilled to disk repeatedly, as the Spill Memory and Spill Disk columns in the Spark UI showed. As an immediate fix, we raised the number of shuffle partitions and enabled adaptive query execution partition coalescing, which keeps each task small enough to run in memory and removes the spill. For the long term, please size shuffle partitions from the shuffle volume or let adaptive execution manage them, and alert on any recurring disk spill.
""")

pausar_ui(spark)

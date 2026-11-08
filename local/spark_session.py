"""Helpers do ambiente LOCAL (VS Code): SparkSession com Delta + utilitários para ler a Spark UI.

Uso típico em um cenário:

    from spark_session import get_spark, pausar_ui, ...

    spark = get_spark("L1_driver_oom", **{"spark.driver.memory": "1g"})
    ...
    pausar_ui(spark)   # imprime "Spark UI: http://localhost:4040" e espera Enter

As chaves de configuração do Spark têm pontos (ex.: "spark.driver.memory"), por isso são passadas
com `**{...}`. Não há equivalente disto no serverless: lá a Spark UI é substituída pelo Query Profile.
"""
import json
import os
import shutil
import sys
import time
import urllib.request
from pathlib import Path

# Os workers Python do Spark precisam usar o MESMO interpretador (o do venv) que roda o script.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

LOCAL_DIR = Path(__file__).resolve().parent
DATA_DIR = LOCAL_DIR / "_data"          # tudo que os cenários gravam fica aqui (ignorado pelo git)
UI_PORT = 4040

# Configs padrão do laboratório. Cada cenário sobrescreve o que precisar via **confs.
DEFAULT_CONFS = {
    "spark.ui.enabled": "true",
    "spark.ui.port": str(UI_PORT),
    "spark.ui.showConsoleProgress": "true",
    "spark.sql.shuffle.partitions": "8",            # 200 (padrão) é exagero para local[4]
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir": str(DATA_DIR / "warehouse"),
    "spark.local.dir": str(DATA_DIR / "spark-tmp"),  # onde o shuffle/spill é gravado
}


def get_spark(app_name="databricks-tips-local", **confs):
    """Cria (ou devolve) a SparkSession local[4] com Delta habilitado e a Spark UI na porta 4040.

    `confs` são configs extras do cenário (sobrescrevem DEFAULT_CONFS), por exemplo:
        get_spark("L1", **{"spark.driver.memory": "1g", "spark.driver.maxResultSize": "256m"})

    Atenção: `spark.driver.memory` só tem efeito se a JVM ainda não foi iniciada, ou seja, na criação da sessão.
    """
    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "spark-tmp").mkdir(exist_ok=True)

    builder = SparkSession.builder.appName(app_name).master("local[4]")
    for key, value in {**DEFAULT_CONFS, **confs}.items():
        builder = builder.config(key, str(value))

    # Adiciona o jar do Delta (spark.jars.packages) compatível com o delta-spark instalado.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ SparkSession '{app_name}' | Spark {spark.version} | master={spark.sparkContext.master}")
    return spark


# --------------------------------------------------------------------------- utilitários de cenário
def titulo(texto):
    """Cabeçalho das etapas no console: 1. Reproduzir, 2. Diagnosticar, 3. Corrigir, 4. Ao cliente."""
    print("\n" + "=" * 78)
    print(texto)
    print("=" * 78)


def cronometrar(rotulo, fn):
    """Executa fn(), imprime o tempo e devolve (resultado, segundos)."""
    t0 = time.time()
    resultado = fn()
    dt = time.time() - t0
    print(f"⏱  {rotulo}: {dt:.1f}s")
    return resultado, dt


def limpar_dir(path):
    """Apaga e recria um diretório (cenários re-executáveis sem limpeza manual)."""
    path = Path(path)
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def heap_da_jvm(spark):
    """Heap da JVM do driver (MB): max / total / livre, lido via py4j."""
    rt = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
    mb = 1024 * 1024
    return {"max_mb": rt.maxMemory() // mb, "total_mb": rt.totalMemory() // mb, "livre_mb": rt.freeMemory() // mb}


def _ui_port(spark):
    """Porta real da UI (4040, ou 4041... se a 4040 estiver ocupada por outra sessão Spark)."""
    url = spark.sparkContext.uiWebUrl
    return url.rsplit(":", 1)[-1].strip("/") if url else str(UI_PORT)


def pausar_ui(spark):
    """Fim de todo script: mantém a Spark UI no ar enquanto você investiga.

    Defina LAB_NO_PAUSE=1 para pular a pausa (útil em testes automáticos).
    """
    print(f"\nSpark UI: http://localhost:{_ui_port(spark)}")
    if os.environ.get("LAB_NO_PAUSE") == "1":
        print("(LAB_NO_PAUSE=1: pausa ignorada)")
    else:
        input("Pressione Enter para encerrar...")
    spark.stop()


# --------------------------------------------------------------------------- Spark UI via REST API
# A Spark UI expõe os mesmos números da tela em JSON (http://localhost:4040/api/v1). Estas funções
# imprimem o que você veria nas abas Stages / Executors / SQL, para comparar com o que a UI mostra.
def _api(spark, path):
    base = f"http://localhost:{_ui_port(spark)}/api/v1"
    app_id = spark.sparkContext.applicationId
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # ignora proxies corporativos
    with opener.open(f"{base}/applications/{app_id}{path}", timeout=10) as resp:
        return json.load(resp)


def _fmt_bytes(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}"
        n /= 1024


def marca_stage(spark):
    """Devolve o maior stageId já existente; use com resumo_stages(desde=...) para olhar só o que veio depois."""
    todos = _api(spark, "/stages")
    return max((s["stageId"] for s in todos), default=-1)


def resumo_stages(spark, top=3, so_com_shuffle=False, desde=-1):
    """Aba Stages → 'Summary Metrics': Median vs Max por stage (as 'top' stages mais demoradas).

    Skew aparece como Max >> Median em Duration e Shuffle Read Size; spill nas colunas Spill (Memory/Disk);
    pressão de GC em GC Time.
    """
    stages = [s for s in _api(spark, "/stages?status=complete")
              if s.get("numCompleteTasks", 0) > 0 and s["stageId"] > desde]
    if so_com_shuffle:
        stages = [s for s in stages if s.get("shuffleReadBytes", 0) > 0]
    stages.sort(key=lambda s: s.get("executorRunTime", 0), reverse=True)
    for s in stages[:top]:
        sid, att = s["stageId"], s["attemptId"]
        try:
            q = _api(spark, f"/stages/{sid}/{att}/taskSummary?quantiles=0.5,1.0")
        except Exception as e:  # noqa: BLE001
            print(f"  stage {sid}: taskSummary indisponível ({type(e).__name__})")
            continue
        med_dur, max_dur = q["executorRunTime"]
        med_sr, max_sr = q["shuffleReadMetrics"]["readBytes"]
        med_gc, max_gc = q["jvmGcTime"]
        ratio = (max_dur / med_dur) if med_dur else float("inf")
        print(
            f"  stage {sid:<3} tasks={s['numCompleteTasks']:<4} "
            f"Duration median={med_dur:>6.0f}ms max={max_dur:>6.0f}ms (max/median={ratio:4.1f}x) | "
            f"ShuffleRead median={_fmt_bytes(med_sr):>10} max={_fmt_bytes(max_sr):>10} | "
            f"Spill mem={_fmt_bytes(s.get('memoryBytesSpilled'))} disco={_fmt_bytes(s.get('diskBytesSpilled'))} | "
            f"GC max={max_gc:.0f}ms"
        )


def resumo_executores(spark):
    """Aba Executors: memória de storage usada/total, GC Time e % de GC sobre o tempo de tarefa."""
    for e in _api(spark, "/allexecutors"):
        dur = e.get("totalDuration", 0) or 1
        mm = e.get("memoryMetrics", {})
        print(
            f"  executor {e['id']:<7} Storage Memory {_fmt_bytes(e.get('memoryUsed'))} / {_fmt_bytes(e.get('maxMemory'))} | "
            f"GC Time {e.get('totalGCTime', 0) / 1000:.1f}s de {dur / 1000:.1f}s de tarefas ({100 * e.get('totalGCTime', 0) / dur:.1f}%) | "
            f"Failed tasks={e.get('failedTasks', 0)}"
        )


def gc_total(spark):
    """(gc_ms, task_ms) acumulados do executor 'driver' (aba Executors: GC Time e Task Time). Use a diferença entre duas leituras."""
    for e in _api(spark, "/allexecutors"):
        if e["id"] == "driver":
            return e.get("totalGCTime", 0), e.get("totalDuration", 0)
    return 0, 0


def metricas_scan_sql(spark, ultimas=2):
    """Aba SQL / DataFrame → nó de scan: 'number of files read' das últimas queries com scan de arquivos."""
    execs = _api(spark, "/sql?details=true&planDescription=false&length=50")
    execs = [e for e in execs if any(
        any(m["name"] == "number of files read" for m in n.get("metrics", [])) for n in e.get("nodes", []))]
    execs.sort(key=lambda e: e["id"])
    for e in execs[-ultimas:]:
        for n in e["nodes"]:
            ms = {m["name"]: m["value"] for m in n.get("metrics", [])}
            if "number of files read" in ms:
                print(f"  SQL execution {e['id']} · {n['nodeName']}: files read={ms['number of files read']} | "
                      f"size of files read={ms.get('size of files read')} | output rows={ms.get('number of output rows')}")

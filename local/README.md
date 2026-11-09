# Ambiente local (VS Code) · cenários que dependem da Spark UI

O Databricks Free Edition (serverless) não tem **Spark UI** (ela é substituída pelo *Query Profile*) e bloqueia várias `spark.conf`
(AQE, `autoBroadcastJoinThreshold`, memória do driver...). Esta pasta traz **5 cenários que rodam na sua máquina**, com PySpark + Delta Lake,
onde você controla as configs e abre a Spark UI em `http://localhost:4040` enquanto investiga.

Mesmo padrão dos notebooks do repositório: **1. Reproduzir → 2. Diagnosticar → 3. Corrigir → 4. Ao cliente** (3 frases em inglês).
Os notebooks do Databricks na raiz do repo **não são alterados** por nada daqui.

## Índice

1. [Pré-requisitos](#1-pré-requisitos)
2. [Instalação passo a passo](#2-instalação-passo-a-passo)
3. [Rodando no VS Code](#3-rodando-no-vs-code)
4. [Os cenários](#4-os-cenários)
5. [Onde olhar na Spark UI](#5-onde-olhar-na-spark-ui)
6. [Serverless (Query Profile) vs Spark UI](#6-serverless-query-profile-vs-spark-ui)
7. [A Spark UI via código (REST)](#7-a-spark-ui-via-código-rest)
8. [Problemas comuns](#8-problemas-comuns)

## 1. Pré-requisitos

| Item | Versão | Observação |
|---|---|---|
| Python | **3.10 a 3.14** | O PySpark 4.2 suporta Python 3.10+ (testado em 3.14) |
| Java | **17, 21 ou 25** (JDK) | Spark 4.2 roda em Java 17/21/25 (25 precisa ser ≥ 25.0.3). Testado com Temurin 25. Não use 26+ |
| RAM | 8 a 16 GB | Os cenários usam 1 a 4 GB de heap + o processo Python |
| Disco | ~5 GB livres | L3 grava *spill* em `local/_data/spark-tmp` (apagado ao final) |
| Internet | no 1º run | O Spark baixa o jar do Delta (`io.delta:delta-spark_4.2_2.13:4.4.1`) do Maven Central; depois fica em cache (`~/.ivy2`) |

### Versões fixadas (compatíveis entre si)

`local/requirements.txt` fixa **`pyspark==4.2.0`** + **`delta-spark==4.4.1`** (o pacote `delta-spark` 4.4.1 exige
`pyspark>=4.0.1,<=4.2.0`; por isso não usamos o pyspark 4.3). `pandas` (2.x), `numpy` (2.x) e `pyarrow` (>=18) ficam com
faixas compatíveis, pois o `toPandas()` do L1 depende deles.

> Se for trocar versões: Delta 3.0 a 3.3 ↔ Spark 3.5 · Delta 4.0 ↔ Spark 4.0 · Delta 4.1 a 4.3 ↔ Spark 4.0/4.1 · Delta 4.4 ↔ Spark até 4.2.
> Troque `pyspark` e `delta-spark` **juntos** e confira a [matriz oficial](https://docs.delta.io/latest/releases.html).

### Java: verificar

```bash
java -version      # deve mostrar: openjdk version "17.x", "21.x" ou "25.x" (Temurin/Corretto/etc.)
echo $JAVA_HOME    # Windows (PowerShell): echo $env:JAVA_HOME
```

Se `java` não existe ou mostra outra versão, instale um JDK suportado (exemplos com 17; troque por 21 ou 25 se preferir):

**macOS (Homebrew)**
```bash
# Mac Intel em macOS recente: o formula openjdk@XX pode não ter bottle ("no bottle available").
# Nesse caso use o cask (instala em /Library/Java/JavaVirtualMachines, sem o passo do ln):
#   brew install --cask temurin@25 && echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 25)' >> ~/.zshrc
brew install openjdk@17
sudo ln -sfn "$(brew --prefix openjdk@17)/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc && source ~/.zshrc
java -version
```

**Linux**
```bash
# Debian / Ubuntu
sudo apt update && sudo apt install -y openjdk-17-jdk
# Fedora / RHEL / CentOS
sudo dnf install -y java-17-openjdk-devel
# Qualquer distro (SDKMAN): curl -s "https://get.sdkman.io" | bash && sdk install java 17.0.13-tem
sudo update-alternatives --config java            # escolha o 17, se houver várias versões
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
java -version
```

**Windows**
```powershell
winget install EclipseAdoptium.Temurin.17.JDK
# feche e reabra o terminal; se necessário defina JAVA_HOME (ajuste o caminho):
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.13.11-hotspot"
java -version
```
> **Windows:** o Spark local precisa de `winutils.exe`/`HADOOP_HOME` para gravar arquivos (Parquet/Delta). O caminho mais simples é usar
> **WSL2** (Ubuntu) e seguir as instruções de Linux, abrindo o VS Code com a extensão *WSL*. Sem WSL, configure `HADOOP_HOME` com o winutils do Hadoop 3.

## 2. Instalação passo a passo

Na **raiz do repositório** (a pasta que contém `local/` e `.vscode/`):

```bash
# 1) ambiente virtual em .venv (o .vscode/settings.json já aponta para ele)
python3 -m venv .venv              # Python 3.10 a 3.14; no Windows: py -3 -m venv .venv

# 2) ativar
source .venv/bin/activate           # Windows (PowerShell): .venv\Scripts\Activate.ps1

# 3) dependências fixadas
python -m pip install --upgrade pip
pip install -r local/requirements.txt

# 4) conferir
python -c "import pyspark, delta; print('pyspark', pyspark.__version__)"
pip check
```

Teste rápido da sessão (cria a SparkSession com Delta e imprime a versão do Spark):

```bash
cd local
LAB_NO_PAUSE=1 python -c "from spark_session import get_spark; s = get_spark('teste'); print(s.range(5).count()); s.stop()"
```

No primeiro uso o Spark baixa o jar do Delta; as linhas `WARN` sobre *native-hadoop library* e *hostname resolves to a loopback* são normais.

## 3. Rodando no VS Code

1. Abra a **raiz** do repositório no VS Code (*File → Open Folder*). Instale a extensão **Python** (inclui o debugpy).
2. `Ctrl/Cmd+Shift+P` → **Python: Select Interpreter** → `.venv`. (`.vscode/settings.json` já aponta para `${workspaceFolder}/.venv/bin/python`;
   no Windows, ajuste para `${workspaceFolder}/.venv/Scripts/python.exe`.)
3. **Run and Debug** (`Ctrl/Cmd+Shift+D`) → escolha **L1 … L5** → `F5`. Cada cenário tem a sua configuração em `.vscode/launch.json`.
4. O script roda no **terminal integrado** (necessário para o `input()` do final). Ao terminar, ele imprime:
   ```
   Spark UI: http://localhost:4040
   Pressione Enter para encerrar...
   ```
   Abra a URL no navegador e **investigue com a UI ainda no ar**. Enter encerra a sessão.
5. Alternativa sem debug: `python local/L2_skew_join.py` (com o venv ativo).

**Dicas**
- Rode **uma sessão por vez**: a UI usa a porta 4040. Se estiver ocupada, o Spark sobe na 4041, e o script imprime a porta real.
- Pause a leitura entre as etapas: rode, olhe a UI, tente diagnosticar **antes** de ler a etapa 2 (os textos "onde olhar" estão no console).
- `LAB_NO_PAUSE=1 python local/L1_driver_oom.py` pula o `input()` (útil para testar). `rm -rf local/_data` zera os dados gerados.
- Cada script usa os helpers de `local/spark_session.py`: `get_spark(app_name, **confs)` cria a sessão `local[4]` com Delta e `spark.ui.port=4040`;
  as configs do cenário entram por `**{"spark.driver.memory": "1g", ...}`.

## 4. Os cenários

Tempos medidos em um notebook de 4 cores (já com o jar do Delta em cache).

| Script | Reproduz | Configs do cenário | Tempo | O que abrir na UI |
|---|---|---|---|---|
| `L1_driver_oom.py` | `collect()`/`toPandas()` de 8M linhas estoura `spark.driver.maxResultSize` | `driver.memory=1g`, `driver.maxResultSize=256m` | ~1 min | Jobs (falha), Executors (driver), Storage, Environment |
| `L2_skew_join.py` | Join com chave quente (50%) + nulos (10%) | AQE **off**, `autoBroadcastJoinThreshold=-1`, 16 partições | ~1 min | Stages → *Summary Metrics* (Max vs Median) e *Event Timeline*; SQL |
| `L3_spill_shuffle.py` | `groupBy`/`orderBy` de 20M linhas com poucas partições | `shuffle.partitions=4`, `driver.memory=1g`, AQE off | ~1 min | Stages → *Spill (Memory)* / *Spill (Disk)*; Executors |
| `L4_gc_pressure.py` | Cache `MEMORY_ONLY` de colunas largas + UDF Python linha a linha | `driver.memory=1g` | ~1,5 min | Executors → *GC Time*; Stages → *GC Time*; Storage → *Fraction Cached* |
| `L5_data_skipping_delta.py` | Filtro com função na coluna vs intervalo em tabela Delta (+ `OPTIMIZE ZORDER BY`) | `driver.memory=4g` | ~40 s | SQL / DataFrame → nó de scan: *number of files read*; plano |

O que cada correção mostra:
- **L1:** agregar no cluster, `limit`, `toLocalIterator()` e gravar em Parquet em vez de coletar.
- **L2:** compara tempos e Max/Median de: baseline, broadcast, nulos separados, salting e AQE skew join.
- **L3:** `shuffle.partitions=200` + AQE *coalesce* com alvo de 16 MB por partição: o *spill* vai a zero.
- **L4:** função nativa no lugar da UDF, cache só das colunas usadas (`MEMORY_AND_DISK`) e `unpersist()`; compara % de GC e tempo.
- **L5:** filtro em intervalo (60 → poucos arquivos lidos) e `OPTIMIZE ... ZORDER BY (user_id)` (60 → 1 arquivo lido no lookup).

> Os números exatos variam por máquina. O que importa é a **forma**: Max ≫ Median no skew, spill > 0 e depois 0, GC acima de 10% e depois menor, arquivos lidos ≪ total.

## 5. Onde olhar na Spark UI

URL: `http://localhost:4040`. Cada aba responde a uma pergunta diferente:

| Aba | O que mostra | O que ela **prova** |
|---|---|---|
| **Jobs** | Lista de jobs, *Event Timeline*, status e **Failure Reason** | **OOM / limite estourado**: o job do `collect()` aparece *FAILED* com `Total size of serialized results ... > spark.driver.maxResultSize`. Também mostra jobs lentos (muitos stages) e retries |
| **Stages** | Por stage: tempo, *Shuffle Read/Write*, **Spill (Memory/Disk)**, **GC Time**; ao abrir: *Summary Metrics*, *Event Timeline* e tabela de tasks | **Skew**: *Duration* e *Shuffle Read Size* com **Max ≫ Median**. **Spill**: colunas Spill (Memory) e Spill (Disk) > 0. **GC**: *GC Time* por task. Falhas de task e retries |
| **Storage** | RDDs/DataFrames em cache: *Storage Level*, *Fraction Cached*, tamanho em memória e em disco | **Cache que não cabe**: *Fraction Cached* < 100% (blocos descartados e recalculados) e tamanho perto do limite de memória de storage |
| **Environment** | Propriedades do Spark **efetivas**, variáveis de sistema, classpath | **Configuração real**: `spark.sql.adaptive.enabled`, `autoBroadcastJoinThreshold`, `shuffle.partitions`, `driver.memory`, `maxResultSize`. Resolve o "achamos que estava configurado" |
| **Executors** | Por executor/driver: *Storage Memory* usada/total, *Task Time (GC Time)*, shuffle, *Disk Used*, tasks falhas, logs e *thread dump* | **GC**: *GC Time* vermelho (> 10% do Task Time). **OOM/memória**: Storage Memory no limite, tasks falhas, executor perdido. **Skew de executores** (em cluster): um executor com muito mais Task Time |
| **SQL / DataFrame** | Plano lógico/físico de cada query, métricas por operador (linhas, tempo, **files read**, *spill size*), plano final do **AQE** | **Tipo de join** (SortMergeJoin vs BroadcastHashJoin), **filtros empurrados** (`PushedFilters`), **data skipping** (*number of files read* vs total de arquivos) e o que o AQE mudou (`AQEShuffleRead`, *skewed*, *coalesced*) |

Roteiro de leitura para qualquer lentidão: **SQL** (qual plano?) → **Stages** (qual stage domina? Max vs Median? spill? GC?) → **Executors** (memória/GC) → **Environment** (que config estava valendo?).

Em cluster real, a aba **Structured Streaming** (stream com `readStream`) mostra *Input Rate*, *Process Rate*, *Batch Duration* e *Operation Duration*; aqui nenhum cenário usa streaming contínuo.

## 6. Serverless (Query Profile) vs Spark UI

O que o notebook serverless do Free Edition já mostra e o que **só** a Spark UI mostra. A coluna do serverless descreve o que se espera
do Query Profile; confirme no seu workspace, pois o produto evolui.

| Pergunta de troubleshooting | Serverless (Query Profile / saída da célula) | Spark UI (clássico / local) |
|---|---|---|
| Qual é o plano e quanto tempo cada operador gastou? | **Sim**: grafo do plano com tempo, linhas e memória de pico por operador | Sim: aba SQL / DataFrame |
| Quantos arquivos/bytes o scan leu? Houve *data skipping*? | **Sim**: nó de scan com arquivos lidos/podados e bytes lidos | Aba SQL: *number of files read*, *size of files read* (no Delta OSS não há "files pruned" explícito) |
| Que tipo de join foi usado? O filtro foi empurrado? | **Sim**: plano e `EXPLAIN FORMATTED` | Sim: SQL → plano físico |
| Há **skew** de verdade? (Max vs Median por task) | **Parcial**: indícios por operador; sem distribuição por task | **Só Spark UI**: Stages → *Summary Metrics*, tabela de tasks e *Event Timeline* |
| Quanto **spill** (memória/disco)? | **Parcial**: métricas de spill por operador, quando disponíveis | **Só Spark UI**: colunas *Spill (Memory)* e *Spill (Disk)* por stage/task |
| **GC Time** | **Não** | **Só Spark UI** (Executors e Stages) e métricas do cluster |
| Memória do driver/executor, tasks falhas, executor perdido | **Não** (compute gerenciado) | **Só Spark UI**: Executors; logs do driver/executor |
| Configs efetivas (`spark.conf` / Environment) | **Parcial**: só as que o serverless permite ler; muitas são bloqueadas | **Só Spark UI**: aba Environment (e você pode alterá-las) |
| Cache (`cache()`/`persist()`) e *Fraction Cached* | **Não**: cache de DataFrame não é suportado no serverless | **Só Spark UI**: aba Storage |
| Falha por `maxResultSize` / OOM do driver | Mensagem de erro na célula; sem logs de cluster | **Só local/clássico**: Jobs → *Failure Reason*, driver logs |
| Alterar AQE, `autoBroadcastJoinThreshold`, `shuffle.partitions`, memória | **Bloqueado** (ou gerenciado automaticamente) | **Local/clássico**: você controla tudo |
| Progresso de streaming | Painel da query no notebook / `recentProgress` | Aba Structured Streaming + `recentProgress` |

Por isso, na entrevista: **no serverless**, comece pelo Query Profile e `EXPLAIN FORMATTED`; para skew, spill e GC, diga que pediria a Spark UI
(ou métricas do cluster clássico) e explique **qual métrica** procuraria. Estes cenários locais treinam exatamente esse olhar.

## 7. A Spark UI via código (REST)

A UI expõe os mesmos números em JSON (`http://localhost:4040/api/v1`). `spark_session.py` traz helpers que imprimem o equivalente das abas
para você comparar com a tela e que também servem para validar os cenários sem abrir o navegador:

| Helper | Equivalente na UI |
|---|---|
| `resumo_stages(spark, top, so_com_shuffle, desde)` | Stages → *Summary Metrics*: Duration (Median/Max), Shuffle Read, Spill, GC |
| `marca_stage(spark)` | Marca o último stage para olhar só o que veio depois |
| `resumo_executores(spark)` | Executors: Storage Memory usada/total, *GC Time* |
| `gc_total(spark)` | Executors: GC Time e Task Time acumulados (use a diferença) |
| `metricas_scan_sql(spark)` | SQL / DataFrame: *number of files read* do nó de scan |
| `heap_da_jvm(spark)` | Heap máximo/total/livre da JVM do driver |

Endpoints úteis para explorar à mão: `/applications`, `/applications/<id>/stages`, `/stages/<id>/<attempt>/taskSummary`,
`/allexecutors`, `/environment`, `/sql?details=true`.

## 8. Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `JAVA_HOME is not set` / `java: command not found` | Java ausente ou fora do PATH | Instale um JDK 17/21/25 e defina `JAVA_HOME` (seção 1) |
| `Unsupported class file major version` | Java mais novo/antigo que o suportado | Use JDK 17, 21 ou 25 |
| Falha ao baixar `delta-spark_4.2_2.13` (Ivy / Maven) | Sem internet ou proxy no 1º run | Rode uma vez com internet (ou configure o proxy do Java/Ivy); depois fica em `~/.ivy2` |
| `Address already in use` / UI em 4041 | Outra sessão Spark usando a 4040 | Encerre a outra sessão; o script imprime a porta real |
| `py4j.protocol.Py4JNetworkError: Answer from Java side is empty` | A JVM morreu (normalmente `OutOfMemoryError`) | Veja o log acima do erro; aumente `spark.driver.memory` do cenário ou reduza `N` |
| Cenário muito lento | Poucos cores ou pouca RAM | Reduza o `N` no topo do script (L1/L2/L3 aceitam facilmente) |
| `No space left on device` no L3 | *Spill* grande em `local/_data/spark-tmp` | Libere ~5 GB ou reduza `N`; o script limpa a pasta ao final |
| Windows: erros de `winutils`/`HADOOP_HOME` | Spark local no Windows nativo | Use WSL2 (recomendado) ou configure `HADOOP_HOME` |

Para recomeçar do zero: `rm -rf local/_data` (apaga dados gerados, warehouse e spill).

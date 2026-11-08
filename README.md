# databricks-tips · Live Troubleshooting Lab

Laboratório para praticar **live troubleshooting** de Databricks / Apache Spark (perfil *Sr. Designated Support Engineer*),
pensado para o **Databricks Free Edition (serverless)**. Cada cenário reproduz um erro real, mostra como diagnosticar,
corrige e fecha com a mensagem ao cliente.

Comentários e markdown em português; as mensagens ao "cliente" estão em inglês, como na entrevista.

## Índice

1. [Importar no Databricks](#1-importar-no-databricks)
2. [Como usar (fluxo de treino)](#2-como-usar-fluxo-de-treino)
3. [Guia de cada notebook](#3-guia-de-cada-notebook)
4. [Re-executar e limpar](#4-re-executar-e-limpar)
5. [Limitações do serverless](#5-limitações-do-serverless)
6. [Validar a sintaxe localmente](#6-validar-a-sintaxe-localmente)
7. [Ambiente local (VS Code) com Spark UI](#7-ambiente-local-vs-code-com-spark-ui)

## 1. Importar no Databricks

Os arquivos `.py` estão no formato *source* do Databricks (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC %md`).
Ao abrir pelo workspace, viram notebooks normais, com células.

### Opção A — Git folder (recomendada)

1. No workspace: **Workspace → Create → Git folder**.
2. **Git repository URL:** `https://github.com/spassos/databricks-tips` · **Git provider:** GitHub.
   (Repositório privado: antes, em *Settings → Linked accounts*, adicione um Personal Access Token do GitHub.)
3. Clique em **Create Git folder**. Os notebooks aparecem na pasta criada.
4. Abra um notebook e, no seletor de compute (canto superior direito), escolha **Serverless**.
5. Para atualizar depois: no Git folder, botão da branch → **Pull**.

### Opção B — Importar os arquivos à mão

1. Baixe os `.py` (ou clone o repo localmente).
2. **Workspace → Create → Import** (ou menu `⋮` de uma pasta → *Import*) e selecione os arquivos.
3. Mantenha todos **na mesma pasta**: os notebooks chamam `%run ./00_setup` por caminho relativo.

> Em ambas as opções o catálogo usado é `workspace` (padrão do Free Edition) e os objetos ficam em `workspace.lab`.

## 2. Como usar (fluxo de treino)

Cada cenário tem 4 etapas, sempre com este título:

| Etapa | O que fazer |
|---|---|
| **1. Reproduzir** | Rode a célula de código. Os erros esperados são capturados por `expect_error`, então o notebook não para. |
| **2. Diagnosticar** | **Antes de abrir**, pare e tente descobrir a causa em voz alta, como na entrevista. Depois rode a célula e compare com **"O que procurar"**. |
| **3. Corrigir** | Aplique o fix imediato e leia a prevenção de longo prazo. |
| **4. Ao cliente** | Escreva a sua versão da resposta em 3 frases (root cause, immediate fix, long term) e só depois leia a do lab. |

Dica: use um cronômetro de 10 a 15 min por cenário, como numa entrevista de live troubleshooting.

## 3. Guia de cada notebook

### `00_setup.py` — preparação

- **Para que serve:** cria o schema `workspace.lab`, os volumes `lab.ckpt` (checkpoints) e `lab.landing` (arquivos do Auto Loader), define `CK`, `LANDING` e os helpers `expect_error`, `reset_dir`, `timed`, `history`.
- **Como usar:** rode uma vez com **Run all** para confirmar que tudo foi criado (o último passo lista os volumes). Depois disso você não precisa abri-lo: os outros notebooks o executam sozinhos via `%run ./00_setup`.
- **Se falhar:** `Catalog 'workspace' not found` indica que o seu workspace usa outro catálogo padrão. Troque o nome em `USE CATALOG` e em `CATALOG`/`CK`/`LANDING`.

### `01_spark.py` — Apache Spark

| ID | Cenário | O que você treina |
|---|---|---|
| S1 | Driver OOM com `collect()` / `toPandas()` | Estimar tamanho em memória, agregar no cluster, `limit`, gravar em tabela |
| S2 | Data skew em join (chave quente + nulos) | `groupBy(chave).count()`, `EXPLAIN FORMATTED`, broadcast, separar nulos, salting |
| S3 | Query lenta sem data skipping | Função na coluna vs intervalo, `PushedFilters`, liquid clustering |

- **Como usar:** execute célula a célula. Cada cenário é independente; pode rodar só o que quiser.
- **Atenção:** S1 pode reiniciar o kernel em vez de dar exceção; reduza `N_S1`. S2 e S3 criam 10M e 4M de linhas, então esperam alguns minutos no Free Edition.
- **Query Profile:** no lugar da Spark UI, abra o *Query Profile* da célula (link "View" abaixo do resultado) e procure o nó de scan (*files pruned*) e o de shuffle (task mais lenta vs mediana).
- Células marcadas **requer cluster clássico** (AQE, `autoBroadcastJoinThreshold`, driver) apenas imprimem "bloqueado no serverless"; é esperado.

### `02_delta.py` — Delta Lake

| ID | Cenário | O que você treina |
|---|---|---|
| D1 | `MERGE` com origem duplicada | Achar chaves duplicadas, `QUALIFY ROW_NUMBER()`, guard-rail de unicidade |
| D2 | Schema mismatch no append | Diff de schema, cast, `mergeSchema` (e por que não resolve tipo) |
| D3 | Time travel, `RESTORE`, `VACUUM` | `DESCRIBE HISTORY`, última versão boa, `RESTORE`, retenção, `SHALLOW CLONE` |
| D4 | Small files, `OPTIMIZE`, liquid clustering | `DESCRIBE DETAIL` (tamanho médio), `OPTIMIZE`, `CLUSTER BY` |
| D5 | Escrita concorrente | Exceções de concorrência, `readVersion`/`isolationLevel`, retry com backoff |

- **Como usar:** as tabelas são recriadas (`CREATE OR REPLACE`) a cada execução, então repita à vontade.
- **D3:** o `VACUUM` com retenção zero é **irreversível para a tabela `lab.d3_trap`** (só ela; é de propósito). A tabela do `RESTORE` é outra.
- **D5:** é uma corrida entre threads; se nenhuma falha aparecer, rode a célula de reprodução de novo.

### `03_streaming.py` — Structured Streaming

| ID | Cenário | O que você treina |
|---|---|---|
| ST1 | Stream quebra após `DELETE` na fonte | `skipChangeCommits`, Change Data Feed + `foreachBatch`/`MERGE` |
| ST2 | Agregação sem watermark | Erro de `append`, `stateOperators`, `withWatermark` |
| ST3 | Auto Loader com coluna nova | `_schemas`, restart esperado, `mergeSchema`, modo `rescue` |
| ST4 | Stream atrasando | `recentProgress`, `durationMs`, backlog, `maxFilesPerTrigger` |

- **Como usar:** todos os streams rodam com `trigger(availableNow=True)` e `awaitTermination()`: cada execução processa o que há e termina. Não deixe streams contínuos abertos.
- **Reset automático:** cada cenário apaga seu checkpoint (`/Volumes/workspace/lab/ckpt/...`) e recria tabelas/arquivos antes de reproduzir.
- **ST4:** leva alguns minutos (30 micro-batches de propósito). Se `recentProgress` vier vazio no serverless, rode a célula de reprodução de novo.

### `04_platform.py` — Plataforma

| ID | Cenário | O que você treina |
|---|---|---|
| A1 | Conectividade | Diagnóstico em camadas: DNS → TCP → TLS → HTTP com `socket`/`ssl` |
| A2 | Model Serving com dependência faltando | `get_model_dependencies`, `mlflow.models.predict(env_manager=...)`, relogar com versões fixas |
| A4 | Permissões no Unity Catalog | `current_user()`, `SHOW GRANTS`, cadeia `USE CATALOG` → `USE SCHEMA` → `SELECT` |

- **Como usar:** esses cenários dependem do workspace (rede, recursos, privilégios). O que importa é o **método**, não reproduzir o erro idêntico.
- **A1:** edite a lista `REQUIRED` para testar destinos reais do seu caso.
- **A2:** se o ambiente isolado não puder ser criado no seu workspace, o erro será de criação do ambiente; use o diagnóstico estático da célula 2.
- **A4:** o `GRANT` de demonstração é revogado no final (`finally`).

### `99_cheatsheet.py` — consulta rápida

Só markdown: tabelas **sintoma → primeiro comando → fix típico** (Spark, Delta, Streaming, Plataforma), limites do serverless e um roteiro de resposta para falar em voz alta. Leia antes da entrevista, **depois** de fazer os cenários.

### Ordem sugerida

`00_setup` → `01_spark` → `02_delta` → `03_streaming` → `04_platform` → `99_cheatsheet`. Os notebooks são independentes; a ordem é só didática.

## 4. Re-executar e limpar

Cada cenário se reseta sozinho: tabelas com `CREATE OR REPLACE`, checkpoints e landing zones apagados antes de reproduzir. Basta rodar o cenário do início.

Para remover tudo que o lab criou:

```sql
DROP SCHEMA IF EXISTS workspace.lab CASCADE;
```

## 5. Limitações do serverless

- **Várias `spark.conf` não podem ser alteradas** (AQE on/off, `autoBroadcastJoinThreshold`, memória do driver...). Essas células ficam em `try/except` e marcadas com **requer cluster clássico**.
- **Sem RDD / `sparkContext`**; a Spark UI é substituída pelo **Query Profile**.
- **Streaming:** sempre `trigger(availableNow=True)` + `awaitTermination()`.

## 6. Validar a sintaxe localmente

Não precisa de Databricks nem de PySpark:

```bash
# Validação completa: formato do notebook, sintaxe de cada célula, estrutura dos cenários e regras do serverless
python3 scripts/check_syntax.py

# Verificação rápida: os arquivos são Python válido (as linhas "# MAGIC" são comentários)
python3 -m py_compile 00_setup.py 01_spark.py 02_delta.py 03_streaming.py 04_platform.py 99_cheatsheet.py
```

O `check_syntax.py` confere: cabeçalho `# Databricks notebook source`, `ast.parse` de cada célula Python (células `%md`, `%sql` e `%run` são puladas),
as 4 etapas de cada cenário na ordem, a mensagem ao cliente com 3 frases, `spark.conf.set` sempre dentro de `try` e precedido do aviso
"requer cluster clássico", ausência de RDD/`sparkContext`, e streams com `availableNow` + `awaitTermination`.

## 7. Ambiente local (VS Code) com Spark UI

O serverless não tem Spark UI e bloqueia várias configs, então os cenários que dependem delas (skew com AQE desligado, spill, GC,
driver pequeno, data skipping com plano físico) têm uma versão **local** em [`local/`](local/README.md): PySpark 3.5.3 + Delta 3.3.0 em `local[4]`,
Spark UI em `http://localhost:4040` e uma configuração de debug por cenário em `.vscode/launch.json`.

| Script | Cenário |
|---|---|
| `local/L1_driver_oom.py` | Driver com pouca memória: `collect()`/`toPandas()` e `spark.driver.maxResultSize` |
| `local/L2_skew_join.py` | Skew em join (chave quente + nulos), AQE desligado; broadcast, nulos, salting e AQE skew join |
| `local/L3_spill_shuffle.py` | Spill de memória/disco com poucas partições de shuffle; ajuste de partições e AQE coalesce |
| `local/L4_gc_pressure.py` | GC alto por cache pesado e UDF Python; funções nativas e `unpersist()` |
| `local/L5_data_skipping_delta.py` | Data skipping em Delta: função na coluna vs intervalo, `OPTIMIZE ZORDER BY` |

Pré-requisitos: **Java 17** e Python 3.10/3.11. Instalação resumida (detalhes, Java por sistema operacional e o guia
**"Onde olhar na Spark UI"** estão em [`local/README.md`](local/README.md)):

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt
python local/L2_skew_join.py      # ou F5 no VS Code; no final, abra http://localhost:4040 e aperte Enter para encerrar
```

A validação (`python3 scripts/check_syntax.py`) também cobre `local/*.py` e os JSON de `.vscode/`, sem as regras específicas do serverless.

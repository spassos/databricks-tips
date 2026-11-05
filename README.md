# databricks-tips · Live Troubleshooting Lab

Laboratório para praticar **live troubleshooting** de Databricks / Apache Spark (perfil *Sr. Designated Support Engineer*),
pensado para o **Databricks Free Edition (serverless)**. Cada cenário reproduz um erro real, mostra como diagnosticar,
corrige e fecha com a mensagem ao cliente.

Comentários e markdown em português; as mensagens ao "cliente" estão em inglês, como na entrevista.

## Estrutura

| Arquivo | Conteúdo |
|---|---|
| `00_setup.py` | `USE CATALOG workspace`, schema `lab`, volumes `lab.ckpt` e `lab.landing`, variáveis `CK` / `LANDING` e helpers |
| `01_spark.py` | S1 driver OOM · S2 data skew em join · S3 query lenta sem data skipping |
| `02_delta.py` | D1 MERGE com origem duplicada · D2 schema mismatch · D3 time travel / RESTORE / VACUUM · D4 small files / OPTIMIZE / liquid clustering · D5 escrita concorrente |
| `03_streaming.py` | ST1 DELETE na fonte · ST2 agregação sem watermark · ST3 Auto Loader com coluna nova · ST4 stream atrasando |
| `04_platform.py` | A1 conectividade · A2 Model Serving com dependência faltando · A4 permissões no Unity Catalog |
| `99_cheatsheet.py` | Só markdown: sintoma → primeiro comando → fix típico |
| `scripts/check_syntax.py` | Validação local de sintaxe e estrutura (sem precisar do Databricks) |

### Formato de cada cenário

1. **Reproduzir:** código que dispara o erro real (erros esperados são capturados por `expect_error`, então "Run all" não para).
2. **Diagnosticar:** o que um support engineer rodaria (`DESCRIBE HISTORY`, `DESCRIBE DETAIL`, `EXPLAIN FORMATTED`, `SHOW GRANTS`, `recentProgress`...) e **o que procurar** na saída.
3. **Corrigir:** fix imediato + prevenção de longo prazo.
4. **Ao cliente:** resumo em inglês de 3 frases (root cause, immediate fix, long term).

## Como conectar o repositório no Databricks

1. No workspace: **Workspace → Create → Git folder**.
2. **Git repository URL:** `https://github.com/spassos/databricks-tips` · provider **GitHub**.
   (Repositório privado: configure antes uma credencial em *Settings → Linked accounts*, com Personal Access Token.)
3. Clique em **Create Git folder**. Os `.py` aparecem como notebooks (o cabeçalho `# Databricks notebook source` é o que os identifica).
4. Abra qualquer notebook e, no seletor de compute, escolha **Serverless**.

## Ordem de execução

1. `00_setup.py`: rode uma vez para confirmar que o catálogo `workspace` existe e que os volumes foram criados.
   Todos os outros notebooks chamam `%run ./00_setup`, então ele roda sozinho quando necessário.
2. `01_spark.py` → `02_delta.py` → `03_streaming.py` → `04_platform.py`. Os notebooks são independentes entre si; a ordem é só didática.
3. `99_cheatsheet.py`: leia no fim (ou antes da entrevista) para consolidar.

**Como treinar:** rode só a célula *Reproduzir*, **pare** e tente diagnosticar sozinho, em voz alta, antes de abrir *Diagnosticar*.
Depois escreva a sua versão da mensagem ao cliente antes de ler a *Ao cliente*.

## Re-executar um cenário

Cada cenário se reseta sozinho: tabelas com `CREATE OR REPLACE`, checkpoints e landing zones apagados antes de reproduzir
(`reset_dir`). Basta rodar o cenário do início. Tabelas ficam em `workspace.lab.*`; para limpar tudo:

```sql
DROP SCHEMA IF EXISTS workspace.lab CASCADE;
```

## Limitações do serverless (e como o lab lida com elas)

- **Várias `spark.conf` não podem ser alteradas** (AQE on/off, `autoBroadcastJoinThreshold`, memória do driver...).
  Essas células ficam em `try/except` e marcadas com **requer cluster clássico**.
- **Sem RDD / `sparkContext`**; a Spark UI é substituída pelo **Query Profile**.
- **Streaming:** sempre `trigger(availableNow=True)` + `awaitTermination()`.
- **S1** pode terminar com reinício do kernel em vez de exceção; se acontecer, reduza `N_S1`.
- **A1, A2 e A4** dependem do workspace (rede, Model Serving, privilégios). O que se treina é o método de diagnóstico.

## Validar a sintaxe localmente

Não precisa de Databricks nem de PySpark:

```bash
# 1) Validação completa: formato do notebook, sintaxe de cada célula, estrutura dos cenários e regras do serverless
python3 scripts/check_syntax.py

# 2) Verificação rápida: os arquivos são Python válido (as linhas "# MAGIC" são comentários)
python3 -m py_compile 00_setup.py 01_spark.py 02_delta.py 03_streaming.py 04_platform.py 99_cheatsheet.py
```

O `check_syntax.py` confere: cabeçalho `# Databricks notebook source`, separadores `# COMMAND ----------`, `ast.parse` de cada célula Python
(células `%md`, `%sql` e `%run` são puladas), as 4 etapas de cada cenário na ordem certa, a mensagem ao cliente com 3 frases,
`spark.conf.set` sempre dentro de `try` e precedido do aviso "requer cluster clássico", ausência de RDD/`sparkContext`,
e streams com `availableNow` + `awaitTermination`.

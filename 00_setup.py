# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do laboratório
# MAGIC
# MAGIC Cria o schema `workspace.lab`, os volumes `lab.ckpt` (checkpoints) e `lab.landing` (arquivos para Auto Loader)
# MAGIC e define as variáveis `CK` e `LANDING` + alguns helpers.
# MAGIC
# MAGIC Os demais notebooks chamam este com `%run ./00_setup`, então você não precisa rodá-lo à mão,
# MAGIC mas rode uma vez para confirmar que o catálogo `workspace` existe no seu Free Edition.
# MAGIC
# MAGIC **Compute:** serverless. Idempotente: pode rodar quantas vezes quiser.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lab

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA lab

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS lab.ckpt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS lab.landing

# COMMAND ----------

import json
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "lab"

# Caminhos dos volumes (Unity Catalog)
CK = f"/Volumes/{CATALOG}/{SCHEMA}/ckpt"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


def reset_dir(path):
    """Apaga e recria um diretório (usado para zerar checkpoints e landing zones antes de reproduzir)."""
    try:
        dbutils.fs.rm(path, True)
    except Exception:
        pass  # não existia ainda
    dbutils.fs.mkdirs(path)


def expect_error(fn, label=""):
    """Roda fn() esperando um erro. Imprime o erro (como o cliente o veria) sem interromper o notebook."""
    try:
        fn()
    except Exception as e:  # noqa: BLE001 - queremos capturar qualquer erro do cenário
        print(f"❌ {label} → {type(e).__name__}")
        print(str(e)[:1500])
        return e
    print(f"⚠️ {label}: NÃO falhou. Confira se o cenário foi resetado ou se o ambiente mudou.")
    return None


def timed(label, fn):
    """Mede o tempo de fn() e devolve o resultado."""
    t0 = time.time()
    result = fn()
    print(f"⏱ {label}: {time.time() - t0:.1f}s")
    return result


def history(table):
    """Atalho para DESCRIBE HISTORY."""
    return spark.sql(f"DESCRIBE HISTORY {table}")


# COMMAND ----------

print("catalog :", spark.sql("SELECT current_catalog()").first()[0])
print("CK      :", CK)
print("LANDING :", LANDING)
display(spark.sql("SHOW VOLUMES IN lab"))

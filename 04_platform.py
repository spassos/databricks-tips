# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Plataforma — A1, A2, A4
# MAGIC
# MAGIC | ID | Cenário |
# MAGIC |---|---|
# MAGIC | A1 | Conectividade: DNS e porta (`socket`) |
# MAGIC | A2 | Model Serving com dependência faltando (`mlflow.models.predict` com `env_manager`) |
# MAGIC | A4 | Permissões no Unity Catalog (`current_user`, `SHOW GRANTS`) |
# MAGIC
# MAGIC > Estes cenários dependem do **workspace** (rede, privilégios, recursos disponíveis no Free Edition).
# MAGIC > Os resultados concretos podem diferir dos descritos; o que treinamos é o **método de diagnóstico**.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1 — Conectividade (DNS e porta)
# MAGIC
# MAGIC **Ticket do cliente:** "Our notebook cannot reach the on-prem SQL Server / an external API. It just hangs and then times out."
# MAGIC
# MAGIC ### 1. Reproduzir — A1
# MAGIC
# MAGIC Testamos quatro tipos de destino: host que resolve e abre a porta, host inexistente (DNS), porta fechada em host público e IP privado sem rota.

# COMMAND ----------

import socket
import ssl


def probe(host, port, timeout=5):
    """Testa DNS e depois TCP. Classifica a falha para sabermos EM QUE CAMADA o problema está."""
    res = {"host": host, "port": port, "dns": None, "result": None, "ms": None}
    t0 = time.time()
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        res["dns"] = ",".join(sorted({i[4][0] for i in infos})[:2])
    except socket.gaierror as e:
        res.update(dns="FALHOU", result=f"DNS_FAILURE ({e})")
        res["ms"] = int((time.time() - t0) * 1000)
        return res
    try:
        with socket.create_connection((host, port), timeout=timeout):
            res["result"] = "TCP_OK"
    except TimeoutError:
        res["result"] = "TIMEOUT (pacotes descartados: firewall / NSG / egress / rota)"
    except ConnectionRefusedError:
        res["result"] = "REFUSED (host alcançável, nada escutando na porta)"
    except OSError as e:
        res["result"] = f"OS_ERROR errno={e.errno} ({e.strerror})"
    res["ms"] = int((time.time() - t0) * 1000)
    return res


TARGETS = [
    ("pypi.org", 443),                           # normalmente liberado
    ("this-host-does-not-exist.invalid", 443),   # DNS
    ("example.com", 5432),                       # porta de banco em host público (fechada/filtrada)
    ("10.255.255.1", 1433),                      # IP privado sem rota (simula SQL Server on-prem)
]

import pandas as pd

display(pd.DataFrame([probe(h, p) for h, p in TARGETS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — A1
# MAGIC
# MAGIC Método em **camadas** (sempre de baixo para cima; a primeira que falha é a causa):
# MAGIC
# MAGIC | Camada | Teste | Falha típica → causa |
# MAGIC |---|---|---|
# MAGIC | DNS | `socket.getaddrinfo` | `gaierror` → nome errado, DNS privado/zona não vinculada, resolvedor sem acesso |
# MAGIC | TCP | `socket.create_connection` | `timeout` → firewall/NSG/UDR/egress control descartando; `refused` → host ok, serviço parado/porta errada |
# MAGIC | TLS | `ssl.wrap_socket` | erro de certificado → proxy com inspeção TLS / CA corporativa ausente |
# MAGIC | HTTP | request real | 401/403/407 → credencial, allowlist ou proxy; 5xx → problema do servidor |
# MAGIC
# MAGIC **Perguntas ao cliente:** qual compute (serverless ou clássico)? região/VNet? o destino é público ou privado? já funcionou antes? o que mudou (firewall, política de rede, DNS)?

# COMMAND ----------

import os
import urllib.request


def layered_check(host, port=443):
    print(f"--- {host}:{port} ---")
    r = probe(host, port)
    print("DNS/TCP:", r["result"], "| ips:", r["dns"], "| ms:", r["ms"])
    if r["result"] != "TCP_OK":
        print("→ parar aqui: o problema está na camada acima (DNS/TCP).\n")
        return
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=5) as s, ctx.wrap_socket(s, server_hostname=host) as ss:
            print("TLS:", ss.version())
    except Exception as e:  # noqa: BLE001
        print("TLS FALHOU:", type(e).__name__, str(e)[:150])
        return
    try:
        with urllib.request.urlopen(f"https://{host}/", timeout=8) as resp:
            print("HTTP:", resp.status)
    except Exception as e:  # noqa: BLE001
        print("HTTP:", type(e).__name__, str(e)[:150])
    print()


layered_check("pypi.org")
print("Proxies no ambiente:", {k: v for k, v in os.environ.items() if "proxy" in k.lower()} or "nenhum")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — A1
# MAGIC
# MAGIC **Fix imediato (depende da camada que falhou):**
# MAGIC - **DNS:** corrigir o nome; no clássico, vincular a zona DNS privada à VNet / usar o resolvedor correto.
# MAGIC - **TCP timeout:** liberar a rota e a porta (security group/NSG, firewall, UDR, peering/Private Link). No **serverless**: política de rede (egress control) do account/workspace
# MAGIC   permitindo o destino, e **Network Connectivity Configuration (NCC)** com private endpoint para recursos privados.
# MAGIC - **Refused:** serviço no ar? porta e IP corretos?
# MAGIC - **TLS:** instalar a CA corporativa ou ajustar a inspeção.
# MAGIC
# MAGIC **Prevenção de longo prazo:** runbook de conectividade (esta função como teste padrão), egress allowlist versionada (IaC), monitoramento sintético do destino,
# MAGIC documentar IPs/portas e donos de cada dependência.
# MAGIC
# MAGIC A célula abaixo é o **teste de aceitação** pós-correção: deve devolver `TCP_OK` para tudo que o cliente precisa alcançar.

# COMMAND ----------

REQUIRED = [("pypi.org", 443)]   # troque pelos destinos que o cliente precisa alcançar (ex.: ("sqlserver.corp.local", 1433))

results = [probe(h, p) for h, p in REQUIRED]
for r in results:
    print(("✅" if r["result"] == "TCP_OK" else "❌"), f"{r['host']}:{r['port']} → {r['result']}")
print("Aceitação:", "OK" if all(r["result"] == "TCP_OK" for r in results) else "PENDENTE — ainda há destinos bloqueados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — A1
# MAGIC
# MAGIC > We tested the path layer by layer and found that the hostname resolves correctly but the TCP connection to the target port times out, which means packets are being dropped between the compute and the destination rather than refused by the service. As an immediate fix, please allow outbound traffic from the compute to that host and port in your firewall or network policy, and for private resources attach a network connectivity configuration with a private endpoint. For the long term, we recommend keeping the allowed destinations in version-controlled configuration and running this connectivity check as part of your deployment pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2 — Model Serving com dependência faltando
# MAGIC
# MAGIC **Ticket do cliente:** "Our serving endpoint fails to start. The build log ends with `ModuleNotFoundError: No module named 'sklearn'`, but the model works in my notebook."
# MAGIC
# MAGIC ### 1. Reproduzir — A2
# MAGIC
# MAGIC Logamos um modelo scikit-learn com `pip_requirements` **sem** `scikit-learn`. No notebook funciona (o pacote já está instalado); num ambiente isolado,
# MAGIC como o do Model Serving, não. `mlflow.models.predict(..., env_manager=...)` recria esse ambiente para testar antes do deploy.
# MAGIC
# MAGIC > Criar o ambiente isolado exige rede para PyPI e `virtualenv`/`uv`. Se o ambiente não puder ser criado no seu workspace, o erro será de criação do ambiente
# MAGIC > e não do pacote; nesse caso siga para o diagnóstico estático (célula 2), que não depende disso.

# COMMAND ----------

import mlflow
import pandas as pd
import sklearn
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from mlflow.models import infer_signature

X, y = make_classification(n_samples=200, n_features=4, random_state=0)
X_df = pd.DataFrame(X, columns=[f"x{i}" for i in range(4)])
model = LogisticRegression().fit(X_df, y)


def log_sklearn(m, **kwargs):
    """Compatível com MLflow 2.x (artifact_path) e 3.x (name)."""
    try:
        return mlflow.sklearn.log_model(m, name="model", **kwargs)
    except TypeError:
        return mlflow.sklearn.log_model(m, artifact_path="model", **kwargs)


with mlflow.start_run(run_name="a2_missing_dependency"):
    info_bad = log_sklearn(model, pip_requirements=["mlflow", "numpy", "pandas"])   # BUG: faltou scikit-learn
MODEL_URI_BAD = info_bad.model_uri
print("modelo logado:", MODEL_URI_BAD)

payload = {"dataframe_split": {"columns": list(X_df.columns), "data": X_df.head(2).values.tolist()}}

print("== No ambiente do notebook (funciona: sklearn já está instalado) ==")
print(mlflow.models.predict(model_uri=MODEL_URI_BAD, input_data=payload, content_type="json", env_manager="local"))

print("== Em ambiente isolado, como no Model Serving ==")
expect_error(
    lambda: mlflow.models.predict(model_uri=MODEL_URI_BAD, input_data=payload, content_type="json", env_manager="virtualenv"),
    "A2 · predict em ambiente isolado (virtualenv)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — A2
# MAGIC
# MAGIC **O que procurar:**
# MAGIC - **Build logs** vs **Service logs** do endpoint (aba do Serving): `ModuleNotFoundError`/`ImportError` no *build* ou na carga do modelo = dependência faltando; erro só em runtime de inferência = código/dado.
# MAGIC - Dependências empacotadas com o modelo: `requirements.txt` / `conda.yaml` (`mlflow.pyfunc.get_model_dependencies`) **versus** os flavors e imports reais.
# MAGIC - Versão do flavor (`sklearn_version`) vs versão em `requirements.txt`: ausente ou divergente.
# MAGIC - Hipótese rápida **sem relogar**: `pip_requirements_override` no `mlflow.models.predict`.
# MAGIC - Python/MLflow do modelo (`python_version`) vs runtime do serving.

# COMMAND ----------

minfo = mlflow.models.get_model_info(MODEL_URI_BAD)
print("flavors:", list(minfo.flavors))
print("sklearn_version registrado no flavor:", minfo.flavors.get("sklearn", {}).get("sklearn_version"))

try:
    req_path = mlflow.pyfunc.get_model_dependencies(MODEL_URI_BAD)
    req_text = open(req_path).read()
    print("== requirements.txt empacotado ==")
    print(req_text)
    print("scikit-learn está nos requirements?", "scikit-learn" in req_text)
except Exception as e:  # noqa: BLE001
    print("não foi possível ler os requirements:", type(e).__name__, str(e)[:200])

print("== Testando a hipótese sem relogar (pip_requirements_override) ==")
expect_error(
    lambda: print(mlflow.models.predict(
        model_uri=MODEL_URI_BAD, input_data=payload, content_type="json", env_manager="virtualenv",
        pip_requirements_override=[f"scikit-learn=={sklearn.__version__}"],
    )),
    "A2 · predict com override de requirements",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — A2
# MAGIC
# MAGIC **Fix imediato:** relogar o modelo com as dependências corretas e **versões fixas**, com `signature` e `input_example`
# MAGIC (ou deixar o MLflow inferir omitindo `pip_requirements`), e **validar no ambiente isolado antes de redeployar**.
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Passo de CI/CD: `mlflow.models.predict(..., env_manager="virtualenv"/"uv")` antes de criar/atualizar o endpoint.
# MAGIC - Fixar versões (`scikit-learn==x.y.z`) e registrar no Unity Catalog (`mlflow.set_registry_uri("databricks-uc")`, `registered_model_name="catalog.schema.modelo"`).
# MAGIC - Sempre logar `signature` + `input_example` (validação de schema na entrada).
# MAGIC - Dependências privadas: empacotar wheels como `code_paths`/artifacts e liberar o acesso à rede do build.
# MAGIC - Model Serving pode não estar disponível no Free Edition: a validação local acima cobre a causa mais comum.

# COMMAND ----------

signature = infer_signature(X_df, model.predict(X_df))

with mlflow.start_run(run_name="a2_fixed"):
    info_ok = log_sklearn(
        model,
        signature=signature,
        input_example=X_df.head(3),
        pip_requirements=[
            f"scikit-learn=={sklearn.__version__}",
            f"pandas=={pd.__version__}",
            f"mlflow=={mlflow.__version__}",
        ],
    )
MODEL_URI_OK = info_ok.model_uri
print("modelo corrigido:", MODEL_URI_OK)

e = expect_error(
    lambda: print("predição no ambiente isolado:", mlflow.models.predict(
        model_uri=MODEL_URI_OK, input_data=payload, content_type="json", env_manager="virtualenv")),
    "A2 · validação pós-fix",
)
if e is not None:
    print("Se o erro acima for de criação do ambiente (rede/virtualenv), o requirements corrigido já está confirmado pelo diagnóstico estático.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — A2
# MAGIC
# MAGIC > The endpoint failed to start because the model was logged with a requirements list that does not include scikit-learn, so the isolated serving environment could not import it, while your notebook worked only because the package happened to be installed there. As an immediate fix, we logged the model again with pinned dependencies, a signature and an input example, and validated it in an isolated environment before redeploying. For the long term, please add that isolated prediction test to your deployment pipeline and register models in Unity Catalog with fixed package versions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A4 — Permissões no Unity Catalog
# MAGIC
# MAGIC **Ticket do cliente:** "A user gets `PERMISSION_DENIED` (or 'table not found') on a table that her colleague can read."
# MAGIC
# MAGIC ### 1. Reproduzir — A4
# MAGIC
# MAGIC No Free Edition você é o único usuário, então reproduzimos o sintoma tentando operar onde **não** temos privilégio (catálogo `system`) e consultando um objeto que não existe/não enxergamos.
# MAGIC Repare: sem `USE CATALOG`/`USE SCHEMA`/`BROWSE`, o Unity Catalog responde "not found" em vez de "denied" (não vaza a existência do objeto).

# COMMAND ----------

expect_error(lambda: spark.sql("CREATE SCHEMA system.lab_denied"), "A4 · CREATE SCHEMA no catálogo system")
expect_error(lambda: spark.sql("SELECT * FROM system.access.audit LIMIT 1").show(), "A4 · SELECT em system.access.audit")
expect_error(lambda: spark.sql("SELECT * FROM workspace.lab.tabela_que_nao_existe").show(), "A4 · tabela inexistente (parece igual a 'sem acesso')")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Diagnosticar — A4
# MAGIC
# MAGIC **O que procurar** (cadeia completa de privilégios do Unity Catalog, de cima para baixo):
# MAGIC 1. **Quem** é o principal? `current_user()`, grupos (`is_account_group_member`), service principal vs usuário.
# MAGIC 2. `USE CATALOG` no catálogo → `USE SCHEMA` no schema → `SELECT` na tabela (e `EXECUTE`/`READ VOLUME` etc. para outros objetos).
# MAGIC 3. `SHOW GRANTS ON CATALOG|SCHEMA|TABLE` e `SHOW GRANTS <principal> ON ...`: privilégios são **herdados** de cima para baixo.
# MAGIC 4. Dono do objeto (`DESCRIBE ... EXTENDED` → Owner) e quem pode `GRANT`.
# MAGIC 5. Row filter / column mask / view dinâmica, Workspace binding do catálogo (catálogo não ligado a este workspace), e *external location* para dados externos.
# MAGIC 6. Para ver o que **o outro usuário** enxerga: `SHOW GRANTS` nele, ou `information_schema.table_privileges`.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE lab.a4_demo AS SELECT 1 AS id, 'segredo' AS info")

me = spark.sql("SELECT current_user() AS usuario, current_catalog() AS catalogo, current_schema() AS schema").first()
print("quem sou eu:", me.asDict())
print("membro do grupo 'users'?", spark.sql("SELECT is_account_group_member('users')").first()[0])

for stmt in [
    "SHOW GRANTS ON CATALOG workspace",
    "SHOW GRANTS ON SCHEMA workspace.lab",
    "SHOW GRANTS ON TABLE workspace.lab.a4_demo",
]:
    print("\n>>", stmt)
    try:
        display(spark.sql(stmt))
    except Exception as e:  # noqa: BLE001
        print("   falhou:", type(e).__name__, str(e)[:200])

print("\n>> Dono da tabela")
display(spark.sql("DESCRIBE TABLE EXTENDED lab.a4_demo").filter("col_name = 'Owner'"))

print("\n>> information_schema.table_privileges")
try:
    display(spark.sql("SELECT grantor, grantee, table_schema, table_name, privilege_type FROM workspace.information_schema.table_privileges WHERE table_schema = 'lab'"))
except Exception as e:  # noqa: BLE001
    print("   falhou:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Corrigir — A4
# MAGIC
# MAGIC **Fix imediato:** conceder a cadeia mínima ao **grupo** (não ao usuário), executada por quem tem `MANAGE`/é dono:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG meu_catalogo TO `grupo-analistas`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  meu_catalogo.meu_schema TO `grupo-analistas`;
# MAGIC GRANT SELECT      ON TABLE   meu_catalogo.meu_schema.minha_tabela TO `grupo-analistas`;
# MAGIC ```
# MAGIC
# MAGIC **Prevenção de longo prazo:**
# MAGIC - Privilégios em **grupos** (idealmente sincronizados do IdP/SCIM), concedidos no nível de **schema** (herança) e não tabela a tabela.
# MAGIC - Donos de objetos = grupos ou service principals, nunca pessoas que saem da empresa.
# MAGIC - Grants versionados como código (Terraform / SQL em repositório) e auditoria via `system.access.audit`.
# MAGIC - Princípio do menor privilégio; `BROWSE` para descoberta sem dados.
# MAGIC
# MAGIC A demonstração abaixo concede e **revoga** `SELECT` na tabela de teste para `account users` (no Free Edition, só você), deixando o ambiente como estava.

# COMMAND ----------

try:
    spark.sql("GRANT SELECT ON TABLE workspace.lab.a4_demo TO `account users`")
    print(">> Depois do GRANT:")
    display(spark.sql("SHOW GRANTS ON TABLE workspace.lab.a4_demo"))
finally:
    try:
        spark.sql("REVOKE SELECT ON TABLE workspace.lab.a4_demo FROM `account users`")
        print(">> Depois do REVOKE (ambiente restaurado):")
        display(spark.sql("SHOW GRANTS ON TABLE workspace.lab.a4_demo"))
    except Exception as e:  # noqa: BLE001
        print("REVOKE não executado:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ao cliente — A4
# MAGIC
# MAGIC > The user is denied because Unity Catalog requires the full chain of privileges, meaning USE CATALOG on the catalog, USE SCHEMA on the schema and SELECT on the table, and the user's groups currently lack at least one of them, which is why the error can even look like a missing table. As an immediate fix, we granted the missing privileges to the group the user belongs to, and we verified the result with SHOW GRANTS on each level. For the long term, please manage access through groups at the schema level, assign ownership to groups or service principals, and keep the grants in version-controlled code.

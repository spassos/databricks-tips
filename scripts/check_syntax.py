#!/usr/bin/env python3
"""Valida os notebooks (.py no formato source do Databricks) sem precisar do Databricks/PySpark.

Checagens:
  * primeira linha "# Databricks notebook source" e separadores "# COMMAND ----------"
  * cada célula Python passa em ast.parse (células %md, %sql, %run, %sh são puladas)
  * cenários dos notebooks 01..04: 4 etapas na ordem (Reproduzir, Diagnosticar, Corrigir, Ao cliente)
  * "Ao cliente" tem exatamente 3 frases
  * spark.conf.set sempre dentro de try/except, em célula precedida de markdown "requer cluster clássico"
  * sem RDD / sparkContext (indisponíveis no serverless)
  * toda célula com writeStream usa availableNow e awaitTermination
  * 99_cheatsheet.py contém só markdown

Uso: python3 scripts/check_syntax.py
"""
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEADER = "# Databricks notebook source"
SEP_RE = re.compile(r"^# COMMAND ----------\s*$", re.MULTILINE)
MAGIC = "# MAGIC"

EXPECTED_SCENARIOS = {
    "01_spark.py": ["S1", "S2", "S3"],
    "02_delta.py": ["D1", "D2", "D3", "D4", "D5"],
    "03_streaming.py": ["ST1", "ST2", "ST3", "ST4"],
    "04_platform.py": ["A1", "A2", "A4"],
}
NOTEBOOKS = ["00_setup.py", *EXPECTED_SCENARIOS, "99_cheatsheet.py"]
STAGES = ["Reproduzir", "Diagnosticar", "Corrigir", "Ao cliente"]
SCENARIO_RE = re.compile(r"^## (ST\d+|S\d+|D\d+|A\d+) — ", re.MULTILINE)
STAGE_RE = re.compile(r"^### ([1-4])\. (Reproduzir|Diagnosticar|Corrigir|Ao cliente) — (\w+)", re.MULTILINE)
WARNING = "requer cluster clássico"


def parse_cells(text):
    """Devolve lista de (kind, body): kind em {'md', 'sql', 'run', 'sh', 'py'}."""
    chunks = SEP_RE.split(text)
    chunks[0] = chunks[0].split("\n", 1)[1] if "\n" in chunks[0] else ""
    cells = []
    for chunk in chunks:
        lines = chunk.strip("\n").split("\n")
        if not lines or lines == [""]:
            cells.append(("empty", ""))
            continue
        if lines[0].startswith(MAGIC):
            magic_lines = [ln for ln in lines if not ln.startswith(MAGIC)]
            body = "\n".join(ln[len(MAGIC):].lstrip(" ") if len(ln) > len(MAGIC) else "" for ln in lines)
            first = body.split("\n", 1)[0].strip()
            kind = first[1:].split()[0] if first.startswith("%") else "py"
            cells.append((kind if kind in {"md", "sql", "run", "sh"} else "magic?", (body, magic_lines)))
        else:
            cells.append(("py", chunk.strip("\n")))
    return cells


def check_conf_in_try(tree, errors, name, idx):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "set"
                and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "conf"):
            cur, inside_try = node, False
            while cur in parents:
                cur = parents[cur]
                if isinstance(cur, ast.Try):
                    inside_try = True
                    break
            if not inside_try:
                errors.append(f"{name} célula {idx}: spark.conf.set fora de try/except (linha {node.lineno})")


def main():
    errors = []
    for name in NOTEBOOKS:
        path = ROOT / name
        if not path.exists():
            errors.append(f"{name}: arquivo ausente")
            continue
        text = path.read_text(encoding="utf-8")
        if text.split("\n", 1)[0] != HEADER:
            errors.append(f"{name}: primeira linha deve ser '{HEADER}'")
            continue

        cells = parse_cells(text)
        n_py = 0
        for i, (kind, body) in enumerate(cells, 1):
            if kind == "empty":
                errors.append(f"{name} célula {i}: vazia")
            elif kind == "magic?":
                errors.append(f"{name} célula {i}: magic desconhecido")
            elif kind in {"md", "sql", "run", "sh"}:
                text_body, magic_lines = body
                if magic_lines:
                    errors.append(f"{name} célula {i}: mistura # MAGIC com código")
            elif kind == "py":
                n_py += 1
                try:
                    tree = ast.parse(body)
                except SyntaxError as e:
                    errors.append(f"{name} célula {i}: SyntaxError linha {e.lineno}: {e.msg}")
                    continue
                for node in ast.walk(tree):
                    if isinstance(node, ast.Attribute) and node.attr in {"rdd", "sparkContext"}:
                        errors.append(f"{name} célula {i}: uso de .{node.attr} (indisponível no serverless)")
                if "spark.conf.set" in body:
                    check_conf_in_try(tree, errors, name, i)
                    prev = cells[i - 2] if i >= 2 else ("", "")
                    if prev[0] != "md" or WARNING not in prev[1][0]:
                        errors.append(f"{name} célula {i}: spark.conf.set precisa de markdown anterior com '{WARNING}'")
                if "writeStream" in body and not ("availableNow" in body and "awaitTermination" in body):
                    errors.append(f"{name} célula {i}: writeStream sem availableNow/awaitTermination")

        if name == "99_cheatsheet.py":
            if n_py:
                errors.append(f"{name}: deve conter só markdown ({n_py} célula(s) de código)")
            print(f"✔ {name}: {len(cells)} células (só markdown)")
            continue
        if name != "00_setup.py":
            first_code = next((c for c in cells if c[0] in {"py", "run"}), None)
            if not first_code or first_code[0] != "run" or "./00_setup" not in first_code[1][0]:
                errors.append(f"{name}: primeira célula executável deve ser '%run ./00_setup'")

        if name in EXPECTED_SCENARIOS:
            md_text = "\n".join(b[0] for k, b in cells if k == "md")
            # sequência de eventos (cenário/etapa) na ordem do arquivo
            events = []
            for m in re.finditer(SCENARIO_RE.pattern + "|" + STAGE_RE.pattern, md_text, re.MULTILINE):
                if m.group(1) and not m.group(2):
                    events.append(("scenario", m.group(1)))
                else:
                    events.append(("stage", m.group(2), m.group(3), m.group(4)))
            found, current = {}, None
            for ev in events:
                if ev[0] == "scenario":
                    current = ev[1]
                    found[current] = []
                elif current is None:
                    errors.append(f"{name}: etapa '{ev[2]}' antes de qualquer cenário")
                else:
                    found[current].append((ev[1], ev[2], ev[3]))
            if list(found) != EXPECTED_SCENARIOS[name]:
                errors.append(f"{name}: cenários {list(found)} != esperado {EXPECTED_SCENARIOS[name]}")
            for sc, stages in found.items():
                if [s[1] for s in stages] != STAGES or [s[0] for s in stages] != ["1", "2", "3", "4"]:
                    errors.append(f"{name} {sc}: etapas {[s[1] for s in stages]} != {STAGES}")
                if any(s[2] != sc for s in stages):
                    errors.append(f"{name} {sc}: título de etapa com ID de cenário errado")
            # "Ao cliente": 3 frases no bloco de citação logo após o título da etapa 4
            for m in re.finditer(r"^### 4\. Ao cliente — (\w+)\n((?:>.*\n?)+)", md_text, re.MULTILINE):
                quote = " ".join(ln.lstrip("> ").strip() for ln in m.group(2).split("\n") if ln.strip())
                sentences = re.findall(r"[^.!?]+[.!?](?:\s|$)", quote + " ")
                if len(sentences) != 3:
                    errors.append(f"{name} {m.group(1)}: 'Ao cliente' tem {len(sentences)} frases (esperado 3)")
            if len(re.findall(r"^### 4\. Ao cliente", md_text, re.MULTILINE)) != len(EXPECTED_SCENARIOS[name]):
                errors.append(f"{name}: falta bloco 'Ao cliente' em algum cenário")

        print(f"✔ {name}: {len(cells)} células ({n_py} de código)")

    if errors:
        print("\n❌ Problemas encontrados:")
        for e in errors:
            print("  -", e)
        return 1
    print("\n✅ Todos os notebooks passaram.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

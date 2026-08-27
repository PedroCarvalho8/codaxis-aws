#!/usr/bin/env python3
"""Exercita o handler da API de leitura sem subir nada na AWS.

O codigo do handler e lido de dentro do template (recurso QueryFunction), e
nao de um arquivo solto, para nao existir uma segunda copia que possa
divergir. O DynamoDB e substituido por um stub que captura a condicao da
Query -- e assim o teste verifica que os prefixos de sk gerados batem com o
formato de chave que o Glue Job grava.
"""

import importlib.util
import json
import os
import sys
import tempfile
import types
from decimal import Decimal
from pathlib import Path

import boto3
import yaml


class Loader(yaml.SafeLoader):
    """SafeLoader que ignora as tags curtas do CloudFormation."""


Loader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        loader.construct_scalar(node)
        if isinstance(node, yaml.ScalarNode)
        else loader.construct_sequence(node)
        if isinstance(node, yaml.SequenceNode)
        else loader.construct_mapping(node)
    ),
)

capturado = {}


def achata(condicao, saida=None):
    """Extrai os literais string de uma condicao do boto3 (And/Equals/Between)."""
    saida = [] if saida is None else saida
    for valor in getattr(condicao, "_values", ()):
        if hasattr(valor, "_values"):
            achata(valor, saida)
        elif isinstance(valor, str):
            saida.append(valor)
    return saida


class TabelaFake:
    def query(self, KeyConditionExpression, **kwargs):
        capturado["sk"] = [
            v for v in achata(KeyConditionExpression) if v.startswith("AGG#")
        ]
        return {
            "Items": [
                {
                    "bucket_start": "2026-08-27T02:00:00Z",
                    "min": Decimal("23.0"),
                    "max": Decimal("24.4"),
                    "avg": Decimal("23.7"),
                    "n": 3,
                    "unit": "C",
                }
            ]
        }


def carrega_handler():
    raiz = Path(__file__).resolve().parent.parent
    template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
    codigo = template["Resources"]["QueryFunction"]["Properties"]["Code"]["ZipFile"]

    boto3.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda nome: TabelaFake())
    os.environ.setdefault("TABLE", "tabela-de-teste")
    os.environ.setdefault("CORS_ORIGIN", "*")

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as arquivo:
        arquivo.write(codigo)
        caminho = arquivo.name
    spec = importlib.util.spec_from_file_location("handler_da_api", caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


handler = carrega_handler().handler
falhas = []


def chama(query, device="sensor-teste", metrica="temperature"):
    return handler(
        {
            "pathParameters": {"device_id": device, "metric": metrica},
            "queryStringParameters": query,
        },
        None,
    )


def checa(rotulo, obtido, esperado):
    ok = obtido == esperado
    print(f"{'OK  ' if ok else 'FALHA'} {rotulo}: {obtido!r}")
    if not ok:
        falhas.append((rotulo, obtido, esperado))


# Granularidade escolhida pela extensao do range (regra descrita no README).
for rotulo, inicio, fim, esperado in [
    ("range 3h  -> 1min", "2026-08-27T00:00:00Z", "2026-08-27T03:00:00Z", "1min"),
    ("range 6h  -> 1min", "2026-08-27T00:00:00Z", "2026-08-27T06:00:00Z", "1min"),
    ("range 7h  -> 1h", "2026-08-27T00:00:00Z", "2026-08-27T07:00:00Z", "1h"),
    ("range 30d -> 1h", "2026-07-28T00:00:00Z", "2026-08-27T00:00:00Z", "1h"),
    ("range 31d -> 1d", "2026-07-27T00:00:00Z", "2026-08-27T00:00:00Z", "1d"),
]:
    checa(rotulo, json.loads(chama({"from": inicio, "to": fim})["body"])["granularity"], esperado)

# Prefixos de sk: precisam casar com o formato gravado pelo Glue Job.
chama({"from": "2026-08-27T00:00:00Z", "to": "2026-08-27T03:00:00Z"})
checa("sk 1min", capturado["sk"], ["AGG#1min#2026-08-27T00:00", "AGG#1min#2026-08-27T03:00"])
chama({"from": "2026-07-28T00:00:00Z", "to": "2026-08-27T00:00:00Z"})
checa("sk 1h", capturado["sk"], ["AGG#1h#2026-07-28T00", "AGG#1h#2026-08-27T00"])
chama({"from": "2026-05-27T00:00:00Z", "to": "2026-08-27T00:00:00Z"})
checa("sk 1d", capturado["sk"], ["AGG#1d#2026-05-27", "AGG#1d#2026-08-27"])

# Validacao de entrada.
checa("sem from/to -> 400", chama({})["statusCode"], 400)
checa("to <= from -> 400", chama({"from": "2026-08-27T03:00:00Z", "to": "2026-08-27T03:00:00Z"})["statusCode"], 400)
checa("data invalida -> 400", chama({"from": "ontem", "to": "hoje"})["statusCode"], 400)
checa("granularity invalida -> 400", chama({"from": "2026-08-27T00:00:00Z", "to": "2026-08-27T03:00:00Z", "granularity": "5min"})["statusCode"], 400)
checa("granularity forcada", json.loads(chama({"from": "2026-08-27T00:00:00Z", "to": "2026-08-27T03:00:00Z", "granularity": "1d"})["body"])["granularity"], "1d")

# Corpo e cabecalhos.
resposta = chama({"from": "2026-01-01T00:00:00Z", "to": "2026-01-01T03:00:00Z"})
corpo = json.loads(resposta["body"])
checa("Decimal virou numero", corpo["points"][0]["max"], 24.4)
checa("contagem", corpo["count"], 1)
checa("sem truncamento", corpo["truncated"], False)
checa("cache de janela fechada", resposta["headers"]["cache-control"], "public, max-age=86400")
aberta = chama({"from": "2026-01-01T00:00:00Z", "to": "2099-01-01T00:00:00Z"})
checa("cache de janela aberta", aberta["headers"]["cache-control"], "public, max-age=30")
checa("cors", aberta["headers"]["access-control-allow-origin"], "*")

print("\n=> todos os casos passaram" if not falhas else f"\n=> {len(falhas)} FALHA(S)")
sys.exit(1 if falhas else 0)

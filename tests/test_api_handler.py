#!/usr/bin/env python3
"""Exercita os handlers da API sem subir nada na AWS.

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


CATALOGO = [
    {"device_id": "sensor-02", "metric": "humidity", "unit": "%",
     "last_seen": "2026-08-27T01:00:00Z"},
    {"device_id": "sensor-01", "metric": "temperature", "unit": "C",
     "last_seen": "2026-08-27T02:00:00Z"},
    {"device_id": "sensor-01", "metric": "humidity", "unit": "%",
     "last_seen": "2026-08-27T02:00:00Z"},
]


# Uma celula que reaparece em dois dias (o trator passou de novo no mesmo
# lugar) e outra que so aparece num deles.
HEAT = {
    "HEAT#trator-01#2026-08-26": [
        {"gh": "6gyf4bf", "lat": Decimal("-23.55"), "lon": Decimal("-46.63"),
         "secs": 120, "dist_m": Decimal("300.5"), "n": 24},
        {"gh": "6gyf4bg", "lat": Decimal("-23.56"), "lon": Decimal("-46.64"),
         "secs": 45, "dist_m": Decimal("110.0"), "n": 9},
    ],
    "HEAT#trator-01#2026-08-27": [
        {"gh": "6gyf4bf", "lat": Decimal("-23.55"), "lon": Decimal("-46.63"),
         "secs": 80, "dist_m": Decimal("200.0"), "n": 16},
    ],
    "HEAT#trator-01#2026-08-27#6gyf4bf": [
        {"gh": "6gyf4bf8m", "lat": Decimal("-23.5505"), "lon": Decimal("-46.6333"),
         "secs": 35, "dist_m": Decimal("12.5"), "n": 7},
    ],
}


class TabelaFake:
    def query(self, KeyConditionExpression, **kwargs):
        literais = achata(KeyConditionExpression)
        capturado["chaves"] = literais
        capturado["sk"] = [v for v in literais if v.startswith("AGG#")]
        capturado.setdefault("pks_heat", [])
        alvo = next((v for v in literais if v.startswith("HEAT#")), None)
        if alvo:
            capturado["pks_heat"].append(alvo)
            capturado["prefixo_sk"] = next(
                (v for v in literais if v.startswith("GH")), None
            )
            return {"Items": [dict(i) for i in HEAT.get(alvo, [])]}
        if "CATALOG" in literais:
            return {"Items": list(CATALOGO)}
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


def carrega_handler(recurso):
    """Importa o codigo inline de um recurso Lambda direto do template."""
    raiz = Path(__file__).resolve().parent.parent
    template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
    codigo = template["Resources"][recurso]["Properties"]["Code"]["ZipFile"]

    boto3.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda nome: TabelaFake())
    os.environ.setdefault("TABLE", "tabela-de-teste")
    os.environ.setdefault("CORS_ORIGIN", "*")

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as arquivo:
        arquivo.write(codigo)
        caminho = arquivo.name
    spec = importlib.util.spec_from_file_location("handler_" + recurso, caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


handler = carrega_handler("QueryFunction").handler
handler_catalogo = carrega_handler("CatalogFunction").handler
handler_heatmap = carrega_handler("HeatmapFunction").handler
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

# ---------------------------------------------------------------- catalogo
catalogo = json.loads(handler_catalogo({}, None)["body"])
checa("catalogo: Query em CATALOG", "CATALOG" in capturado["chaves"], True)
checa("catalogo: contagem de devices", catalogo["count"], 2)
checa("catalogo: ordem dos devices",
      [d["device_id"] for d in catalogo["devices"]], ["sensor-01", "sensor-02"])
checa("catalogo: metricas agrupadas e ordenadas",
      [m["metric"] for m in catalogo["devices"][0]["metrics"]],
      ["humidity", "temperature"])
checa("catalogo: unidade preservada",
      catalogo["devices"][0]["metrics"][1]["unit"], "C")
checa("catalogo: cache curto",
      handler_catalogo({}, None)["headers"]["cache-control"], "public, max-age=60")

# ----------------------------------------------------------------- heatmap
def chama_heat(query, device="trator-01"):
    capturado["pks_heat"] = []
    return handler_heatmap(
        {"pathParameters": {"device_id": device}, "queryStringParameters": query},
        None,
    )


checa("heat: sem from/to -> 400", chama_heat({})["statusCode"], 400)
checa("heat: to < from -> 400",
      chama_heat({"from": "2026-08-27", "to": "2026-08-26"})["statusCode"], 400)
checa("heat: data invalida -> 400",
      chama_heat({"from": "27/08/2026", "to": "27/08/2026"})["statusCode"], 400)
checa("heat: intervalo longo demais -> 400",
      chama_heat({"from": "2026-01-01", "to": "2026-06-01"})["statusCode"], 400)

grossa = json.loads(chama_heat({"from": "2026-08-26", "to": "2026-08-27"})["body"])
checa("heat: uma Query por dia", capturado["pks_heat"],
      ["HEAT#trator-01#2026-08-26", "HEAT#trator-01#2026-08-27"])
checa("heat: precisao grossa por padrao", grossa["precision"], 7)
checa("heat: prefixo de sk", capturado["prefixo_sk"], "GH7#")
checa("heat: celulas distintas", grossa["count"], 2)
# 120 s no dia 26 + 80 s no dia 27 na mesma celula
checa("heat: permanencia somada entre dias",
      [c["secs"] for c in grossa["cells"]], [200, 45])
checa("heat: distancia somada entre dias",
      round(grossa["cells"][0]["dist_m"], 1), 500.5)
checa("heat: amostras somadas", grossa["cells"][0]["n"], 40)
checa("heat: cellSize p7", round(grossa["cellSize"]["lat"], 9), 0.001373291)

fina = json.loads(
    chama_heat({"from": "2026-08-27", "to": "2026-08-27", "cell": "6gyf4bf"})["body"]
)
checa("heat: cell muda a precisao", fina["precision"], 9)
checa("heat: cell entra na particao", capturado["pks_heat"],
      ["HEAT#trator-01#2026-08-27#6gyf4bf"])
checa("heat: prefixo de sk fino", capturado["prefixo_sk"], "GH9#")
checa("heat: celula fina devolvida", fina["cells"][0]["gh"], "6gyf4bf8m")
checa("heat: cellSize p9", round(fina["cellSize"]["lat"], 11), 4.291534e-05)

print("\n=> todos os casos passaram" if not falhas else f"\n=> {len(falhas)} FALHA(S)")
sys.exit(1 if falhas else 0)

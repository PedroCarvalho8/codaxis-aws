import json
import os

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
ORIGEM = os.environ["CORS_ORIGIN"]
TETO = 20000
DIAS_MAX = 31

# Tamanho da celula em graus, derivado da precisao: p caracteres = 5p bits,
# repartidos entre longitude (os impares) e latitude. Vai na resposta para o
# cliente desenhar o retangulo sem precisar decodificar geohash.
def tamanho_celula(precisao):
    bits = 5 * precisao
    bits_lon = (bits + 1) // 2
    bits_lat = bits // 2
    return 180.0 / (2 ** bits_lat), 360.0 / (2 ** bits_lon)


def responde(status, corpo, cache="no-store"):
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": ORIGEM,
            "cache-control": cache,
        },
        "body": json.dumps(corpo, default=float),
    }


def dias_entre(inicio, fim):
    from datetime import date, timedelta
    a = date.fromisoformat(inicio)
    b = date.fromisoformat(fim)
    if b < a:
        return None
    saida, atual = [], a
    while atual <= b and len(saida) <= DIAS_MAX:
        saida.append(atual.isoformat())
        atual += timedelta(days=1)
    return saida


def handler(event, context):
    caminho = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}
    device = caminho.get("device_id")

    try:
        dias = dias_entre(query["from"], query["to"])
    except (KeyError, ValueError, TypeError):
        return responde(400, {"erro": "from e to obrigatorios, em AAAA-MM-DD"})
    if dias is None:
        return responde(400, {"erro": "to precisa ser maior ou igual a from"})
    if len(dias) > DIAS_MAX:
        return responde(400, {"erro": f"intervalo maior que {DIAS_MAX} dias"})

    # A celula grossa e a particao da fina: o cliente manda a celula que esta
    # no viewport e recebe so o detalhe dela, em vez do dia inteiro a 5 m.
    celula = query.get("cell")
    precisao = 9 if celula else 7
    prefixo = f"GH{precisao}"

    celulas = {}
    truncado = False
    for dia in dias:
        pk = f"HEAT#{device}#{dia}" + (f"#{celula}" if celula else "")
        proxima = None
        while True:
            extra = {"ExclusiveStartKey": proxima} if proxima else {}
            pagina = TABELA.query(
                KeyConditionExpression=(
                    Key("pk").eq(pk) & Key("sk").begins_with(prefixo + "#")
                ),
                **extra,
            )
            for item in pagina["Items"]:
                # Somar entre dias: a mesma celula reaparece a cada passagem.
                chave = item["gh"]
                acumulado = celulas.setdefault(
                    chave,
                    {"gh": chave, "lat": item["lat"], "lon": item["lon"],
                     "secs": 0, "dist_m": 0, "n": 0},
                )
                acumulado["secs"] += int(item["secs"])
                acumulado["dist_m"] += float(item["dist_m"])
                acumulado["n"] += int(item["n"])
            proxima = pagina.get("LastEvaluatedKey")
            if not proxima or len(celulas) >= TETO:
                truncado = truncado or bool(proxima)
                break

    d_lat, d_lon = tamanho_celula(precisao)
    return responde(
        200,
        {
            "device_id": device,
            "precision": precisao,
            "cell": celula,
            "from": query["from"],
            "to": query["to"],
            "cellSize": {"lat": d_lat, "lon": d_lon},
            "truncated": truncado,
            "count": len(celulas),
            "cells": sorted(celulas.values(), key=lambda c: c["gh"]),
        },
        "public, max-age=300",
    )

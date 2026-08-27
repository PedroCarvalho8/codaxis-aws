import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
ORIGEM = os.environ["CORS_ORIGIN"]
TETO = 5000

# granularidade -> (range maximo em segundos, caracteres do prefixo de tempo)
# Espelha o formato de chave gravado pelo Glue Job:
#   1min aaaa-mm-ddTHH:MM | 1h aaaa-mm-ddTHH | 1d aaaa-mm-dd
FAIXAS = (("1min", 6 * 3600, 16), ("1h", 30 * 86400, 13), ("1d", None, 10))
CORTES = {g: c for g, _, c in FAIXAS}


def instante(texto):
    return datetime.fromisoformat(texto.replace("Z", "+00:00"))


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


def handler(event, context):
    caminho = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}
    device, metrica = caminho.get("device_id"), caminho.get("metric")

    try:
        inicio, fim = instante(query["from"]), instante(query["to"])
    except (KeyError, ValueError, TypeError):
        return responde(400, {"erro": "from e to obrigatorios, em ISO-8601 UTC"})
    if fim <= inicio:
        return responde(400, {"erro": "to precisa ser maior que from"})

    gran = query.get("granularity")
    if not gran:
        janela = (fim - inicio).total_seconds()
        gran = next(g for g, teto, _ in FAIXAS if teto is None or janela <= teto)
    if gran not in CORTES:
        return responde(400, {"erro": f"granularity invalida: {gran}"})

    corte = CORTES[gran]
    condicao = Key("pk").eq(f"DEV#{device}#{metrica}") & Key("sk").between(
        f"AGG#{gran}#{inicio.isoformat()[:corte]}",
        f"AGG#{gran}#{fim.isoformat()[:corte]}",
    )

    pontos, proxima = [], None
    while len(pontos) < TETO:
        extra = {"ExclusiveStartKey": proxima} if proxima else {}
        pagina = TABELA.query(KeyConditionExpression=condicao, **extra)
        pontos += [
            {
                "t": item["bucket_start"],
                "min": item["min"],
                "max": item["max"],
                "avg": item["avg"],
                "n": int(item["n"]),
                "unit": item.get("unit") or None,
            }
            for item in pagina["Items"]
        ]
        proxima = pagina.get("LastEvaluatedKey")
        if not proxima:
            break

    # Bucket fechado nao muda mais; so a janela que alcanca o presente
    # precisa de cache curto.
    encerrado = fim < datetime.now(timezone.utc)
    return responde(
        200,
        {
            "device_id": device,
            "metric": metrica,
            "granularity": gran,
            "from": query["from"],
            "to": query["to"],
            "truncated": len(pontos) >= TETO,
            "count": len(pontos),
            "points": pontos,
        },
        "public, max-age=86400" if encerrado else "public, max-age=30",
    )

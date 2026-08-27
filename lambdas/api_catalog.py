import json
import os
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
ORIGEM = os.environ["CORS_ORIGIN"]


def handler(event, context):
    # O Glue Job mantem um item por (device, metrica) sob pk = "CATALOG".
    # Sem ele, listar dispositivos exigiria um Scan da tabela inteira.
    dispositivos = defaultdict(list)
    proxima = None
    while True:
        extra = {"ExclusiveStartKey": proxima} if proxima else {}
        pagina = TABELA.query(
            KeyConditionExpression=Key("pk").eq("CATALOG"), **extra
        )
        for item in pagina["Items"]:
            dispositivos[item["device_id"]].append({
                "metric": item["metric"],
                "unit": item.get("unit") or None,
                "last_seen": item.get("last_seen"),
            })
        proxima = pagina.get("LastEvaluatedKey")
        if not proxima:
            break

    corpo = {
        "count": len(dispositivos),
        "devices": [
            {
                "device_id": device,
                "metrics": sorted(metricas, key=lambda m: m["metric"]),
            }
            for device, metricas in sorted(dispositivos.items())
        ],
    }
    return {
        "statusCode": 200,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": ORIGEM,
            "cache-control": "public, max-age=60",
        },
        "body": json.dumps(corpo),
    }

import json
import os

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def molda(item):
    """Item DEVICEMETA -> DeviceResponse do contrato argus."""
    return {
        "id": item["sk"],
        "code": item["sk"],
        "label": item.get("label") or "",
        "createdAt": item.get("created_at"),
        "revokedAt": item.get("revoked_at"),
        "active": bool(item.get("active")),
    }


def handler(event, context):
    rota = event.get("routeKey", "")
    caminho = event.get("pathParameters") or {}

    if rota == "GET /api/devices":
        # Metadados numa particao unica: listar e uma Query, nao um Scan nem
        # N chamadas ao IoT. A particao cresce com a frota, nao com o dado.
        itens, proxima = [], None
        while True:
            extra = {"ExclusiveStartKey": proxima} if proxima else {}
            pagina = TABELA.query(
                KeyConditionExpression=Key("pk").eq("DEVICEMETA"), **extra
            )
            itens += pagina["Items"]
            proxima = pagina.get("LastEvaluatedKey")
            if not proxima:
                break
        return resp(200, [molda(i) for i in itens])

    if rota == "GET /api/devices/{code}":
        codigo = caminho.get("code")
        r = TABELA.get_item(Key={"pk": "DEVICEMETA", "sk": codigo})
        if "Item" not in r:
            return resp(404, {"message": f"device {codigo} nao existe"})
        return resp(200, molda(r["Item"]))

    return resp(404, {"message": "rota desconhecida"})
